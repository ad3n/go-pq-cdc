package timescaledb

import (
	"context"
	goerrors "errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ad3n/go-pq-cdc/logger"
	"github.com/ad3n/go-pq-cdc/pq"
	"github.com/go-playground/errors"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

var (
	typeMap     = pgtype.NewMap()
	HyperTables = sync.Map{}
)

type TimescaleDB struct {
	conn      pq.Connection
	ctx       context.Context
	cancel    context.CancelFunc
	mu        sync.Mutex
	workers   sync.WaitGroup
	closeOnce sync.Once
	closed    atomic.Bool
}

func (tdb *TimescaleDB) SyncHyperTables(ctx context.Context) {
	tdb.mu.Lock()
	if tdb.closed.Load() {
		tdb.mu.Unlock()
		return
	}

	tdb.workers.Add(1)
	tdb.mu.Unlock()
	defer func() {
		tdb.workers.Done()
		tdb.Close(context.Background())
	}()

	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(tdb.ctx, cancel)
	defer func() {
		stop()
		cancel()
	}()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		hyperTables, err := tdb.FindHyperTables(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}

			logger.Error("timescale tables", "error", err)
			continue
		}

		logger.Debug("timescale tables", "tables", hyperTables)
	}
}

func (tdb *TimescaleDB) Close(ctx context.Context) {
	tdb.closeOnce.Do(func() {
		tdb.closed.Store(true)
		tdb.cancel()
		ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()

		tdb.mu.Lock()
		defer func() {
			tdb.mu.Unlock()
			tdb.workers.Wait()
		}()

		tdb.conn.Close(ctx)
	})
}

func (tdb *TimescaleDB) FindHyperTables(ctx context.Context) (map[string]string, error) {
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(tdb.ctx, cancel)
	defer func() {
		stop()
		cancel()
	}()

	tdb.mu.Lock()
	defer tdb.mu.Unlock()

	if tdb.closed.Load() {
		return nil, context.Canceled
	}

	query := "SELECT h.hypertable_schema, h.hypertable_name, c.chunk_schema, c.chunk_name FROM timescaledb_information.chunks c JOIN timescaledb_information.hypertables h ON c.hypertable_schema = h.hypertable_schema AND c.hypertable_name = h.hypertable_name;"
	resultReader := tdb.conn.Exec(ctx, query)
	defer resultReader.Close()

	results, err := resultReader.ReadAll()
	if err != nil {
		var pgErr *pgconn.PgError
		if goerrors.As(err, &pgErr) {
			if pgErr.Code == "42P01" {
				tdb.cancel()
				logger.Debug("timescale db hypertable relation not found", "error", err)
				return nil, nil
			}
		}

		return nil, errors.Wrap(err, "hyper tables result")
	}

	if len(results) == 0 || results[0].CommandTag.String() == "SELECT 0" {
		return nil, nil
	}

	if err = resultReader.Close(); err != nil {
		return nil, errors.Wrap(err, "hyper tables result reader close")
	}

	ht, err := decodeHyperTablesResult(results)
	if err != nil {
		return nil, errors.Wrap(err, "hyper tables result decode")
	}

	for k, v := range ht {
		HyperTables.Store(k, v)
	}

	return ht, nil
}

func NewTimescaleDB(ctx context.Context, dsn string) (*TimescaleDB, error) {
	conn, err := pq.NewConnection(ctx, dsn)
	if err != nil {
		return nil, errors.Wrap(err, "new postgresql connection")
	}

	owner, cancel := context.WithCancel(context.Background())
	return &TimescaleDB{conn: conn, ctx: owner, cancel: cancel}, nil
}

func decodeHyperTablesResult(results []*pgconn.Result) (map[string]string, error) {
	res := make(map[string]string)

	for _, result := range results {
		for i := range len(result.Rows) {
			var hyperName, hyperSchema, chunkSchema, chunkName string
			for j, fd := range result.FieldDescriptions {
				v, err := decodeTextColumnData(result.Rows[i][j], fd.DataTypeOID)
				if err != nil {
					return nil, err
				}

				if v == nil {
					continue
				}

				switch fd.Name {
				case "hypertable_schema":
					hyperSchema = v.(string)
				case "hypertable_name":
					hyperName = v.(string)
				case "chunk_schema":
					chunkSchema = v.(string)
				case "chunk_name":
					chunkName = v.(string)
				}
			}
			res[fmt.Sprintf("%s.%s", chunkSchema, chunkName)] = fmt.Sprintf("%s.%s", hyperSchema, hyperName)
		}
	}

	return res, nil
}

func decodeTextColumnData(data []byte, dataType uint32) (interface{}, error) {
	if dt, ok := typeMap.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(typeMap, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}
