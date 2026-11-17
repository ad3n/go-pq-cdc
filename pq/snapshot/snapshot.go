package snapshot

import (
	"context"
	goerrors "errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/ad3n/go-pq-cdc/config"
	"github.com/ad3n/go-pq-cdc/internal/metric"
	"github.com/ad3n/go-pq-cdc/logger"
	"github.com/ad3n/go-pq-cdc/pq"
	"github.com/ad3n/go-pq-cdc/pq/message/format"
	"github.com/ad3n/go-pq-cdc/pq/publication"
	"github.com/go-playground/errors"
	"github.com/jackc/pgx/v5/pgtype"
)

var (
	ErrSnapshotInvalidated = goerrors.New("snapshot invalidated by coordinator restart")
)

type (
	Handler func(event *format.Snapshot) error

	Snapshotter struct {
		healthcheckConn    pq.Connection
		exportSnapshotConn pq.Connection
		metric             metric.Metric
		metadataConn       pq.Connection
		keepaliveCancel    context.CancelFunc
		typeMap            *pgtype.Map
		connectionPool     *ConnectionPool
		orderByCache       map[string]orderByCacheEntry
		leafRoots          map[string]string
		keepaliveDone      chan struct{}
		dsn                string
		cachedSnapshotID   string
		tables             publication.Tables
		config             config.SnapshotConfig
		orderByMu          sync.RWMutex
		keepaliveMu        sync.Mutex
		exportConnClosed   bool
	}

	orderByCacheEntry struct {
		clause  string
		columns []string
	}
)

func New(ctx context.Context, snapshotConfig config.SnapshotConfig, tables publication.Tables, dsn string, m metric.Metric) (_ *Snapshotter, err error) {
	metadataConn, err := pq.NewConnection(ctx, dsn)
	if err != nil {
		return nil, errors.Wrap(err, "create metadata connection")
	}

	defer func() {
		if err != nil {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			metadataConn.Close(cleanupCtx)
		}
	}()

	healthcheckConn, err := pq.NewConnection(ctx, dsn)
	if err != nil {
		return nil, errors.Wrap(err, "create healthcheck connection")
	}

	defer func() {
		if err != nil {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			healthcheckConn.Close(cleanupCtx)
		}
	}()

	connectionPool, err := NewConnectionPool(ctx, dsn, 5)
	if err != nil {
		return nil, errors.Wrap(err, "create connection pool")
	}

	return &Snapshotter{
		dsn:             dsn,
		metadataConn:    metadataConn,
		healthcheckConn: healthcheckConn,
		connectionPool:  connectionPool,
		config:          snapshotConfig,
		tables:          tables,
		typeMap:         pgtype.NewMap(),
		metric:          m,
		orderByCache:    make(map[string]orderByCacheEntry),
	}, nil
}

func (s *Snapshotter) Prepare(ctx context.Context, slotName string) error {
	instanceID := generateInstanceID(s.config.InstanceID)
	logger.Debug("[snapshot] preparing", "instanceID", instanceID)

	isCoordinator, err := s.setupJob(ctx, slotName, instanceID)
	if err != nil {
		return errors.Wrap(err, "setup job")
	}

	if isCoordinator {
		logger.Debug("[coordinator] snapshot transaction kept OPEN - replication slot must be created NOW")
	}

	return nil
}

func (s *Snapshotter) Execute(ctx context.Context, handler Handler, slotName string) error {
	startTime := time.Now()
	instanceID := generateInstanceID(s.config.InstanceID)
	logger.Debug("[snapshot] executing", "instanceID", instanceID)

	job, err := s.loadJob(ctx, slotName)
	if err != nil || job == nil {
		return errors.New("job not found - Prepare() must be called first")
	}

	if err := s.executeWorker(ctx, slotName, instanceID, job, handler, startTime); err != nil {
		return errors.Wrap(err, "execute worker")
	}

	if err := s.finalizeSnapshot(ctx, slotName, job, handler); err != nil {
		return errors.Wrap(err, "finalize snapshot")
	}

	logger.Info("[snapshot] execution completed", "instanceID", instanceID, "duration", time.Since(startTime))
	return nil
}

func (s *Snapshotter) finalizeSnapshot(ctx context.Context, slotName string, job *Job, handler Handler) error {
	allCompleted, err := s.checkJobCompleted(ctx, slotName)
	if err != nil {
		return errors.Wrap(err, "check job completed")
	}

	if !allCompleted {
		return nil
	}

	logger.Info("[snapshot] all chunks completed, finalizing snapshot")

	if err := s.markJobAsCompleted(ctx, slotName); err != nil {
		logger.Warn("[snapshot] failed to mark job as completed", "error", err)
		return err
	}

	s.closeAllConnections(ctx, true)

	return handler(&format.Snapshot{
		EventType:  format.SnapshotEventTypeEnd,
		ServerTime: time.Now().UTC(),
		LSN:        job.SnapshotLSN,
	})
}

func (s *Snapshotter) closeAllConnections(ctx context.Context, commitExport bool) {
	logger.Info("[snapshot] closing all connections")

	s.closeExportSnapshotConnection(ctx, commitExport)

	if s.connectionPool != nil {
		s.connectionPool.Close(ctx)
		s.connectionPool = nil
	}

	if s.metadataConn != nil {
		if err := s.metadataConn.Close(ctx); err != nil {
			logger.Warn("[snapshot] error closing metadata connection", "error", err)
		}

		s.metadataConn = nil
	}

	if s.healthcheckConn != nil {
		if err := s.healthcheckConn.Close(ctx); err != nil {
			logger.Warn("[snapshot] error closing healthcheck connection", "error", err)
		}

		s.healthcheckConn = nil
	}

	logger.Info("[snapshot] all connections closed")
}

func (s *Snapshotter) closeExportSnapshotConnection(ctx context.Context, commit bool) {
	s.stopSnapshotKeepalive()

	s.keepaliveMu.Lock()
	if s.exportConnClosed {
		s.keepaliveMu.Unlock()
		return
	}

	s.exportConnClosed = true
	exportConn := s.exportSnapshotConn
	s.keepaliveMu.Unlock()

	if exportConn == nil {
		return
	}

	if commit {
		logger.Info("[coordinator] committing and closing snapshot export connection")
		if err := s.execSQL(ctx, exportConn, "COMMIT"); err != nil {
			logger.Warn("[coordinator] failed to commit snapshot transaction, attempting rollback", "error", err)
			if rollbackErr := s.execSQL(ctx, exportConn, "ROLLBACK"); rollbackErr != nil {
				logger.Error("[coordinator] failed to rollback snapshot transaction", "error", rollbackErr)
			}
		}
	} else {
		logger.Info("[coordinator] rolling back and closing snapshot export connection")
		if err := s.execSQL(ctx, exportConn, "ROLLBACK"); err != nil {
			logger.Warn("[coordinator] failed to rollback snapshot transaction", "error", err)
		}
	}

	if err := exportConn.Close(ctx); err != nil {
		logger.Warn("[coordinator] error closing export snapshot connection", "error", err)
	}

	s.keepaliveMu.Lock()
	s.exportSnapshotConn = nil
	s.keepaliveMu.Unlock()
}

func (s *Snapshotter) Close(ctx context.Context) {
	if s == nil {
		return
	}

	logger.Debug("[snapshot] closing snapshotter (fallback cleanup)")
	s.closeAllConnections(ctx, false)
}

func (s *Snapshotter) startSnapshotKeepalive(parentCtx context.Context, conn pq.Connection) {
	s.stopSnapshotKeepalive()

	keepaliveCtx, keepaliveCancel := context.WithCancel(parentCtx)
	done := make(chan struct{})

	s.keepaliveMu.Lock()
	s.keepaliveCancel = keepaliveCancel
	s.keepaliveDone = done
	s.exportConnClosed = false
	s.keepaliveMu.Unlock()

	go s.snapshotTransactionKeepalive(keepaliveCtx, conn, done)
}

func (s *Snapshotter) stopSnapshotKeepalive() {
	s.keepaliveMu.Lock()
	cancel := s.keepaliveCancel
	done := s.keepaliveDone
	s.keepaliveCancel = nil
	s.keepaliveDone = nil
	s.keepaliveMu.Unlock()

	if cancel != nil {
		cancel()
	}

	if done != nil {
		<-done
	}
}

func (s *Snapshotter) decodeColumnData(data []byte, dataTypeOID uint32) (interface{}, error) {
	if dt, ok := s.typeMap.TypeForOID(dataTypeOID); ok {
		return dt.Codec.DecodeValue(s.typeMap, dataTypeOID, pgtype.TextFormatCode, data)
	}

	return string(data), nil
}

func generateInstanceID(configuredID string) string {
	if configuredID != "" {
		return configuredID
	}

	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	pid := os.Getpid()
	return fmt.Sprintf("%s-%d", hostname, pid)
}
