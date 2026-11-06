package snapshot

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/ad3n/go-pq-cdc/config"
	"github.com/ad3n/go-pq-cdc/logger"
	"github.com/ad3n/go-pq-cdc/pq"
	"github.com/go-playground/errors"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type gatedHeartbeatConnection struct {
	pq.Connection
	entered  chan struct{}
	canceled chan struct{}
	release  chan struct{}
	once     sync.Once
}

func (c *gatedHeartbeatConnection) Exec(ctx context.Context, sql string) *pgconn.MultiResultReader {
	c.once.Do(func() { close(c.entered) })
	<-ctx.Done()
	close(c.canceled)
	<-c.release
	return (&pgconn.PgConn{}).Exec(ctx, sql)
}

func TestParseClaimedChunkWithPhysicalPartition(t *testing.T) {
	now := time.Now().UTC()
	row := [][]byte{
		[]byte("42"),
		[]byte("public"),
		[]byte("events"),
		[]byte("archive"),
		[]byte("events_2026_01"),
		[]byte("3"),
		[]byte("12"),
		[]byte("100"),
		nil,
		nil,
		[]byte("12"),
		nil,
		[]byte("true"),
		[]byte("ctid_block"),
		[]byte("0"),
	}

	chunk, err := (&Snapshotter{}).parseClaimedChunk(row, "snapshot", "worker", now)
	require.NoError(t, err)
	assert.Equal(t, int64(42), chunk.ID)
	assert.Equal(t, "public", chunk.TableSchema)
	assert.Equal(t, "events", chunk.TableName)
	assert.Equal(t, "archive", chunk.PhysicalTableSchema)
	assert.Equal(t, "events_2026_01", chunk.PhysicalTableName)
	assert.Equal(t, 3, chunk.ChunkIndex)
	assert.Equal(t, int64(12), chunk.ChunkStart)
	assert.Equal(t, int64(100), chunk.ChunkSize)
	assert.Equal(t, int64(12), *chunk.BlockStart)
	assert.Nil(t, chunk.BlockEnd)
	assert.True(t, chunk.IsLastChunk)
	assert.Equal(t, PartitionStrategyCTIDBlock, chunk.PartitionStrategy)
}

// The dropped-partition path relies on the SQLSTATE surviving the Wrap chain between
// the chunk query and executeChunkProcessing.
func TestIsUndefinedTableErrorSeesThroughWraps(t *testing.T) {
	pgErr := &pgconn.PgError{Code: "42P01"}
	wrapped := errors.Wrap(errors.Wrap(errors.Wrap(pgErr, "execute chunk query"), "execute function"), "process chunk with transaction")

	assert.True(t, isUndefinedTableError(wrapped))
	assert.False(t, isUndefinedTableError(errors.Wrap(&pgconn.PgError{Code: "42703"}, "execute chunk query")))
}

func TestHeartbeatStopJoinsWorker(t *testing.T) {
	logger.InitLogger(slog.Default())
	conn := &gatedHeartbeatConnection{entered: make(chan struct{}), canceled: make(chan struct{}), release: make(chan struct{})}
	s := &Snapshotter{healthcheckConn: conn, config: config.SnapshotConfig{HeartbeatInterval: time.Millisecond}}
	stop, chunks := s.startHeartbeat(t.Context())
	var release sync.Once
	defer func() { release.Do(func() { close(conn.release) }); stop() }()
	chunks <- 1
	select {
	case <-conn.entered:
	case <-time.After(time.Second):
		t.Fatal("heartbeat query did not start")
	}
	stopped := make(chan struct{})
	go func() { stop(); close(stopped) }()
	select {
	case <-conn.canceled:
	case <-time.After(time.Second):
		t.Fatal("heartbeat query was not canceled")
	}
	select {
	case <-stopped:
		t.Fatal("stop returned while heartbeat query still running")
	case <-time.After(20 * time.Millisecond):
	}
	release.Do(func() { close(conn.release) })
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("heartbeat worker did not exit")
	}
	s.healthcheckConn = nil
	stop()
}
