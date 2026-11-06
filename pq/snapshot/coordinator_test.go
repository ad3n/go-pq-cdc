package snapshot

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"

	"github.com/ad3n/go-pq-cdc/config"
	"github.com/ad3n/go-pq-cdc/logger"
	"github.com/ad3n/go-pq-cdc/pq"
	"github.com/ad3n/go-pq-cdc/pq/publication"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
)

type failingMetadataConnection struct{ pq.Connection }

func (failingMetadataConnection) Exec(ctx context.Context, sql string) *pgconn.MultiResultReader {
	return (&pgconn.PgConn{}).Exec(ctx, sql)
}

func (failingMetadataConnection) Close(context.Context) error { return nil }

func ptrInt64(v int64) *int64 { return &v }

func TestAndCondition(t *testing.T) {
	t.Run("both empty returns empty", func(t *testing.T) {
		assert.Empty(t, andCondition("", ""))
	})
	t.Run("only existing returns existing", func(t *testing.T) {
		assert.Equal(t, "a = 1", andCondition("a = 1", ""))
	})
	t.Run("only extra returns parenthesized", func(t *testing.T) {
		assert.Equal(t, "(b = 2)", andCondition("", "b = 2"))
	})
	t.Run("combines with AND and parenthesizes extra for OR precedence", func(t *testing.T) {
		assert.Equal(t, "a = 1 AND (b = 2)", andCondition("a = 1", "b = 2"))
	})
}

func TestGetQueryCondition(t *testing.T) {
	newSnapshotter := func(cfg config.SnapshotConfig, tables publication.Tables) *Snapshotter {
		return &Snapshotter{config: cfg, tables: tables}
	}

	t.Run("returns empty when no conditions defined", func(t *testing.T) {
		s := newSnapshotter(config.SnapshotConfig{}, publication.Tables{
			{Name: "users", Schema: "public"},
		})
		assert.Empty(t, s.getQueryCondition("public", "users"))
	})

	t.Run("returns global condition when no per-table override", func(t *testing.T) {
		s := newSnapshotter(
			config.SnapshotConfig{QueryCondition: "is_active = true"},
			publication.Tables{{Name: "users", Schema: "public"}},
		)
		assert.Equal(t, "is_active = true", s.getQueryCondition("public", "users"))
	})

	t.Run("per-table condition overrides global", func(t *testing.T) {
		s := newSnapshotter(
			config.SnapshotConfig{QueryCondition: "is_active = true"},
			publication.Tables{
				{Name: "users", Schema: "public", QueryCondition: "deleted_at IS NULL"},
			},
		)
		assert.Equal(t, "deleted_at IS NULL", s.getQueryCondition("public", "users"))
	})

	t.Run("empty per-table falls back to global", func(t *testing.T) {
		s := newSnapshotter(
			config.SnapshotConfig{QueryCondition: "is_active = true"},
			publication.Tables{
				{Name: "users", Schema: "public", QueryCondition: ""},
				{Name: "orders", Schema: "public", QueryCondition: "status <> 'cancelled'"},
			},
		)
		assert.Equal(t, "is_active = true", s.getQueryCondition("public", "users"))
		assert.Equal(t, "status <> 'cancelled'", s.getQueryCondition("public", "orders"))
	})

	t.Run("empty schema is normalized to public", func(t *testing.T) {
		s := newSnapshotter(
			config.SnapshotConfig{},
			publication.Tables{
				{Name: "users", Schema: "", QueryCondition: "active"},
			},
		)
		assert.Equal(t, "active", s.getQueryCondition("", "users"))
		assert.Equal(t, "active", s.getQueryCondition("public", "users"))
	})

	t.Run("different schema does not match", func(t *testing.T) {
		s := newSnapshotter(
			config.SnapshotConfig{QueryCondition: "global_cond"},
			publication.Tables{
				{Name: "users", Schema: "tenant_a", QueryCondition: "table_cond"},
			},
		)
		assert.Equal(t, "global_cond", s.getQueryCondition("public", "users"))
		assert.Equal(t, "table_cond", s.getQueryCondition("tenant_a", "users"))
	})
}

func TestBuildPrimaryKeyBoundsQueryWithCondition(t *testing.T) {
	s := &Snapshotter{
		config: config.SnapshotConfig{QueryCondition: "deleted_at IS NULL"},
	}

	query := s.buildPrimaryKeyBoundsQuery(publication.Table{
		Schema: "public",
		Name:   "users",
	}, "id")

	assert.Contains(t, query, "FROM public.users")
	assert.Contains(t, query, "WHERE (deleted_at IS NULL)")
}

func TestBuildChunkQueryWithCondition(t *testing.T) {
	s := &Snapshotter{}

	t.Run("integer range injects condition into WHERE", func(t *testing.T) {
		chunk := &Chunk{
			TableSchema:       "public",
			TableName:         "users",
			PartitionStrategy: PartitionStrategyIntegerRange,
			RangeStart:        ptrInt64(1),
			RangeEnd:          ptrInt64(1000),
			ChunkSize:         500,
		}
		q := s.buildChunkQuery(chunk, "id", []string{"id"}, "is_active = true")
		assert.Contains(t, q, "WHERE id >= 1 AND id <= 1000 AND (is_active = true)")
		assert.Contains(t, q, "ORDER BY id LIMIT 500")
	})

	t.Run("integer range without condition is unchanged from legacy shape", func(t *testing.T) {
		chunk := &Chunk{
			TableSchema:       "public",
			TableName:         "users",
			PartitionStrategy: PartitionStrategyIntegerRange,
			RangeStart:        ptrInt64(1),
			RangeEnd:          ptrInt64(10),
			ChunkSize:         5,
		}
		q := s.buildChunkQuery(chunk, "id", []string{"id"}, "")
		assert.Equal(t, "SELECT * FROM public.users WHERE id >= 1 AND id <= 10 ORDER BY id LIMIT 5", q)
	})

	t.Run("offset strategy injects condition into WHERE", func(t *testing.T) {
		chunk := &Chunk{
			TableSchema:       "public",
			TableName:         "orders",
			PartitionStrategy: PartitionStrategyOffset,
			ChunkSize:         100,
			ChunkStart:        200,
		}
		q := s.buildChunkQuery(chunk, "id", nil, "status = 'active'")
		assert.Contains(t, q, "WHERE (status = 'active')")
		assert.Contains(t, q, "ORDER BY id LIMIT 100 OFFSET 200")
	})

	t.Run("OR in condition is parenthesized with integer range", func(t *testing.T) {
		chunk := &Chunk{
			TableSchema:       "public",
			TableName:         "users",
			PartitionStrategy: PartitionStrategyIntegerRange,
			RangeStart:        ptrInt64(1),
			RangeEnd:          ptrInt64(1000),
			ChunkSize:         500,
		}
		q := s.buildChunkQuery(chunk, "id", []string{"id"}, "status = 'a' OR status = 'b'")
		assert.Contains(t, q, "AND (status = 'a' OR status = 'b')")
	})

	t.Run("offset strategy without condition omits WHERE", func(t *testing.T) {
		chunk := &Chunk{
			TableSchema:       "public",
			TableName:         "orders",
			PartitionStrategy: PartitionStrategyOffset,
			ChunkSize:         100,
			ChunkStart:        0,
		}
		q := s.buildChunkQuery(chunk, "id", nil, "")
		assert.NotContains(t, q, "WHERE")
	})

	t.Run("ctid block with bounds injects condition", func(t *testing.T) {
		chunk := &Chunk{
			TableSchema:       "public",
			TableName:         "events",
			PartitionStrategy: PartitionStrategyCTIDBlock,
			BlockStart:        ptrInt64(0),
			BlockEnd:          ptrInt64(100),
		}
		q := s.buildChunkQuery(chunk, "", nil, "tenant_id = 7")
		assert.Contains(t, q, "WHERE ctid >= '(0,0)'::tid AND ctid < '(100,0)'::tid AND (tenant_id = 7)")
	})

	t.Run("ctid last chunk (nil BlockEnd) injects condition", func(t *testing.T) {
		chunk := &Chunk{
			TableSchema:       "public",
			TableName:         "events",
			PartitionStrategy: PartitionStrategyCTIDBlock,
			BlockStart:        ptrInt64(50),
			BlockEnd:          nil,
			IsLastChunk:       true,
		}
		q := s.buildChunkQuery(chunk, "", nil, "tenant_id = 7")
		assert.Contains(t, q, "WHERE ctid >= '(50,0)'::tid AND (tenant_id = 7)")
	})

	t.Run("ctid empty table with condition uses WHERE", func(t *testing.T) {
		chunk := &Chunk{
			TableSchema:       "public",
			TableName:         "events",
			PartitionStrategy: PartitionStrategyCTIDBlock,
		}
		q := s.buildChunkQuery(chunk, "", nil, "tenant_id = 7")
		assert.Contains(t, q, "FROM public.events WHERE (tenant_id = 7)")
	})

	t.Run("ctid empty table without condition has no WHERE", func(t *testing.T) {
		chunk := &Chunk{
			TableSchema:       "public",
			TableName:         "events",
			PartitionStrategy: PartitionStrategyCTIDBlock,
		}
		q := s.buildChunkQuery(chunk, "", nil, "")
		assert.NotContains(t, q, "WHERE")
	})

	t.Run("integer range without bounds falls back to offset with condition", func(t *testing.T) {
		chunk := &Chunk{
			TableSchema:       "public",
			TableName:         "users",
			PartitionStrategy: PartitionStrategyIntegerRange,
			ChunkSize:         10,
		}
		q := s.buildChunkQuery(chunk, "id", []string{"id"}, "is_active = true")
		assert.Contains(t, q, "WHERE (is_active = true)")
		assert.Contains(t, q, "ORDER BY id LIMIT 10")
	})
}

func TestBuildChunkQueryUsesPhysicalPartition(t *testing.T) {
	s := &Snapshotter{}

	tests := []struct {
		name    string
		chunk   *Chunk
		orderBy string
		pk      []string
	}{
		{
			name: "integer range",
			chunk: &Chunk{
				TableSchema:         "public",
				TableName:           "events",
				PhysicalTableSchema: "archive",
				PhysicalTableName:   "events_2026_01",
				PartitionStrategy:   PartitionStrategyIntegerRange,
				RangeStart:          ptrInt64(1),
				RangeEnd:            ptrInt64(10),
				ChunkSize:           10,
			},
			orderBy: "id",
			pk:      []string{"id"},
		},
		{
			name: "ctid block",
			chunk: &Chunk{
				TableSchema:         "public",
				TableName:           "events",
				PhysicalTableSchema: "archive",
				PhysicalTableName:   "events_2026_01",
				PartitionStrategy:   PartitionStrategyCTIDBlock,
				BlockStart:          ptrInt64(0),
				IsLastChunk:         true,
			},
		},
		{
			name: "offset",
			chunk: &Chunk{
				TableSchema:         "public",
				TableName:           "events",
				PhysicalTableSchema: "archive",
				PhysicalTableName:   "events_2026_01",
				PartitionStrategy:   PartitionStrategyOffset,
				ChunkSize:           10,
			},
			orderBy: "id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := s.buildChunkQuery(tt.chunk, tt.orderBy, tt.pk, "")
			assert.Contains(t, query, "FROM archive.events_2026_01")
			assert.NotContains(t, query, "FROM public.events")
		})
	}
}

func TestBuildChunkValueStringIncludesPhysicalPartition(t *testing.T) {
	s := &Snapshotter{}
	chunk := &Chunk{
		SlotName:            "snapshot",
		TableSchema:         "public",
		TableName:           "events",
		PhysicalTableSchema: "public",
		PhysicalTableName:   "events_2026_01",
		ChunkSize:           100,
		Status:              ChunkStatusPending,
		PartitionStrategy:   PartitionStrategyCTIDBlock,
	}

	value := s.buildChunkValueString(chunk)
	assert.Contains(t, value, "'public', 'events', 'public', 'events_2026_01'")

	chunk.PhysicalTableSchema = ""
	chunk.PhysicalTableName = ""
	value = s.buildChunkValueString(chunk)
	assert.Contains(t, value, "'public', 'events', NULL, NULL")
}

func TestCoordinatorFailureClosesExportBeforeRetry(t *testing.T) {
	logger.InitLogger(slog.Default())
	addr, opened, closed := startLifecyclePostgres(t, exportSnapshotResponse)
	s := &Snapshotter{dsn: "postgres://test:test@" + addr + "/test?sslmode=disable", metadataConn: failingMetadataConnection{}}
	defer s.Close(context.Background())

	for attempt := int32(1); attempt <= 3; attempt++ {
		if err := s.initializeCoordinator(t.Context(), "test", 0); err == nil {
			t.Fatal("expected metadata failure")
		}
		waitClosedConnections(t, opened, closed, attempt)
		if s.exportSnapshotConn != nil || s.keepaliveDone != nil || s.keepaliveCancel != nil {
			t.Fatal("failed attempt retained export resources")
		}
	}
}

func TestExportReplacementStopsPreviousKeepaliveAndClosesConnection(t *testing.T) {
	logger.InitLogger(slog.Default())
	var rollbacks atomic.Int32
	addr, opened, closed := startLifecyclePostgres(t, func(id int32, sql string, backend *pgproto3.Backend) bool {
		if sql == "ROLLBACK" {
			rollbacks.Add(1)
		}
		return exportSnapshotResponse(id, sql, backend)
	})
	s := &Snapshotter{dsn: "postgres://test:test@" + addr + "/test?sslmode=disable"}
	defer s.Close(context.Background())

	if err := s.exportSnapshotTransaction(t.Context()); err != nil {
		t.Fatal(err)
	}
	previous := s.exportSnapshotConn
	previousDone := s.keepaliveDone
	if err := s.exportSnapshotTransaction(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !previous.IsClosed() {
		t.Fatal("previous export connection is still open")
	}
	select {
	case <-previousDone:
	default:
		t.Fatal("previous keepalive still running")
	}
	currentDone := s.keepaliveDone
	s.Close(context.Background())
	select {
	case <-currentDone:
	default:
		t.Fatal("final keepalive still running")
	}
	waitClosedConnections(t, opened, closed, 2)
	if rollbacks.Load() != 2 {
		t.Fatalf("rollback count=%d want 2", rollbacks.Load())
	}
}

func TestCanceledExportReplacementCleansPreviousConnection(t *testing.T) {
	logger.InitLogger(slog.Default())
	addr, opened, closed := startLifecyclePostgres(t, exportSnapshotResponse)
	s := &Snapshotter{dsn: "postgres://test:test@" + addr + "/test?sslmode=disable"}
	defer s.Close(context.Background())

	if err := s.exportSnapshotTransaction(t.Context()); err != nil {
		t.Fatal(err)
	}
	previousDone := s.keepaliveDone
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := s.exportSnapshotTransaction(ctx); err == nil {
		t.Fatal("expected cancellation")
	}
	select {
	case <-previousDone:
	default:
		t.Fatal("previous keepalive still running")
	}
	waitClosedConnections(t, opened, closed, 1)
}

func exportSnapshotResponse(_ int32, sql string, backend *pgproto3.Backend) bool {
	if sql != "SELECT pg_export_snapshot()" {
		return false
	}
	backend.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{{Name: []byte("pg_export_snapshot"), DataTypeOID: 25, DataTypeSize: -1}}})
	backend.Send(&pgproto3.DataRow{Values: [][]byte{[]byte("snapshot")}})
	backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
	return true
}

func TestExportFailureCleansConnection(t *testing.T) {
	logger.InitLogger(slog.Default())
	addr, opened, closed := startLifecyclePostgres(t, func(_ int32, sql string, backend *pgproto3.Backend) bool {
		if sql != "SELECT pg_export_snapshot()" {
			return false
		}

		backend.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "42501", Message: "permission denied"})
		return true
	})
	s := &Snapshotter{dsn: "postgres://test:test@" + addr + "/test?sslmode=disable"}
	defer s.Close(context.Background())

	if err := s.exportSnapshotTransaction(t.Context()); err == nil {
		t.Fatal("expected export failure")
	}

	waitClosedConnections(t, opened, closed, 1)
	if s.exportSnapshotConn != nil || s.keepaliveDone != nil || s.keepaliveCancel != nil {
		t.Fatal("failed export retained resources")
	}
}
