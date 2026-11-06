package slot

import (
	"context"
	goerrors "errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/ad3n/go-pq-cdc/logger"
	"github.com/ad3n/go-pq-cdc/pq"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

type blockedMetricsConn struct {
	pq.Connection
	entered  chan struct{}
	release  chan struct{}
	canceled chan error
	closed   bool
}

func (c *blockedMetricsConn) IsClosed() bool { return c.closed }

func (c *blockedMetricsConn) Close(context.Context) error {
	c.closed = true
	return nil
}

func (c *blockedMetricsConn) Exec(ctx context.Context, sql string) *pgconn.MultiResultReader {
	close(c.entered)
	select {
	case <-ctx.Done():
	case <-c.release:
	}

	c.canceled <- ctx.Err()
	return (&pgconn.PgConn{}).Exec(ctx, sql)
}

func TestReplicationSlotCommandsQuoteTheName(t *testing.T) {
	if got := createReplicationSlotCommand("test-slot", false); got != `CREATE_REPLICATION_SLOT "test-slot" LOGICAL pgoutput` {
		t.Fatalf("create: got %s", got)
	}
	if got := createReplicationSlotCommand("test_slot", true); got != `CREATE_REPLICATION_SLOT "test_slot" LOGICAL pgoutput (FAILOVER true)` {
		t.Fatalf("create failover: got %s", got)
	}
	if got := alterReplicationSlotFailoverCommand("test-slot"); got != `ALTER_REPLICATION_SLOT "test-slot" (FAILOVER true)` {
		t.Fatalf("alter: got %s", got)
	}
}

// TestConfigValidateAcceptsAutoLiteral keeps the documented default usable: a
// config file may spell the default as "auto" instead of leaving it empty.
func TestConfigValidateAcceptsAutoLiteral(t *testing.T) {
	cfg := Config{Name: "cdc_slot", SlotActivityCheckerInterval: 3000, ProtoVersion: 2, LogicalMessages: "auto"}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("auto must be accepted as the documented default: %v", err)
	}
}

// fakeSlotConn is a pq.Connection whose Exec always fails: Exec on a
// zero-value pgconn is rejected by its status check and yields a closed,
// error-carrying MultiResultReader without panicking. The tests assert on the
// reconnect/retry decisions via the call counters, not on query results.
type fakeSlotConn struct {
	closed       bool
	connectErr   error
	connectCalls int
	execCalls    int
	execHook     func(call int)
}

func (c *fakeSlotConn) Connect(context.Context) error {
	c.connectCalls++
	if c.connectErr != nil {
		return c.connectErr
	}
	c.closed = false
	return nil
}

func (c *fakeSlotConn) IsClosed() bool { return c.closed }

func (c *fakeSlotConn) Close(context.Context) error {
	c.closed = true
	return nil
}

func (c *fakeSlotConn) ReceiveMessage(context.Context) (pgproto3.BackendMessage, error) {
	return nil, goerrors.New("unused")
}

func (c *fakeSlotConn) Frontend() *pgproto3.Frontend { return nil }

func (c *fakeSlotConn) Exec(ctx context.Context, sql string) *pgconn.MultiResultReader {
	c.execCalls++
	if c.execHook != nil {
		c.execHook(c.execCalls)
	}
	return (&pgconn.PgConn{}).Exec(ctx, sql)
}

func newTestSlot(conn pq.Connection) *Slot {
	logger.InitLogger(logger.NewSlog(slog.LevelError))
	return &Slot{
		cfg:       Config{Name: "test_slot"},
		conn:      conn,
		statusSQL: "SELECT 1",
	}
}

func TestInfoReconnectsClosedConnectionBeforeQuery(t *testing.T) {
	conn := &fakeSlotConn{closed: true}
	s := newTestSlot(conn)

	_, err := s.Info(context.Background())
	if err == nil {
		t.Fatal("expected query error to surface after pre-reconnect")
	}
	if conn.connectCalls != 1 {
		t.Fatalf("expected 1 connect (pre-reconnect), got %d", conn.connectCalls)
	}
	if conn.execCalls != 1 {
		t.Fatalf("expected exactly 1 exec after pre-reconnect, got %d", conn.execCalls)
	}
}

func TestInfoRetriesOnceAfterConnectionDiesMidQuery(t *testing.T) {
	conn := &fakeSlotConn{}
	// First exec kills the connection mid-query; the retry runs on the
	// reconnected conn (hook cleared) and its error surfaces.
	conn.execHook = func(call int) {
		if call == 1 {
			conn.closed = true
			conn.execHook = nil
		}
	}
	s := newTestSlot(conn)

	_, err := s.Info(context.Background())
	if err == nil {
		t.Fatal("expected retry error to surface")
	}
	if conn.connectCalls != 1 {
		t.Fatalf("expected 1 reconnect, got %d", conn.connectCalls)
	}
	if conn.execCalls != 2 {
		t.Fatalf("expected exactly 2 execs (initial + one retry), got %d", conn.execCalls)
	}
}

func TestInfoDoesNotRetryQueryErrorOnLiveConnection(t *testing.T) {
	conn := &fakeSlotConn{}
	s := newTestSlot(conn)

	_, err := s.Info(context.Background())
	if err == nil {
		t.Fatal("expected query error to surface")
	}
	if conn.connectCalls != 0 {
		t.Fatalf("expected no reconnect for a non-connection error, got %d", conn.connectCalls)
	}
	if conn.execCalls != 1 {
		t.Fatalf("expected exactly 1 exec, got %d", conn.execCalls)
	}
}

func TestInfoReturnsErrorWhenReconnectFails(t *testing.T) {
	conn := &fakeSlotConn{connectErr: goerrors.New("dial failed")}
	conn.execHook = func(int) { conn.closed = true }
	s := newTestSlot(conn)

	_, err := s.Info(context.Background())
	if err == nil {
		t.Fatal("expected reconnect error to surface")
	}
	if conn.connectCalls != 1 {
		t.Fatalf("expected 1 reconnect attempt, got %d", conn.connectCalls)
	}
	if conn.execCalls != 1 {
		t.Fatalf("expected no query retry after failed reconnect, got %d execs", conn.execCalls)
	}
}

func TestInfoDuringShutdownDoesNotReconnect(t *testing.T) {
	conn := &fakeSlotConn{}
	s := newTestSlot(conn)
	// Close() flips closed outside the mutex; simulate it landing mid-query.
	conn.execHook = func(int) {
		conn.closed = true
		s.closed.Store(true)
	}

	_, err := s.Info(context.Background())
	if !goerrors.Is(err, ErrorSlotClosed) {
		t.Fatalf("expected ErrorSlotClosed, got %v", err)
	}
	if conn.connectCalls != 0 {
		t.Fatalf("expected no reconnect during shutdown, got %d", conn.connectCalls)
	}
}

func TestInfoOnClosedSlotReturnsSlotClosed(t *testing.T) {
	conn := &fakeSlotConn{}
	s := newTestSlot(conn)
	s.closed.Store(true)

	_, err := s.Info(context.Background())
	if !goerrors.Is(err, ErrorSlotClosed) {
		t.Fatalf("expected ErrorSlotClosed, got %v", err)
	}
	if conn.execCalls != 0 || conn.connectCalls != 0 {
		t.Fatalf("expected no query activity on closed slot, got %d execs, %d connects", conn.execCalls, conn.connectCalls)
	}
}

// A physical / not-yet-reserved slot reports an empty confirmed_flush_lsn.
// It must not blow up with a cryptic "lsn parse: EOF"; the empty column is
// skipped and the logical-type check yields a clear error instead.
func TestDecodeSlotInfoResult_EmptyConfirmedFlushLSN(t *testing.T) {
	result := &pgconn.Result{
		FieldDescriptions: []pgconn.FieldDescription{
			{Name: "slot_name", DataTypeOID: pgtype.TextOID},
			{Name: "slot_type", DataTypeOID: pgtype.TextOID},
			{Name: "restart_lsn", DataTypeOID: pgtype.TextOID},
			{Name: "confirmed_flush_lsn", DataTypeOID: pgtype.TextOID},
		},
		Rows: [][][]byte{{
			[]byte("contents_to_contentmedias_slot"),
			[]byte("physical"),
			[]byte("0/1A2B3C4"),
			[]byte(""), // NULL pg_lsn decoded as empty string
		}},
	}

	info, err := decodeSlotInfoResult(result)
	if err != nil {
		t.Fatalf("expected empty confirmed_flush_lsn to be skipped, got: %v", err)
	}
	if info.ConfirmedFlushLSN != 0 {
		t.Fatalf("expected zero ConfirmedFlushLSN, got: %v", info.ConfirmedFlushLSN)
	}
	if info.Type != Physical {
		t.Fatalf("expected Physical type, got: %v", info.Type)
	}
}

func TestMetricsStopsAfterCancellationOrClose(t *testing.T) {
	for _, cancelParent := range []bool{false, true} {
		ctx, cancel := context.WithCancel(t.Context())
		s := NewSlot("", "", Config{SlotActivityCheckerInterval: 3600000}, nil, nil)
		s.conn = &fakeSlotConn{closed: true}
		finished := make(chan struct{})
		go func() {
			defer close(finished)

			s.Metrics(ctx)
		}()
		if cancelParent {
			cancel()
		}

		if !cancelParent {
			s.Close(t.Context())
		}

		select {
		case <-finished:
		case <-time.After(time.Second):
			s.closed.Store(true)
			s.ticker.Reset(time.Millisecond)
			<-finished
			t.Error("metrics worker did not stop after cancellation or Close")
		}

		cancel()
		s.Close(t.Context())
	}
}

func TestMetricsConcurrentAdmissionAndClose(t *testing.T) {
	for range 32 {
		s := NewSlot("", "", Config{SlotActivityCheckerInterval: 3600000}, nil, nil)
		s.conn = &fakeSlotConn{closed: true}
		var workers sync.WaitGroup
		for range 8 {
			workers.Go(func() { s.Metrics(t.Context()) })
			workers.Go(func() { s.Close(t.Context()) })
		}

		finished := make(chan struct{})
		go func() {
			defer close(finished)

			workers.Wait()
		}()
		select {
		case <-finished:
		case <-time.After(time.Second):
			t.Fatal("concurrent metrics admission and Close deadlocked")
		}
	}
}

func TestCloseCancelsActiveMetricsQueryAndJoinsWorker(t *testing.T) {
	conn := &blockedMetricsConn{entered: make(chan struct{}), release: make(chan struct{}), canceled: make(chan error, 1)}
	defer close(conn.release)

	s := NewSlot("", "", Config{SlotActivityCheckerInterval: 1}, nil, nil)
	s.conn = conn
	finished := make(chan struct{})
	go func() {
		defer close(finished)

		s.Metrics(t.Context())
	}()
	select {
	case <-conn.entered:
	case <-time.After(time.Second):
		t.Fatal("metrics query did not start")
	}

	closed := make(chan struct{})
	go func() {
		defer close(closed)

		s.Close(t.Context())
	}()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close deadlocked waiting for the metrics query mutex")
	}

	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("metrics worker survived Close")
	}

	if err := <-conn.canceled; !goerrors.Is(err, context.Canceled) {
		t.Fatalf("Close did not cancel the metrics query: %v", err)
	}
}
