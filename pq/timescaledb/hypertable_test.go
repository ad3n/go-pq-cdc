package timescaledb

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ad3n/go-pq-cdc/logger"
	"github.com/ad3n/go-pq-cdc/pq"
	"github.com/jackc/pgx/v5/pgconn"
)

type lifecycleConnection struct {
	pq.Connection
	entered  chan struct{}
	release  chan struct{}
	canceled chan error
	closes   atomic.Int32
}

func (c *lifecycleConnection) Close(context.Context) error {
	c.closes.Add(1)
	return nil
}

func (c *lifecycleConnection) Exec(ctx context.Context, sql string) *pgconn.MultiResultReader {
	if c.entered != nil {
		close(c.entered)
		select {
		case <-ctx.Done():
		case <-c.release:
		}

		c.canceled <- ctx.Err()
	}

	return (&pgconn.PgConn{}).Exec(ctx, sql)
}

func TestSyncCancellationClosesConnection(t *testing.T) {
	for i := range 32 {
		conn := &lifecycleConnection{}
		tdb := newLifecycleTimescale(conn)
		ctx, cancel := context.WithCancel(t.Context())
		finished := make(chan struct{})
		go func() {
			defer close(finished)

			tdb.SyncHyperTables(ctx)
		}()
		if i%2 == 0 {
			cancel()
		}

		if i%2 != 0 {
			tdb.cancel()
		}

		select {
		case <-finished:
		case <-time.After(time.Second):
			t.Fatal("Timescale worker ignored cancellation")
		}

		cancel()
		tdb.Close(t.Context())
		if conn.closes.Load() != 1 {
			t.Fatal("cancellation did not close the Timescale connection exactly once")
		}
	}
}

func TestCloseCancelsQueryAndStopsWorker(t *testing.T) {
	conn := &lifecycleConnection{entered: make(chan struct{}), release: make(chan struct{}), canceled: make(chan error, 1)}
	defer close(conn.release)

	tdb := newLifecycleTimescale(conn)
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)

		tdb.SyncHyperTables(t.Context())
	}()
	queryDone := make(chan struct{})
	go func() {
		defer close(queryDone)

		tdb.FindHyperTables(context.Background())
	}()
	select {
	case <-conn.entered:
	case <-time.After(time.Second):
		t.Fatal("Timescale query did not start")
	}

	closed := make(chan struct{})
	go func() {
		defer close(closed)

		tdb.Close(t.Context())
	}()
	for _, done := range []chan struct{}{closed, workerDone, queryDone} {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("Timescale shutdown left a query or worker running")
		}
	}

	if err := <-conn.canceled; err != context.Canceled {
		t.Fatalf("query was not canceled: %v", err)
	}

	if conn.closes.Load() != 1 {
		t.Fatal("Timescale connection was not closed exactly once")
	}
}

func TestConcurrentSyncAndClose(t *testing.T) {
	for range 32 {
		conn := &lifecycleConnection{}
		tdb := newLifecycleTimescale(conn)
		var workers sync.WaitGroup
		for range 8 {
			workers.Go(func() { tdb.SyncHyperTables(t.Context()) })
			workers.Go(func() { tdb.Close(t.Context()) })
		}

		finished := make(chan struct{})
		go func() {
			defer close(finished)

			workers.Wait()
		}()
		select {
		case <-finished:
		case <-time.After(time.Second):
			t.Fatal("concurrent Timescale admission and shutdown deadlocked")
		}

		tdb.SyncHyperTables(t.Context())
		if conn.closes.Load() != 1 {
			t.Fatal("concurrent shutdown did not close the connection exactly once")
		}
	}
}

func newLifecycleTimescale(conn pq.Connection) *TimescaleDB {
	logger.InitLogger(slog.Default())
	ctx, cancel := context.WithCancel(context.Background())
	return &TimescaleDB{conn: conn, ctx: ctx, cancel: cancel}
}
