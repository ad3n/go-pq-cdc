package heartbeat

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ad3n/go-pq-cdc/config"
	"github.com/ad3n/go-pq-cdc/logger"
	"github.com/ad3n/go-pq-cdc/pq"
	"github.com/jackc/pgx/v5/pgconn"
)

type blockedHeartbeatConnection struct {
	pq.Connection
	entered  chan struct{}
	canceled chan struct{}
	release  chan struct{}
	closes   atomic.Int32
}

func (c *blockedHeartbeatConnection) Exec(ctx context.Context, sql string) *pgconn.MultiResultReader {
	close(c.entered)
	<-ctx.Done()
	close(c.canceled)
	<-c.release
	return (&pgconn.PgConn{}).Exec(ctx, sql)
}

func (c *blockedHeartbeatConnection) Close(context.Context) error {
	c.closes.Add(1)
	return nil
}

func TestHeartbeatCloseCancelsQueryAndJoinsWorker(t *testing.T) {
	logger.InitLogger(slog.Default())
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	conn := &blockedHeartbeatConnection{entered: make(chan struct{}), canceled: make(chan struct{}), release: make(chan struct{})}
	h := New("", config.HeartbeatConfig{Interval: time.Millisecond})
	h.conn = conn
	var release sync.Once
	defer func() {
		cancel()
		release.Do(func() { close(conn.release) })
		h.Close(context.Background())
	}()

	workerDone := make(chan struct{})
	go func() { h.Run(ctx); close(workerDone) }()
	select {
	case <-conn.entered:
	case <-time.After(time.Second):
		t.Fatal("heartbeat query did not start")
	}
	closed := make(chan struct{})
	go func() {
		var callers sync.WaitGroup
		for range 10 {
			callers.Go(func() { h.Close(context.Background()) })
		}
		callers.Wait()
		close(closed)
	}()
	select {
	case <-conn.canceled:
	case <-time.After(time.Second):
		t.Fatal("Close did not cancel heartbeat query")
	}
	select {
	case <-closed:
		t.Fatal("Close returned while worker was still in query")
	case <-time.After(20 * time.Millisecond):
	}
	release.Do(func() { close(conn.release) })
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close deadlocked")
	}
	select {
	case <-workerDone:
	case <-time.After(time.Second):
		t.Fatal("heartbeat worker did not exit")
	}
	if ctx.Err() != nil {
		t.Fatal("Close canceled the caller context")
	}
	if conn.closes.Load() != 1 {
		t.Fatalf("connection closes=%d want 1", conn.closes.Load())
	}
	var callers sync.WaitGroup
	for range 10 {
		callers.Go(func() { h.Close(context.Background()) })
	}
	callers.Wait()
	if conn.closes.Load() != 1 {
		t.Fatal("repeated Close closed connection again")
	}
}

func TestHeartbeatCloseStopsIdleAndLateWorkers(t *testing.T) {
	logger.InitLogger(slog.Default())
	for _, before := range []bool{false, true} {
		ctx, cancel := context.WithCancel(t.Context())
		h := New("", config.HeartbeatConfig{Interval: time.Hour})
		conn := &blockedHeartbeatConnection{}
		h.conn = conn
		if before {
			h.Close(context.Background())
		}
		done := make(chan struct{})
		go func() { h.Run(ctx); close(done) }()
		h.Close(context.Background())
		select {
		case <-done:
		case <-time.After(time.Second):
			cancel()
			<-done
			t.Fatal("heartbeat worker outlived Close")
		}
		cancel()
		if conn.closes.Load() != 1 {
			t.Fatalf("idle connection closes=%d want 1", conn.closes.Load())
		}
	}
}

func TestHeartbeatParentCancellationCleansConnection(t *testing.T) {
	logger.InitLogger(slog.Default())
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	conn := &blockedHeartbeatConnection{entered: make(chan struct{}), canceled: make(chan struct{}), release: make(chan struct{})}
	close(conn.release)
	h := New("", config.HeartbeatConfig{Interval: time.Millisecond})
	h.conn = conn
	defer h.Close(context.Background())

	done := make(chan struct{})
	go func() { h.Run(ctx); close(done) }()
	select {
	case <-conn.entered:
	case <-time.After(time.Second):
		t.Fatal("heartbeat query did not start")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not stop on parent cancellation")
	}
	if conn.closes.Load() != 1 || !h.closed.Load() {
		t.Fatalf("connection closes=%d heartbeat closed=%v", conn.closes.Load(), h.closed.Load())
	}
}
