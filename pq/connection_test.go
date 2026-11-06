package pq

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

func TestConnectClosesFailedPingBeforeRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	failed := make(chan struct{})
	addr, opened, closed := startLifecyclePostgres(t, func(id int32, sql string, backend *pgproto3.Backend) bool {
		backend.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "XX000", Message: "ping failure"})
		close(failed)
		return true
	})
	done := make(chan error, 1)
	go func() {
		_, err := NewConnection(ctx, "postgres://test:test@"+addr+"/test?sslmode=disable")
		done <- err
	}()
	select {
	case <-failed:
	case <-time.After(time.Second):
		t.Fatal("ping did not execute")
	}
	waitClosedConnections(t, opened, closed, 1)
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected ping failure")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("connection retry ignored cancellation")
	}
}

func startLifecyclePostgres(t *testing.T, query func(int32, string, *pgproto3.Backend) bool) (string, *atomic.Int32, *atomic.Int32) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var opened, closed atomic.Int32
	var connections sync.Map
	var workers sync.WaitGroup
	accepted := make(chan struct{})
	go func() {
		defer close(accepted)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			id := opened.Add(1)
			connections.Store(conn, struct{}{})
			workers.Add(1)
			go func() {
				defer func() { conn.Close(); connections.Delete(conn); closed.Add(1); workers.Done() }()
				conn.SetDeadline(time.Now().Add(5 * time.Second))
				backend := pgproto3.NewBackend(conn, conn)
				msg, err := backend.ReceiveStartupMessage()
				if err != nil {
					return
				}
				if _, ok := msg.(*pgproto3.SSLRequest); ok {
					if _, err = conn.Write([]byte("N")); err != nil {
						return
					}
					msg, err = backend.ReceiveStartupMessage()
					if err != nil {
						return
					}
				}
				if _, ok := msg.(*pgproto3.StartupMessage); !ok {
					return
				}
				backend.Send(&pgproto3.AuthenticationOk{})
				backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
				if err = backend.Flush(); err != nil {
					return
				}
				for {
					msg, err := backend.Receive()
					if err != nil {
						return
					}
					if _, ok := msg.(*pgproto3.Terminate); ok {
						return
					}
					q, ok := msg.(*pgproto3.Query)
					if !ok {
						return
					}
					if !query(id, q.String, backend) {
						backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 0")})
					}
					backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
					if err = backend.Flush(); err != nil {
						return
					}
				}
			}()
		}
	}()
	t.Cleanup(func() {
		listener.Close()
		<-accepted
		connections.Range(func(key, value any) bool { key.(net.Conn).Close(); return true })
		workers.Wait()
	})
	return listener.Addr().String(), &opened, &closed
}

func waitClosedConnections(t *testing.T, opened, closed *atomic.Int32, expected int32) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for closed.Load() != expected && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if opened.Load() != expected || closed.Load() != expected {
		t.Fatalf("connections opened=%d closed=%d, want %d each", opened.Load(), closed.Load(), expected)
	}
}
