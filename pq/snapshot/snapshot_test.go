package snapshot

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ad3n/go-pq-cdc/config"
	"github.com/ad3n/go-pq-cdc/logger"
	"github.com/jackc/pgx/v5/pgproto3"
)

func TestNewRollsBackPartialConnections(t *testing.T) {
	logger.InitLogger(slog.Default())
	for _, failureAt := range []int32{2, 3, 6} {
		t.Run(fmt.Sprint(failureAt), func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			failed := make(chan struct{})
			addr, opened, closed := startLifecyclePostgres(t, func(id int32, sql string, backend *pgproto3.Backend) bool {
				if id != failureAt {
					return false
				}
				close(failed)
				backend.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "XX000", Message: "initialization failure"})
				return true
			})
			done := make(chan error, 1)
			go func() {
				_, err := New(ctx, config.SnapshotConfig{}, nil, "postgres://test:test@"+addr+"/test?sslmode=disable", nil)
				done <- err
			}()
			select {
			case <-failed:
			case <-time.After(time.Second):
				t.Fatal("failure point not reached")
			}
			deadline := time.Now().Add(500 * time.Millisecond)
			for closed.Load() == 0 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if closed.Load() == 0 {
				t.Fatal("failed connection was not closed")
			}
			cancel()
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("expected initialization failure")
				}
			case <-time.After(time.Second):
				t.Fatal("snapshot initialization did not stop")
			}
			waitClosedConnections(t, opened, closed, failureAt)
		})
	}
}

func TestNewTransfersConnectionOwnership(t *testing.T) {
	logger.InitLogger(slog.Default())
	addr, opened, closed := startLifecyclePostgres(t, func(int32, string, *pgproto3.Backend) bool { return false })
	s, err := New(t.Context(), config.SnapshotConfig{}, nil, "postgres://test:test@"+addr+"/test?sslmode=disable", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(context.Background())
	if opened.Load() != 7 || closed.Load() != 0 {
		t.Fatalf("premature cleanup: opened=%d closed=%d", opened.Load(), closed.Load())
	}
	s.Close(context.Background())
	waitClosedConnections(t, opened, closed, 7)
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
