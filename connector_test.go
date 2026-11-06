package cdc

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/ad3n/go-pq-cdc/config"
	"github.com/ad3n/go-pq-cdc/logger"
	"github.com/ad3n/go-pq-cdc/pq/publication"
	"github.com/ad3n/go-pq-cdc/pq/slot"
	"github.com/jackc/pgx/v5/pgproto3"
)

type shutdownServer struct{}

func (*shutdownServer) Listen() {}

func (*shutdownServer) Shutdown() {}

func TestCaptureSlotStopsOnCancellation(t *testing.T) {
	logger.InitLogger(slog.Default())
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	finished := make(chan struct{})
	go func() {
		defer close(finished)

		(&connector{}).CaptureSlot(ctx)
	}()
	select {
	case <-finished:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("slot capture ignored cancellation")
	}
}

func TestWaitUntilReadyObservesClosedConnector(t *testing.T) {
	c := &connector{readyCh: make(chan struct{}), cancelCh: make(chan os.Signal)}
	close(c.cancelCh)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	if err := c.WaitUntilReady(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("closed connector readiness returned %v", err)
	}
}

func TestCloseWithPendingSignalReleasesSourceWorker(t *testing.T) {
	logger.InitLogger(slog.Default())
	for _, sig := range []os.Signal{os.Interrupt, syscall.SIGTERM} {
		t.Run(sig.String(), func(t *testing.T) {
			c := &connector{cancelCh: make(chan os.Signal, 1), server: &shutdownServer{}}
			c.cancelCh <- sig
			c.Close()
			done := make(chan struct{})
			go func() {
				defer close(done)

				for range c.cancelCh {
				}
			}()
			select {
			case <-done:
			case <-time.After(time.Second):
				close(c.cancelCh)
				<-done
				t.Fatal("Close consumed the signal without closing the channel")
			}

			var callers sync.WaitGroup
			for range 8 {
				callers.Go(c.Close)
			}

			callers.Wait()
		})
	}
}

func TestNewConnectorRollsBackSnapshotOnTimescaleFailure(t *testing.T) {
	t.Setenv("PGSSLMODE", "disable")
	addr, opened, closed := startLifecyclePostgres(t, func(id int32, sql string, backend *pgproto3.Backend) bool {
		if strings.Contains(sql, "timescaledb_information") {
			backend.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "XX000", Message: "timescale initialization failure"})
			return true
		}
		if strings.Contains(sql, "pg_publication") {
			backend.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{{Name: []byte("pubname"), DataTypeOID: 25, DataTypeSize: -1}, {Name: []byte("pubtables"), DataTypeOID: 1009, DataTypeSize: -1}}})
			backend.Send(&pgproto3.DataRow{Values: [][]byte{[]byte("test"), []byte("{public.test}")}})
			backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
			return true
		}
		return false
	})
	host, portText, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{Host: host, Port: port, Username: "test", Password: "test", Database: "test",
		Publication: publication.Config{Name: "test"}, Slot: slot.Config{Name: "test"},
		Snapshot:         config.SnapshotConfig{Enabled: true, Tables: publication.Tables{{Schema: "public", Name: "test"}}},
		ExtensionSupport: config.ExtensionSupport{EnableTimeScaleDB: true}}
	c, err := NewConnector(t.Context(), cfg, nil)
	if err == nil || c != nil {
		t.Fatalf("NewConnector returned %v, %v", c, err)
	}
	if !strings.Contains(err.Error(), "timescale initialization failure") {
		t.Fatalf("unexpected error: %v", err)
	}
	waitClosedConnections(t, opened, closed, 9)
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
