package cdc

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/ad3n/go-pq-cdc/logger"
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
