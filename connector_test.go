package cdc

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/logger"
)

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
