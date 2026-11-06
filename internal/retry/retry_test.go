package retry

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRetryStopsDuringDelay(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	attempted := make(chan struct{})
	done := make(chan error, 1)
	cfg := OnErrorConfig[int](5)
	go func() {
		_, err := cfg.Do(ctx, func() (int, error) { close(attempted); return 0, errors.New("retry") })
		done <- err
	}()
	<-attempted
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected cancellation, got %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("retry delay ignored cancellation")
	}
}

func TestRetryAttempts(t *testing.T) {
	cfg := OnErrorConfig[int](2)
	attempts := 0
	result, err := cfg.Do(t.Context(), func() (int, error) {
		attempts++
		if attempts == 1 {
			return 0, errors.New("transient")
		}
		return 42, nil
	})
	if err != nil || result != 42 || attempts != 2 {
		t.Fatalf("result=%d attempts=%d error=%v", result, attempts, err)
	}
}
