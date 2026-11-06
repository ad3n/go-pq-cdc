package retry

import (
	"context"
	"time"

	"github.com/avast/retry-go/v4"
)

var DefaultOptions = []retry.Option{
	retry.LastErrorOnly(true),
	retry.Delay(time.Second),
	retry.DelayType(retry.FixedDelay),
}

type Config[T any] struct {
	Options []retry.Option
}

func (rc Config[T]) Do(ctx context.Context, f retry.RetryableFuncWithData[T]) (T, error) {
	options := append([]retry.Option{retry.Context(ctx)}, rc.Options...)
	return retry.DoWithData(f, options...)
}

func OnErrorConfig[T any](attemptCount uint) Config[T] {
	cfg := Config[T]{
		Options: []retry.Option{retry.Attempts(attemptCount)},
	}
	cfg.Options = append(cfg.Options, DefaultOptions...)
	return cfg
}
