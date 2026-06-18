package main

import (
	"context"
	"time"
)

func retry(ctx context.Context, attempts int, baseDelay time.Duration, fn func() error) error {
	var last error
	for attempt := range attempts {
		if err := fn(); err != nil {
			last = err
		} else {
			return nil
		}
		delay := baseDelay * time.Duration(1<<min(attempt, 5))
		if err := sleepContext(ctx, delay); err != nil {
			if last != nil {
				return last
			}
			return err
		}
	}
	return last
}

func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func sendErr(errCh chan<- error, err error) {
	select {
	case errCh <- err:
	default:
	}
}

func firstErr(errCh <-chan error) error {
	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}
