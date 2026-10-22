package processing

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"
)

// Retry never converts an infrastructure outage into a poison message.
// The caller stops without committing after transient retries are exhausted.
func Retry(ctx context.Context, attempts int, base time.Duration, fn func() error) (int, error) {
	var err error
	for n := 1; n <= attempts; n++ {
		if err = ctx.Err(); err != nil {
			return n - 1, err
		}
		err = fn()
		if err == nil {
			return n, nil
		}
		var permanent *PermanentError
		if errors.As(err, &permanent) || ctx.Err() != nil || n == attempts {
			return n, err
		}
		delay := base + time.Duration(rand.Int64N(int64(base/2)+1))
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return n, ctx.Err()
		case <-timer.C:
		}
		base *= 2
		if base > 10*time.Second {
			base = 10 * time.Second
		}
	}
	return 0, errors.New("attempts must be positive")
}
