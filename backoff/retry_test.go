package backoff_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/m-lab/pusher/backoff"
)

func TestRetry(t *testing.T) {
	count := 0
	backoff.Retry(
		func() error {
			if count < 5 {
				count++
				return fmt.Errorf("Count was %d (and was < 5)", count)
			}
			return nil
		},
		time.Duration(1)*time.Millisecond,
		time.Duration(10)*time.Millisecond,
		"test",
	)
}
