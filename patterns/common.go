package patterns

import (
	"context"
	"errors"
)

// ErrAlreadyRun is returned when a single-shot pattern's Run method is
// invoked more than once on the same instance.
var ErrAlreadyRun = errors.New("pattern: already run")

// defaultBufferSize is the fallback used when a caller passes 0 or a
// negative value to a constructor that takes a buffer size.
const defaultBufferSize = 100

// maxRetainedErrors caps the in-memory error slice held by each pattern. The
// counter on the metrics side is unaffected, but a runaway faulty processor
// can't grow the slice without bound.
const maxRetainedErrors = 1024

// isContextErr reports whether err signals a context cancellation/timeout.
// These are expected outcomes (not real failures) and should not be counted
// against the pattern's error budget.
func isContextErr(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
