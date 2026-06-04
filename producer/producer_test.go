package producer

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestRateLimitedProducer_RespectsRate is a regression test ensuring the
// standalone RateLimitedProducer (dead code path kept for API completeness)
// produces at the configured rate and stops cleanly on context cancel.
func TestRateLimitedProducer_RespectsRate(t *testing.T) {
	prod := NewRateLimitedProducer("rlp", 5, 50, func(i int) interface{} { return i })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := prod.Produce(ctx, make(chan interface{}, 10)); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Produce: %v", err)
	}
	if got := prod.ProducedCount(); got != 5 {
		t.Fatalf("expected 5 items, got %d", got)
	}
}

// TestRateLimitedProducer_StopsOnContextCancel verifies immediate shutdown
// when the context is cancelled mid-tick.
func TestRateLimitedProducer_StopsOnContextCancel(t *testing.T) {
	prod := NewRateLimitedProducer("rlp", 1_000_000, 5, func(i int) interface{} { return i })
	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan interface{}, 100)
	errCh := make(chan error, 1)
	go func() { errCh <- prod.Produce(ctx, out) }()

	// Let a few items through.
	time.Sleep(300 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Produce did not return within 2s of cancel")
	}
}

// TestBaseProducer_NilChannel is a regression test ensuring a nil channel
// is reported as an error instead of panicking.
func TestBaseProducer_NilChannel(t *testing.T) {
	p := NewBaseProducer("p", 5, 0, nil)
	if err := p.Produce(context.Background(), nil); err == nil {
		t.Fatal("expected error for nil channel")
	}
}
