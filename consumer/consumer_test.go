package consumer

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestBaseConsumer_ProcessorAndBaseAccessors guards the public API used by
// patterns.WorkerPool.SetWorker to extract the closure from a template.
func TestBaseConsumer_ProcessorAndBaseAccessors(t *testing.T) {
	called := 0
	fn := func(data interface{}) error { called++; return nil }
	c := NewBaseConsumer("test", 0, fn)

	if c.Processor() == nil {
		t.Fatal("Processor() returned nil")
	}
	if c.Base() != c {
		t.Fatal("Base() must return the receiver for direct *BaseConsumer users")
	}

	// Drain a pre-closed channel so Consume returns immediately.
	in := make(chan interface{})
	close(in)
	_ = c.Consume(context.Background(), in)
	if called != 0 {
		t.Fatal("processor should not be invoked on a closed channel")
	}
}

// TestPrintConsumer_BaseAccessor verifies the wrapper re-exports Base().
func TestPrintConsumer_BaseAccessor(t *testing.T) {
	c := NewPrintConsumer("p", 0)
	if c.Base() == nil {
		t.Fatal("PrintConsumer.Base() returned nil")
	}
}

// TestAggregateConsumer_BaseAccessor verifies the wrapper re-exports Base().
func TestAggregateConsumer_BaseAccessor(t *testing.T) {
	c := NewAggregateConsumer("a", 0)
	if c.Base() == nil {
		t.Fatal("AggregateConsumer.Base() returned nil")
	}
}

// TestWorkerConsumer_BaseAccessor verifies the wrapper re-exports Base().
func TestWorkerConsumer_BaseAccessor(t *testing.T) {
	c := NewWorkerConsumer("w", 0, 0, nil)
	if c.Base() == nil {
		t.Fatal("WorkerConsumer.Base() returned nil")
	}
}

// TestBatchConsumer_BaseAccessor verifies the wrapper re-exports Base().
func TestBatchConsumer_BaseAccessor(t *testing.T) {
	c := NewBatchConsumer("b", 5, 0, nil)
	if c.Base() == nil {
		t.Fatal("BatchConsumer.Base() returned nil")
	}
}

// TestBaseConsumer_PanicInProcessorIsCaught ensures a faulty processor does
// not bring down the consumer.
func TestBaseConsumer_PanicInProcessorIsCaught(t *testing.T) {
	c := NewBaseConsumer("p", 0, func(data interface{}) error {
		panic("boom")
	})
	in := make(chan interface{}, 1)
	in <- 42
	// Consume with a short context so the consumer exits cleanly.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err := c.Consume(ctx, in)
	// Consume should not propagate the panic; the channel will close when
	// the source is done, so we may get nil or a context error.
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.ErrorCount() == 0 {
		t.Fatal("expected error count to be incremented for panicked processor")
	}
}
