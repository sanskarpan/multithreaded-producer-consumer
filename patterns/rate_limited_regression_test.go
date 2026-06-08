package patterns

import (
	"context"
	"testing"
	"time"

	"github.com/sanskarpan/producer-consumer/consumer"
	"github.com/sanskarpan/producer-consumer/producer"
)

// TestRateLimitedPattern_ConsumerReceivesData is a regression test for the bug
// where the consumer passed to RateLimitedPattern was captured but never called.
// Every item produced must also be observed by the consumer.
func TestRateLimitedPattern_ConsumerReceivesData(t *testing.T) {
	const total = 30
	pattern := NewRateLimitedPattern(64, 200, 200)
	cons := consumer.NewAggregateConsumer("sink", 0)
	pattern.AddProducer(producer.NewIntProducer("source", 0, total, 0))
	pattern.AddConsumer(cons)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pattern.Run(ctx); err != nil {
		t.Fatalf("pattern run: %v", err)
	}

	produced, consumed := pattern.Stats()
	if produced != total {
		t.Fatalf("expected %d produced, got %d", total, produced)
	}
	if consumed != total {
		t.Fatalf("expected %d consumed, got %d", total, consumed)
	}
	if got := cons.ConsumedCount(); got != total {
		t.Fatalf("consumer ConsumedCount() = %d, want %d", got, total)
	}
}

// TestRateLimitedPattern_StatsDuringExecution verifies that Stats() returns
// live consumed counts during pattern execution, not just after Run returns.
func TestRateLimitedPattern_StatsDuringExecution(t *testing.T) {
	const total = 100
	pattern := NewRateLimitedPattern(64, 50, 50)
	cons := consumer.NewAggregateConsumer("sink", 0)
	pattern.AddProducer(producer.NewIntProducer("source", 0, total, 0))
	pattern.AddConsumer(cons)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	patternDone := make(chan error, 1)
	go func() {
		patternDone <- pattern.Run(ctx)
	}()

	// Poll Stats() periodically while the pattern runs.
	var lastConsumed int
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_, consumed := pattern.Stats()
		if consumed > lastConsumed {
			// Stats() reported a non-zero consumed count during execution.
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Wait for pattern to finish.
	err := <-patternDone
	if err != nil {
		t.Fatalf("pattern run: %v", err)
	}

	produced, consumed := pattern.Stats()
	if produced != total {
		t.Fatalf("expected %d produced, got %d", total, produced)
	}
	if consumed != total {
		t.Fatalf("expected %d consumed, got %d", total, consumed)
	}
}
