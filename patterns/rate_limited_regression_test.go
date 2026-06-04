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
