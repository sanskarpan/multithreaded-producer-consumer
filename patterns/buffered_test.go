package patterns

import (
	"context"
	"testing"
	"time"

	"github.com/sanskarpan/producer-consumer/consumer"
	"github.com/sanskarpan/producer-consumer/producer"
)

func TestBufferedPattern_BasicOperation(t *testing.T) {
	pattern := NewBufferedPattern(50)

	prod := producer.NewIntProducer("p1", 0, 100, 0)
	cons := consumer.NewAggregateConsumer("c1", 0)

	pattern.AddProducer(prod)
	pattern.AddConsumer(cons)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := pattern.Run(ctx); err != nil {
		t.Fatalf("Pattern run failed: %v", err)
	}

	if prod.ProducedCount() != 100 {
		t.Errorf("Expected 100 items produced, got %d", prod.ProducedCount())
	}
	if cons.ConsumedCount() != 100 {
		t.Errorf("Expected 100 items consumed, got %d", cons.ConsumedCount())
	}
}

func TestBufferedPattern_BufferSize(t *testing.T) {
	bufferSize := 25
	pattern := NewBufferedPattern(bufferSize)

	if pattern.BufferSize() != bufferSize {
		t.Errorf("Expected buffer size %d, got %d", bufferSize, pattern.BufferSize())
	}

	if pattern.ChannelCap() != bufferSize {
		t.Errorf("Expected channel capacity %d, got %d", bufferSize, pattern.ChannelCap())
	}
}

func TestBufferedPattern_DefaultBufferSize(t *testing.T) {
	pattern := NewBufferedPattern(0)

	if pattern.BufferSize() != 100 {
		t.Errorf("Expected default buffer size 100, got %d", pattern.BufferSize())
	}
}

func TestBufferedPattern_FastProducerSlowConsumer(t *testing.T) {
	pattern := NewBufferedPattern(10)

	prod := producer.NewIntProducer("p1", 0, 50, 0)                 // Fast producer
	cons := consumer.NewAggregateConsumer("c1", 5*time.Millisecond) // Slow consumer

	pattern.AddProducer(prod)
	pattern.AddConsumer(cons)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := pattern.Run(ctx); err != nil {
		t.Fatalf("Pattern run failed: %v", err)
	}

	if prod.ProducedCount() != 50 {
		t.Errorf("Expected 50 items produced, got %d", prod.ProducedCount())
	}
	if cons.ConsumedCount() != 50 {
		t.Errorf("Expected 50 items consumed, got %d", cons.ConsumedCount())
	}
}

func TestBufferedPattern_MultipleProducersConsumers(t *testing.T) {
	pattern := NewBufferedPattern(100)

	// Add multiple producers
	for i := 0; i < 3; i++ {
		prod := producer.NewIntProducer(string(rune('a'+i)), i*10, 10, 0)
		pattern.AddProducer(prod)
	}

	// Add multiple consumers
	for i := 0; i < 3; i++ {
		cons := consumer.NewAggregateConsumer(string(rune('A'+i)), 0)
		pattern.AddConsumer(cons)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := pattern.Run(ctx); err != nil {
		t.Fatalf("Pattern run failed: %v", err)
	}
}
