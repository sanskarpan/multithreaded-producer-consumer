package patterns

import (
	"context"
	"testing"
	"time"

	"github.com/sanskarpan/producer-consumer/consumer"
	"github.com/sanskarpan/producer-consumer/producer"
)

func TestBasicPattern_SingleProducerSingleConsumer(t *testing.T) {
	pattern := NewBasicPattern()

	// Create producer and consumer
	prod := producer.NewIntProducer("p1", 0, 10, 0)
	cons := consumer.NewAggregateConsumer("c1", 0)

	pattern.AddProducer(prod)
	pattern.AddConsumer(cons)

	// Run with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := pattern.Run(ctx); err != nil {
		t.Fatalf("Pattern run failed: %v", err)
	}

	// Verify
	if prod.ProducedCount() != 10 {
		t.Errorf("Expected 10 items produced, got %d", prod.ProducedCount())
	}
	if cons.ConsumedCount() != 10 {
		t.Errorf("Expected 10 items consumed, got %d", cons.ConsumedCount())
	}
}

func TestBasicPattern_MultipleProducersMultipleConsumers(t *testing.T) {
	pattern := NewBasicPattern()

	// Create multiple producers
	prod1 := producer.NewIntProducer("p1", 0, 5, 0)
	prod2 := producer.NewIntProducer("p2", 100, 5, 0)
	pattern.AddProducer(prod1)
	pattern.AddProducer(prod2)

	// Create multiple consumers
	cons1 := consumer.NewAggregateConsumer("c1", 0)
	cons2 := consumer.NewAggregateConsumer("c2", 0)
	pattern.AddConsumer(cons1)
	pattern.AddConsumer(cons2)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := pattern.Run(ctx); err != nil {
		t.Fatalf("Pattern run failed: %v", err)
	}

	// Verify total production and consumption
	totalProduced := prod1.ProducedCount() + prod2.ProducedCount()
	totalConsumed := cons1.ConsumedCount() + cons2.ConsumedCount()

	if totalProduced != 10 {
		t.Errorf("Expected 10 items produced, got %d", totalProduced)
	}
	if totalConsumed != 10 {
		t.Errorf("Expected 10 items consumed, got %d", totalConsumed)
	}
}

func TestBasicPattern_ContextCancellation(t *testing.T) {
	pattern := NewBasicPattern()

	prod := producer.NewIntProducer("p1", 0, 1000, 10*time.Millisecond)
	cons := consumer.NewAggregateConsumer("c1", 0)

	pattern.AddProducer(prod)
	pattern.AddConsumer(cons)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_ = pattern.Run(ctx)

	// Should be cancelled before producing all 1000 items
	if prod.ProducedCount() >= 1000 {
		t.Errorf("Expected cancellation before 1000 items, got %d", prod.ProducedCount())
	}
}

func TestBasicPattern_NoProducers(t *testing.T) {
	pattern := NewBasicPattern()
	cons := consumer.NewAggregateConsumer("c1", 0)
	pattern.AddConsumer(cons)

	ctx := context.Background()
	err := pattern.Run(ctx)

	if err == nil {
		t.Error("Expected error when no producers configured")
	}
}

func TestBasicPattern_NoConsumers(t *testing.T) {
	pattern := NewBasicPattern()
	prod := producer.NewIntProducer("p1", 0, 10, 0)
	pattern.AddProducer(prod)

	ctx := context.Background()
	err := pattern.Run(ctx)

	if err == nil {
		t.Error("Expected error when no consumers configured")
	}
}
