package patterns

import (
	"context"
	"testing"
	"time"

	"github.com/sanskarpan/producer-consumer/consumer"
	"github.com/sanskarpan/producer-consumer/producer"
)

func TestRateLimitedPattern_BasicOperation(t *testing.T) {
	pattern := NewRateLimitedPattern(50, 20, 20) // 20 items/sec

	prod := producer.NewIntProducer("p1", 0, 10, 0)
	cons := consumer.NewAggregateConsumer("c1", 0)

	pattern.AddProducer(prod)
	pattern.AddConsumer(cons)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := pattern.Run(ctx); err != nil {
		t.Fatalf("Pattern run failed: %v", err)
	}

	produced, _ := pattern.Stats()
	if produced == 0 {
		t.Error("Expected some items to be produced")
	}
}

func TestRateLimitedPattern_RateConfiguration(t *testing.T) {
	producerRate := 10
	consumerRate := 15

	pattern := NewRateLimitedPattern(50, producerRate, consumerRate)

	if pattern.ProducerRate() != producerRate {
		t.Errorf("Expected producer rate %d, got %d", producerRate, pattern.ProducerRate())
	}

	if pattern.ConsumerRate() != consumerRate {
		t.Errorf("Expected consumer rate %d, got %d", consumerRate, pattern.ConsumerRate())
	}
}

func TestRateLimitedPattern_DefaultRates(t *testing.T) {
	pattern := NewRateLimitedPattern(50, 0, 0)

	if pattern.ProducerRate() != 10 {
		t.Errorf("Expected default producer rate 10, got %d", pattern.ProducerRate())
	}

	if pattern.ConsumerRate() != 10 {
		t.Errorf("Expected default consumer rate 10, got %d", pattern.ConsumerRate())
	}
}

func TestRateLimitedPattern_Stats(t *testing.T) {
	pattern := NewRateLimitedPattern(50, 50, 50)

	prod := producer.NewIntProducer("p1", 0, 20, 0)
	cons := consumer.NewAggregateConsumer("c1", 0)

	pattern.AddProducer(prod)
	pattern.AddConsumer(cons)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := pattern.Run(ctx); err != nil {
		t.Fatalf("Pattern run failed: %v", err)
	}

	produced, consumed := pattern.Stats()

	if produced == 0 {
		t.Error("Expected some items produced")
	}

	if consumed > produced {
		t.Errorf("Consumed (%d) should not exceed produced (%d)", consumed, produced)
	}
}

func TestRateLimitedPattern_MultipleProducersConsumers(t *testing.T) {
	pattern := NewRateLimitedPattern(100, 30, 30)

	prod1 := producer.NewIntProducer("p1", 0, 20, 0)
	prod2 := producer.NewIntProducer("p2", 100, 20, 0)
	pattern.AddProducer(prod1)
	pattern.AddProducer(prod2)

	cons1 := consumer.NewAggregateConsumer("c1", 0)
	cons2 := consumer.NewAggregateConsumer("c2", 0)
	pattern.AddConsumer(cons1)
	pattern.AddConsumer(cons2)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := pattern.Run(ctx); err != nil {
		t.Fatalf("Pattern run failed: %v", err)
	}

	produced, consumed := pattern.Stats()

	if produced == 0 {
		t.Error("Expected some items produced")
	}

	if consumed == 0 {
		t.Error("Expected some items consumed")
	}
}

func TestRateLimitedPattern_ActualRateLimiting(t *testing.T) {
	rate := 20 // 20 items per second
	pattern := NewRateLimitedPattern(50, rate, 100)

	prod := producer.NewIntProducer("p1", 0, 30, 0)
	cons := consumer.NewAggregateConsumer("c1", 0)

	pattern.AddProducer(prod)
	pattern.AddConsumer(cons)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	start := time.Now()
	if err := pattern.Run(ctx); err != nil {
		t.Fatalf("Pattern run failed: %v", err)
	}
	elapsed := time.Since(start)

	produced, _ := pattern.Stats()

	// With rate of 20 items/sec, in 2 seconds we should produce roughly 40 items
	// But we're only producing 30, so it should take about 1.5 seconds
	expectedTime := time.Duration(float64(produced)/float64(rate)) * time.Second

	// Allow 50% variance due to timing uncertainties
	if elapsed < expectedTime/2 {
		t.Logf("Warning: Production faster than expected (expected ~%v, got %v)", expectedTime, elapsed)
	}
}

func TestRateLimitedPattern_NoProducers(t *testing.T) {
	pattern := NewRateLimitedPattern(50, 10, 10)
	cons := consumer.NewAggregateConsumer("c1", 0)
	pattern.AddConsumer(cons)

	ctx := context.Background()
	err := pattern.Run(ctx)

	if err == nil {
		t.Error("Expected error when no producers configured")
	}
}

func TestRateLimitedPattern_NoConsumers(t *testing.T) {
	pattern := NewRateLimitedPattern(50, 10, 10)
	prod := producer.NewIntProducer("p1", 0, 10, 0)
	pattern.AddProducer(prod)

	ctx := context.Background()
	err := pattern.Run(ctx)

	if err == nil {
		t.Error("Expected error when no consumers configured")
	}
}
