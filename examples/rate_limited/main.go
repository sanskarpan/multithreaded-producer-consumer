// Rate-limited pattern example
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/sanskarpan/producer-consumer/consumer"
	"github.com/sanskarpan/producer-consumer/patterns"
	"github.com/sanskarpan/producer-consumer/producer"
)

func main() {
	fmt.Println("=== Rate-Limited Pattern ===")
	fmt.Println("Controlling throughput with rate limiting")
	fmt.Println()

	// Create rate-limited pattern
	// Producer: 10 items/sec, Consumer: 15 items/sec
	producerRate := 10
	consumerRate := 15
	pattern := patterns.NewRateLimitedPattern(50, producerRate, consumerRate)

	fmt.Printf("Producer rate: %d items/sec\n", pattern.ProducerRate())
	fmt.Printf("Consumer rate: %d items/sec\n", pattern.ConsumerRate())
	fmt.Println()

	// Add producer
	prod := producer.NewIntProducer("RateLimitedSource", 0, 30, 0)
	pattern.AddProducer(prod)

	// Add consumer
	cons := consumer.NewAggregateConsumer("RateLimitedSink", 0)
	pattern.AddConsumer(cons)

	// Run with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	fmt.Println("Starting rate-limited production and consumption...")
	fmt.Println("Producer will be rate-limited to 10 items/sec")
	fmt.Println()

	start := time.Now()

	if err := pattern.Run(ctx); err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	elapsed := time.Since(start)

	// Get statistics
	produced, consumed := pattern.Stats()

	// Print statistics
	fmt.Println("\n=== Statistics ===")
	fmt.Printf("Items produced: %d\n", produced)
	fmt.Printf("Items consumed: %d\n", consumed)
	fmt.Printf("Total time: %v\n", elapsed)
	fmt.Printf("Actual producer rate: %.2f items/sec\n", float64(produced)/elapsed.Seconds())
	fmt.Printf("Actual consumer rate: %.2f items/sec\n", float64(consumed)/elapsed.Seconds())

	// Verify rate limiting
	expectedTime := time.Duration(float64(produced)/float64(producerRate)) * time.Second
	fmt.Printf("\nExpected time for %d items at %d/sec: ~%v\n", produced, producerRate, expectedTime)
	fmt.Printf("Actual time: %v\n", elapsed)

	fmt.Println("\n✓ Rate-limited pattern completed successfully!")
}
