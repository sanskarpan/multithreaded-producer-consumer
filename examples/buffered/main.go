// Buffered channel pattern example
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
	fmt.Println("=== Buffered Channel Pattern ===")
	fmt.Println("Using buffered channel for decoupling")
	fmt.Println()

	// Create pattern with buffer size of 20
	bufferSize := 20
	pattern := patterns.NewBufferedPattern(bufferSize)

	fmt.Printf("Buffer size: %d\n", pattern.BufferSize())
	fmt.Println()

	// Fast producer, slow consumer scenario
	prod := producer.NewIntProducer("FastProducer", 0, 50, 10*time.Millisecond)
	pattern.AddProducer(prod)

	// Slow consumer
	cons := consumer.NewPrintConsumer("SlowConsumer", 50*time.Millisecond)
	pattern.AddConsumer(cons)

	// Run with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fmt.Println("Starting fast producer and slow consumer...")
	fmt.Println("The buffer allows the producer to continue even when consumer is slow")
	fmt.Println()

	start := time.Now()

	if err := pattern.Run(ctx); err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	elapsed := time.Since(start)

	// Print statistics
	fmt.Println("\n=== Statistics ===")
	fmt.Printf("Produced: %d items\n", prod.ProducedCount())
	fmt.Printf("Consumed: %d items\n", cons.ConsumedCount())
	fmt.Printf("Total time: %v\n", elapsed)
	fmt.Printf("Buffer capacity: %d\n", pattern.ChannelCap())

	fmt.Println("\n✓ Pattern completed successfully!")
}
