// Basic producer-consumer pattern example
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
	fmt.Println("=== Basic Producer-Consumer Pattern ===")
	fmt.Println("Using unbuffered channel for direct hand-off")
	fmt.Println()

	// Create the pattern
	pattern := patterns.NewBasicPattern()

	// Add 2 producers
	prod1 := producer.NewStringProducer("P1", "msg", 5, 100*time.Millisecond)
	prod2 := producer.NewStringProducer("P2", "data", 5, 150*time.Millisecond)
	pattern.AddProducer(prod1)
	pattern.AddProducer(prod2)

	// Add 2 consumers
	cons1 := consumer.NewPrintConsumer("C1", 50*time.Millisecond)
	cons2 := consumer.NewPrintConsumer("C2", 50*time.Millisecond)
	pattern.AddConsumer(cons1)
	pattern.AddConsumer(cons2)

	// Run with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	fmt.Println("Starting producers and consumers...")
	start := time.Now()

	if err := pattern.Run(ctx); err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	elapsed := time.Since(start)

	// Print statistics
	fmt.Println("\n=== Statistics ===")
	fmt.Printf("Producer P1: %d items produced\n", prod1.ProducedCount())
	fmt.Printf("Producer P2: %d items produced\n", prod2.ProducedCount())
	fmt.Printf("Consumer C1: %d items consumed\n", cons1.ConsumedCount())
	fmt.Printf("Consumer C2: %d items consumed\n", cons2.ConsumedCount())
	fmt.Printf("Total time: %v\n", elapsed)

	fmt.Println("\n✓ Pattern completed successfully!")
}
