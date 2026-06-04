// Fan-out/Fan-in pattern example
package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sanskarpan/producer-consumer/patterns"
	"github.com/sanskarpan/producer-consumer/producer"
)

func main() {
	fmt.Println("=== Fan-Out/Fan-In Pattern ===")
	fmt.Println("Multiple workers process in parallel, results merged to single channel")
	fmt.Println()

	// Processor that simulates work
	var processedCount int
	var mu sync.Mutex
	processor := func(data interface{}) error {
		time.Sleep(20 * time.Millisecond) // Simulate processing
		mu.Lock()
		processedCount++
		mu.Unlock()
		return nil
	}

	// Create fan-out/fan-in pattern with 4 workers
	numWorkers := 4
	pattern := patterns.NewFanOutFanIn(numWorkers, 50, processor)

	// Add producer
	prod := producer.NewIntProducer("DataSource", 0, 40, 10*time.Millisecond)
	pattern.AddProducer(prod)

	// Run with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fmt.Printf("Starting with %d parallel workers...\n", numWorkers)
	fmt.Println()

	// Start pattern in goroutine
	errChan := make(chan error, 1)
	start := time.Now()

	go func() {
		errChan <- pattern.Run(ctx)
	}()

	// Collect output
	outputCount := 0
	results := make([]interface{}, 0)
	done := make(chan struct{})

	go func() {
		for result := range pattern.OutputChan() {
			results = append(results, result)
			outputCount++
			if outputCount%10 == 0 {
				fmt.Printf("Collected %d results...\n", outputCount)
			}
		}
		close(done)
	}()

	// Wait for completion
	err := <-errChan
	<-done
	elapsed := time.Since(start)

	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	// Print statistics
	fmt.Println("\n=== Statistics ===")
	fmt.Printf("Produced: %d items\n", prod.ProducedCount())
	fmt.Printf("Processed: %d items\n", pattern.Processed())
	fmt.Printf("Collected: %d results\n", outputCount)
	fmt.Printf("Total time: %v\n", elapsed)
	fmt.Printf("Average throughput: %.2f items/sec\n", float64(outputCount)/elapsed.Seconds())

	fmt.Println("\n✓ Fan-out/fan-in completed successfully!")
}
