// Worker pool pattern example
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/sanskarpan/producer-consumer/patterns"
	"github.com/sanskarpan/producer-consumer/producer"
)

func main() {
	fmt.Println("=== Worker Pool Pattern ===")
	fmt.Println("Fixed number of workers processing tasks from a queue")
	fmt.Println()

	// Create worker pool with 4 workers
	numWorkers := 4
	pool := patterns.NewWorkerPool(numWorkers, 50)

	fmt.Printf("Number of workers: %d\n", pool.NumWorkers())
	fmt.Println()

	// Install a custom processor so the per-worker stats actually reflect work.
	pool.SetProcessor(func(data interface{}) error {
		// Simulate a small unit of work. Without this, the pool only tracks
		// what flows through the task channel, not what each worker actually
		// did, so WorkerStats would be all zeros.
		time.Sleep(2 * time.Millisecond)
		return nil
	})

	// Add multiple producers
	prod1 := producer.NewTaskProducer("TaskGen-1", 20, 1, 20*time.Millisecond)
	prod2 := producer.NewTaskProducer("TaskGen-2", 20, 2, 30*time.Millisecond)
	pool.AddProducer(prod1)
	pool.AddProducer(prod2)

	// Run with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fmt.Println("Starting task producers and worker pool...")
	start := time.Now()

	if err := pool.Run(ctx); err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	elapsed := time.Since(start)

	// Print statistics
	fmt.Println("\n=== Statistics ===")
	fmt.Printf("Total tasks produced: %d\n", prod1.ProducedCount()+prod2.ProducedCount())
	fmt.Printf("Execution time: %v\n", elapsed)

	fmt.Println("\nWorker load distribution:")
	stats := pool.WorkerStats()
	for workerID, count := range stats {
		fmt.Printf("  %s: %d tasks\n", workerID, count)
	}

	fmt.Println("\n✓ Worker pool completed successfully!")
}
