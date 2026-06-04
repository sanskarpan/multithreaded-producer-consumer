// Pipeline pattern example
package main

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/sanskarpan/producer-consumer/patterns"
	"github.com/sanskarpan/producer-consumer/producer"
)

func main() {
	fmt.Println("=== Pipeline Pattern ===")
	fmt.Println("Multi-stage data processing pipeline")
	fmt.Println()

	// Create pipeline
	pipeline := patterns.NewPipeline(50)

	// Add producer
	prod := producer.NewStringProducer("Generator", "data", 20, 10*time.Millisecond)
	pipeline.AddProducer(prod)

	// Stage 1: Uppercase transformation (2 workers)
	fmt.Println("Stage 1: Uppercase transformation (2 workers)")
	pipeline.AddStage("Uppercase", func(data interface{}) error {
		if str, ok := data.(string); ok {
			_ = strings.ToUpper(str)
		}
		return nil
	}, 2)

	// Stage 2: Add prefix (1 worker)
	fmt.Println("Stage 2: Add prefix (1 worker)")
	pipeline.AddStage("AddPrefix", func(data interface{}) error {
		if str, ok := data.(string); ok {
			_ = "PROCESSED_" + str
		}
		return nil
	}, 1)

	// Stage 3: Final validation (3 workers)
	fmt.Println("Stage 3: Validation (3 workers)")
	var validatedCount int64
	pipeline.AddStage("Validate", func(data interface{}) error {
		atomic.AddInt64(&validatedCount, 1)
		time.Sleep(5 * time.Millisecond) // Simulate validation
		return nil
	}, 3)

	fmt.Println()

	// Run pipeline
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fmt.Println("Running pipeline...")
	start := time.Now()

	if err := pipeline.Run(ctx); err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	elapsed := time.Since(start)

	// Print statistics
	fmt.Println("\n=== Statistics ===")
	fmt.Printf("Items produced: %d\n", prod.ProducedCount())
	fmt.Printf("Items validated: %d\n", atomic.LoadInt64(&validatedCount))
	fmt.Printf("Total time: %v\n", elapsed)
	fmt.Printf("Throughput: %.2f items/sec\n", float64(prod.ProducedCount())/elapsed.Seconds())

	fmt.Println("\n✓ Pipeline completed successfully!")
}
