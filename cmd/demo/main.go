// Interactive demo application for producer-consumer patterns
package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sanskarpan/producer-consumer/consumer"
	"github.com/sanskarpan/producer-consumer/patterns"
	"github.com/sanskarpan/producer-consumer/producer"
)

func main() {
	fmt.Println("╔══════════════════════════════════════════════════════╗")
	fmt.Println("║  Producer-Consumer Pattern Demo                      ║")
	fmt.Println("║  Interactive demonstrations of concurrency patterns  ║")
	fmt.Println("╚══════════════════════════════════════════════════════╝")
	fmt.Println()

	scanner := bufio.NewScanner(os.Stdin)

	for {
		showMenu()

		fmt.Print("\nEnter your choice (1-7, or 0 to exit): ")
		scanner.Scan()
		choice := strings.TrimSpace(scanner.Text())

		switch choice {
		case "1":
			runBasicPattern()
		case "2":
			runBufferedPattern()
		case "3":
			runWorkerPool()
		case "4":
			runFanOutFanIn()
		case "5":
			runPipeline()
		case "6":
			runRateLimited()
		case "7":
			runComparison()
		case "0":
			fmt.Println("\n👋 Goodbye!")
			return
		default:
			fmt.Println("\n❌ Invalid choice. Please try again.")
		}

		fmt.Print("\n\nPress Enter to continue...")
		scanner.Scan()
		clearScreen()
	}
}

func showMenu() {
	fmt.Println("═══ Available Patterns ═══")
	fmt.Println("1. Basic (Unbuffered Channel)")
	fmt.Println("2. Buffered Channel")
	fmt.Println("3. Worker Pool")
	fmt.Println("4. Fan-Out/Fan-In")
	fmt.Println("5. Pipeline")
	fmt.Println("6. Rate-Limited")
	fmt.Println("7. Compare All Patterns")
	fmt.Println("0. Exit")
}

func clearScreen() {
	fmt.Print("\033[H\033[2J")
}

func runBasicPattern() {
	fmt.Println("\n═══ Basic Pattern (Unbuffered Channel) ═══")
	fmt.Println("Direct hand-off between producers and consumers")
	fmt.Println()

	pattern := patterns.NewBasicPattern()

	// Add producers
	prod1 := producer.NewStringProducer("P1", "msg", 5, 50*time.Millisecond)
	prod2 := producer.NewStringProducer("P2", "data", 5, 50*time.Millisecond)
	pattern.AddProducer(prod1)
	pattern.AddProducer(prod2)

	// Add consumers
	cons1 := consumer.NewPrintConsumer("C1", 30*time.Millisecond)
	cons2 := consumer.NewPrintConsumer("C2", 30*time.Millisecond)
	pattern.AddConsumer(cons1)
	pattern.AddConsumer(cons2)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	fmt.Println("▶ Running...")
	fmt.Println()

	if err := pattern.Run(ctx); err != nil {
		fmt.Printf("❌ Error: %v\n", err)
		return
	}

	printStats(map[string]interface{}{
		"Producer P1":  prod1.ProducedCount(),
		"Producer P2":  prod2.ProducedCount(),
		"Consumer C1":  cons1.ConsumedCount(),
		"Consumer C2":  cons2.ConsumedCount(),
		"Elapsed Time": time.Since(start),
	})
}

func runBufferedPattern() {
	fmt.Println("\n═══ Buffered Channel Pattern ═══")
	fmt.Println("Decoupling producers and consumers with a buffer")
	fmt.Println()

	bufferSize := 20
	pattern := patterns.NewBufferedPattern(bufferSize)

	prod := producer.NewIntProducer("FastProducer", 0, 30, 10*time.Millisecond)
	pattern.AddProducer(prod)

	cons := consumer.NewPrintConsumer("SlowConsumer", 40*time.Millisecond)
	pattern.AddConsumer(cons)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	fmt.Printf("▶ Running with buffer size: %d\n", bufferSize)
	fmt.Println()

	if err := pattern.Run(ctx); err != nil {
		fmt.Printf("❌ Error: %v\n", err)
		return
	}

	printStats(map[string]interface{}{
		"Buffer Size":  bufferSize,
		"Produced":     prod.ProducedCount(),
		"Consumed":     cons.ConsumedCount(),
		"Elapsed Time": time.Since(start),
	})
}

func runWorkerPool() {
	fmt.Println("\n═══ Worker Pool Pattern ═══")
	fmt.Println("Fixed number of workers processing from a shared queue")
	fmt.Println()

	numWorkers := 4
	pool := patterns.NewWorkerPool(numWorkers, 50)

	prod1 := producer.NewTaskProducer("Gen-1", 15, 1, 20*time.Millisecond)
	prod2 := producer.NewTaskProducer("Gen-2", 15, 2, 20*time.Millisecond)
	pool.AddProducer(prod1)
	pool.AddProducer(prod2)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	fmt.Printf("▶ Running with %d workers\n", numWorkers)
	fmt.Println()

	if err := pool.Run(ctx); err != nil {
		fmt.Printf("❌ Error: %v\n", err)
		return
	}

	stats := pool.WorkerStats()
	fmt.Println("\n📊 Worker Statistics:")
	for workerID, count := range stats {
		fmt.Printf("  %s: %d tasks\n", workerID, count)
	}

	printStats(map[string]interface{}{
		"Total Tasks":  prod1.ProducedCount() + prod2.ProducedCount(),
		"Elapsed Time": time.Since(start),
	})
}

func runFanOutFanIn() {
	fmt.Println("\n═══ Fan-Out/Fan-In Pattern ═══")
	fmt.Println("Parallel processing with result aggregation")
	fmt.Println()

	var processedCount int
	var mu sync.Mutex
	processor := func(data interface{}) error {
		time.Sleep(15 * time.Millisecond)
		mu.Lock()
		processedCount++
		mu.Unlock()
		return nil
	}

	numWorkers := 4
	pattern := patterns.NewFanOutFanIn(numWorkers, 50, processor)

	prod := producer.NewIntProducer("DataSource", 0, 30, 5*time.Millisecond)
	pattern.AddProducer(prod)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	start := time.Now()
	fmt.Printf("▶ Running with %d parallel workers\n", numWorkers)
	fmt.Println()

	errChan := make(chan error, 1)
	go func() {
		errChan <- pattern.Run(ctx)
	}()

	outputCount := 0
	done := make(chan struct{})
	go func() {
		for range pattern.OutputChan() {
			outputCount++
		}
		close(done)
	}()

	err := <-errChan
	<-done

	if err != nil {
		fmt.Printf("❌ Error: %v\n", err)
		return
	}

	elapsed := time.Since(start)

	printStats(map[string]interface{}{
		"Produced":         prod.ProducedCount(),
		"Processed":        pattern.Processed(),
		"Output Collected": outputCount,
		"Elapsed Time":     elapsed,
		"Throughput":       fmt.Sprintf("%.2f items/sec", float64(outputCount)/elapsed.Seconds()),
	})
}

func runPipeline() {
	fmt.Println("\n═══ Pipeline Pattern ═══")
	fmt.Println("Multi-stage data processing")
	fmt.Println()

	pipeline := patterns.NewPipeline(50)

	prod := producer.NewStringProducer("Generator", "data", 20, 10*time.Millisecond)
	pipeline.AddProducer(prod)

	var stage1Count, stage2Count, stage3Count int64
	pipeline.AddStage("Stage-1", func(data interface{}) error {
		atomic.AddInt64(&stage1Count, 1)
		time.Sleep(5 * time.Millisecond)
		return nil
	}, 2)

	pipeline.AddStage("Stage-2", func(data interface{}) error {
		atomic.AddInt64(&stage2Count, 1)
		time.Sleep(3 * time.Millisecond)
		return nil
	}, 2)

	pipeline.AddStage("Stage-3", func(data interface{}) error {
		atomic.AddInt64(&stage3Count, 1)
		time.Sleep(2 * time.Millisecond)
		return nil
	}, 2)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	start := time.Now()
	fmt.Println("▶ Running 3-stage pipeline")
	fmt.Println("  Stage 1: 2 workers")
	fmt.Println("  Stage 2: 2 workers")
	fmt.Println("  Stage 3: 2 workers")
	fmt.Println()

	if err := pipeline.Run(ctx); err != nil {
		fmt.Printf("❌ Error: %v\n", err)
		return
	}

	printStats(map[string]interface{}{
		"Items Produced":    prod.ProducedCount(),
		"Stage 1 Processed": atomic.LoadInt64(&stage1Count),
		"Stage 2 Processed": atomic.LoadInt64(&stage2Count),
		"Stage 3 Processed": atomic.LoadInt64(&stage3Count),
		"Elapsed Time":      time.Since(start),
	})
}

func runRateLimited() {
	fmt.Println("\n═══ Rate-Limited Pattern ═══")
	fmt.Println("Controlling throughput with rate limiting")
	fmt.Println()

	producerRate := 15
	consumerRate := 20
	pattern := patterns.NewRateLimitedPattern(50, producerRate, consumerRate)

	prod := producer.NewIntProducer("Source", 0, 30, 0)
	pattern.AddProducer(prod)

	cons := consumer.NewAggregateConsumer("Sink", 0)
	pattern.AddConsumer(cons)

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()

	start := time.Now()
	fmt.Printf("▶ Running with rates:\n")
	fmt.Printf("  Producer: %d items/sec\n", producerRate)
	fmt.Printf("  Consumer: %d items/sec\n", consumerRate)
	fmt.Println()

	if err := pattern.Run(ctx); err != nil {
		fmt.Printf("❌ Error: %v\n", err)
		return
	}

	elapsed := time.Since(start)
	produced, consumed := pattern.Stats()

	printStats(map[string]interface{}{
		"Produced":             produced,
		"Consumed":             consumed,
		"Elapsed Time":         elapsed,
		"Actual Producer Rate": fmt.Sprintf("%.2f items/sec", float64(produced)/elapsed.Seconds()),
		"Actual Consumer Rate": fmt.Sprintf("%.2f items/sec", float64(consumed)/elapsed.Seconds()),
	})
}

func runComparison() {
	fmt.Println("\n═══ Pattern Comparison ═══")
	fmt.Println("Running all patterns with same workload for comparison")
	fmt.Println()

	itemCount := 50
	results := make(map[string]time.Duration)

	// Basic Pattern
	fmt.Print("Running Basic Pattern... ")
	pattern1 := patterns.NewBasicPattern()
	pattern1.AddProducer(producer.NewIntProducer("P", 0, itemCount, 0))
	pattern1.AddConsumer(consumer.NewAggregateConsumer("C", 0))
	ctx1, cancel1 := context.WithTimeout(context.Background(), 5*time.Second)
	start1 := time.Now()
	_ = pattern1.Run(ctx1)
	results["Basic"] = time.Since(start1)
	cancel1()
	fmt.Println("✓")

	// Buffered Pattern
	fmt.Print("Running Buffered Pattern... ")
	pattern2 := patterns.NewBufferedPattern(50)
	pattern2.AddProducer(producer.NewIntProducer("P", 0, itemCount, 0))
	pattern2.AddConsumer(consumer.NewAggregateConsumer("C", 0))
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	start2 := time.Now()
	_ = pattern2.Run(ctx2)
	results["Buffered"] = time.Since(start2)
	cancel2()
	fmt.Println("✓")

	// Worker Pool
	fmt.Print("Running Worker Pool... ")
	pattern3 := patterns.NewWorkerPool(4, 50)
	pattern3.AddProducer(producer.NewIntProducer("P", 0, itemCount, 0))
	ctx3, cancel3 := context.WithTimeout(context.Background(), 5*time.Second)
	start3 := time.Now()
	_ = pattern3.Run(ctx3)
	results["Worker Pool"] = time.Since(start3)
	cancel3()
	fmt.Println("✓")

	// Pipeline
	fmt.Print("Running Pipeline... ")
	pattern5 := patterns.NewPipeline(50)
	pattern5.AddProducer(producer.NewIntProducer("P", 0, itemCount, 0))
	pattern5.AddStage("Process", func(interface{}) error { return nil }, 2)
	ctx5, cancel5 := context.WithTimeout(context.Background(), 5*time.Second)
	start5 := time.Now()
	_ = pattern5.Run(ctx5)
	results["Pipeline"] = time.Since(start5)
	cancel5()
	fmt.Println("✓")

	fmt.Println("\n📊 Performance Comparison:")
	fmt.Println("─────────────────────────────")
	for name, duration := range results {
		fmt.Printf("%-15s: %v\n", name, duration)
	}
}

func printStats(stats map[string]interface{}) {
	fmt.Println("\n📊 Statistics:")
	fmt.Println("─────────────────────────────")
	for key, value := range stats {
		fmt.Printf("%-20s: %v\n", key, value)
	}
	fmt.Println("\n✓ Completed successfully!")
}
