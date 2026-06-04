package patterns

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/sanskarpan/producer-consumer/producer"
)

func TestFanOutFanIn_BasicOperation(t *testing.T) {
	processedCount := 0
	var mu sync.Mutex

	processor := func(data interface{}) error {
		mu.Lock()
		processedCount++
		mu.Unlock()
		return nil
	}

	pattern := NewFanOutFanIn(4, 50, processor)

	prod := producer.NewIntProducer("p1", 0, 100, 0)
	pattern.AddProducer(prod)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Run the pattern
	errChan := make(chan error, 1)
	go func() {
		errChan <- pattern.Run(ctx)
	}()

	// Collect output
	outputCount := 0
	done := make(chan struct{})
	go func() {
		for range pattern.OutputChan() {
			outputCount++
		}
		close(done)
	}()

	// Wait for completion
	err := <-errChan
	<-done

	if err != nil {
		t.Fatalf("Pattern run failed: %v", err)
	}

	if prod.ProducedCount() != 100 {
		t.Errorf("Expected 100 items produced, got %d", prod.ProducedCount())
	}

	if processedCount != 100 {
		t.Errorf("Expected 100 items processed, got %d", processedCount)
	}

	if outputCount != 100 {
		t.Errorf("Expected 100 items in output, got %d", outputCount)
	}
}

func TestFanOutFanIn_MultipleProducers(t *testing.T) {
	processedCount := 0
	var mu sync.Mutex

	processor := func(data interface{}) error {
		mu.Lock()
		processedCount++
		mu.Unlock()
		time.Sleep(1 * time.Millisecond) // Simulate work
		return nil
	}

	pattern := NewFanOutFanIn(4, 100, processor)

	prod1 := producer.NewIntProducer("p1", 0, 50, 0)
	prod2 := producer.NewIntProducer("p2", 100, 50, 0)
	pattern.AddProducer(prod1)
	pattern.AddProducer(prod2)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

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
		t.Fatalf("Pattern run failed: %v", err)
	}

	totalProduced := prod1.ProducedCount() + prod2.ProducedCount()
	if totalProduced != 100 {
		t.Errorf("Expected 100 items produced, got %d", totalProduced)
	}

	if processedCount != 100 {
		t.Errorf("Expected 100 items processed, got %d", processedCount)
	}
}

func TestFanOutFanIn_ProcessedCount(t *testing.T) {
	processor := func(data interface{}) error {
		return nil
	}

	pattern := NewFanOutFanIn(4, 50, processor)
	prod := producer.NewIntProducer("p1", 0, 50, 0)
	pattern.AddProducer(prod)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	errChan := make(chan error, 1)
	go func() {
		errChan <- pattern.Run(ctx)
	}()

	// Consume output
	go func() {
		for range pattern.OutputChan() {
		}
	}()

	err := <-errChan
	if err != nil {
		t.Fatalf("Pattern run failed: %v", err)
	}

	if pattern.Processed() != 50 {
		t.Errorf("Expected 50 items processed, got %d", pattern.Processed())
	}
}

func TestFanOutFanIn_NoProducers(t *testing.T) {
	processor := func(data interface{}) error {
		return nil
	}

	pattern := NewFanOutFanIn(4, 50, processor)

	ctx := context.Background()
	err := pattern.Run(ctx)

	if err == nil {
		t.Error("Expected error when no producers configured")
	}
}

func TestFanOutFanIn_ParallelProcessing(t *testing.T) {
	processingTimes := make(map[int]time.Time)
	var mu sync.Mutex

	processor := func(data interface{}) error {
		mu.Lock()
		processingTimes[len(processingTimes)] = time.Now()
		mu.Unlock()
		time.Sleep(10 * time.Millisecond) // Simulate work
		return nil
	}

	pattern := NewFanOutFanIn(4, 50, processor)
	prod := producer.NewIntProducer("p1", 0, 20, 0)
	pattern.AddProducer(prod)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	start := time.Now()

	errChan := make(chan error, 1)
	go func() {
		errChan <- pattern.Run(ctx)
	}()

	go func() {
		for range pattern.OutputChan() {
		}
	}()

	err := <-errChan
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Pattern run failed: %v", err)
	}

	// With 4 workers, 20 items at 10ms each should take roughly 50-100ms (parallel)
	// Sequential would take 200ms
	if elapsed > 150*time.Millisecond {
		t.Logf("Warning: Processing took longer than expected: %v (may not be truly parallel)", elapsed)
	}
}
