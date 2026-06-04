package patterns

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sanskarpan/producer-consumer/producer"
)

func TestPipeline_BasicOperation(t *testing.T) {
	pipeline := NewPipeline(50)

	prod := producer.NewIntProducer("p1", 0, 10, 0)
	pipeline.AddProducer(prod)

	// Stage 1: Double the value
	processedStage1 := make([]interface{}, 0)
	var mu1 sync.Mutex
	pipeline.AddStage("double", func(data interface{}) error {
		mu1.Lock()
		processedStage1 = append(processedStage1, data)
		mu1.Unlock()
		return nil
	}, 1)

	// Stage 2: Add 10
	processedStage2 := make([]interface{}, 0)
	var mu2 sync.Mutex
	pipeline.AddStage("add10", func(data interface{}) error {
		mu2.Lock()
		processedStage2 = append(processedStage2, data)
		mu2.Unlock()
		return nil
	}, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := pipeline.Run(ctx); err != nil {
		t.Fatalf("Pipeline run failed: %v", err)
	}

	if prod.ProducedCount() != 10 {
		t.Errorf("Expected 10 items produced, got %d", prod.ProducedCount())
	}

	if len(processedStage1) != 10 {
		t.Errorf("Expected 10 items in stage 1, got %d", len(processedStage1))
	}

	if len(processedStage2) != 10 {
		t.Errorf("Expected 10 items in stage 2, got %d", len(processedStage2))
	}
}

func TestPipeline_MultipleStages(t *testing.T) {
	pipeline := NewPipeline(50)

	prod := producer.NewIntProducer("p1", 0, 20, 0)
	pipeline.AddProducer(prod)

	// Add 3 stages
	for i := 1; i <= 3; i++ {
		stageName := fmt.Sprintf("stage-%d", i)
		pipeline.AddStage(stageName, func(data interface{}) error {
			return nil
		}, 1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := pipeline.Run(ctx); err != nil {
		t.Fatalf("Pipeline run failed: %v", err)
	}

	if prod.ProducedCount() != 20 {
		t.Errorf("Expected 20 items produced, got %d", prod.ProducedCount())
	}
}

func TestPipeline_MultipleWorkers(t *testing.T) {
	pipeline := NewPipeline(100)

	prod := producer.NewIntProducer("p1", 0, 100, 0)
	pipeline.AddProducer(prod)

	processedCount := 0
	var mu sync.Mutex

	// Stage with multiple workers
	pipeline.AddStage("parallel-stage", func(data interface{}) error {
		mu.Lock()
		processedCount++
		mu.Unlock()
		time.Sleep(1 * time.Millisecond) // Simulate work
		return nil
	}, 4) // 4 workers

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := pipeline.Run(ctx); err != nil {
		t.Fatalf("Pipeline run failed: %v", err)
	}

	if processedCount != 100 {
		t.Errorf("Expected 100 items processed, got %d", processedCount)
	}
}

func TestPipeline_RunWithOutput(t *testing.T) {
	pipeline := NewPipeline(50)

	prod := producer.NewIntProducer("p1", 0, 20, 0)
	pipeline.AddProducer(prod)

	pipeline.AddStage("process", func(data interface{}) error {
		return nil
	}, 2)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	outputChan, err := pipeline.RunWithOutput(ctx)
	if err != nil {
		t.Fatalf("Pipeline RunWithOutput failed: %v", err)
	}

	// Collect output
	outputCount := 0
	for range outputChan {
		outputCount++
	}

	if outputCount != 20 {
		t.Errorf("Expected 20 items in output, got %d", outputCount)
	}
}

func TestPipeline_NoProducers(t *testing.T) {
	pipeline := NewPipeline(50)
	pipeline.AddStage("stage1", func(data interface{}) error { return nil }, 1)

	ctx := context.Background()
	err := pipeline.Run(ctx)

	if err == nil {
		t.Error("Expected error when no producers configured")
	}
}

func TestPipeline_NoStages(t *testing.T) {
	pipeline := NewPipeline(50)
	prod := producer.NewIntProducer("p1", 0, 10, 0)
	pipeline.AddProducer(prod)

	ctx := context.Background()
	err := pipeline.Run(ctx)

	if err == nil {
		t.Error("Expected error when no stages configured")
	}
}

func TestPipeline_MultipleProducers(t *testing.T) {
	pipeline := NewPipeline(100)

	prod1 := producer.NewIntProducer("p1", 0, 30, 0)
	prod2 := producer.NewIntProducer("p2", 100, 30, 0)
	pipeline.AddProducer(prod1)
	pipeline.AddProducer(prod2)

	processedCount := 0
	var mu sync.Mutex

	pipeline.AddStage("process", func(data interface{}) error {
		mu.Lock()
		processedCount++
		mu.Unlock()
		return nil
	}, 2)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := pipeline.Run(ctx); err != nil {
		t.Fatalf("Pipeline run failed: %v", err)
	}

	totalProduced := prod1.ProducedCount() + prod2.ProducedCount()
	if totalProduced != 60 {
		t.Errorf("Expected 60 items produced, got %d", totalProduced)
	}

	if processedCount != 60 {
		t.Errorf("Expected 60 items processed, got %d", processedCount)
	}
}

func TestPipeline_DefaultWorkerCount(t *testing.T) {
	pipeline := NewPipeline(50)

	prod := producer.NewIntProducer("p1", 0, 10, 0)
	pipeline.AddProducer(prod)

	// Add stage with 0 workers (should default to 1)
	pipeline.AddStage("stage", func(data interface{}) error { return nil }, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := pipeline.Run(ctx); err != nil {
		t.Fatalf("Pipeline should work with default worker count")
	}
}
