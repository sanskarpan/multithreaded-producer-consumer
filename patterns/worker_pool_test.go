package patterns

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/sanskarpan/producer-consumer/consumer"
	"github.com/sanskarpan/producer-consumer/producer"
)

func TestWorkerPool_BasicOperation(t *testing.T) {
	pool := NewWorkerPool(4, 50)

	prod := producer.NewIntProducer("p1", 0, 100, 0)
	pool.AddProducer(prod)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := pool.Run(ctx); err != nil {
		t.Fatalf("Worker pool run failed: %v", err)
	}

	if prod.ProducedCount() != 100 {
		t.Errorf("Expected 100 items produced, got %d", prod.ProducedCount())
	}

	stats := pool.WorkerStats()
	totalProcessed := 0
	for _, count := range stats {
		totalProcessed += count
	}

	if totalProcessed != 100 {
		t.Errorf("Expected 100 items processed by workers, got %d", totalProcessed)
	}
}

func TestWorkerPool_NumWorkers(t *testing.T) {
	numWorkers := 8
	pool := NewWorkerPool(numWorkers, 50)

	if pool.NumWorkers() != numWorkers {
		t.Errorf("Expected %d workers, got %d", numWorkers, pool.NumWorkers())
	}

	prod := producer.NewIntProducer("p1", 0, 10, 0)
	pool.AddProducer(prod)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := pool.Run(ctx); err != nil {
		t.Fatalf("Worker pool run failed: %v", err)
	}

	stats := pool.WorkerStats()
	if len(stats) != numWorkers {
		t.Errorf("Expected %d worker stats, got %d", numWorkers, len(stats))
	}
}

func TestWorkerPool_LoadDistribution(t *testing.T) {
	pool := NewWorkerPool(4, 100)

	prod := producer.NewIntProducer("p1", 0, 100, 0)
	pool.AddProducer(prod)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := pool.Run(ctx); err != nil {
		t.Fatalf("Worker pool run failed: %v", err)
	}

	stats := pool.WorkerStats()

	// Check that work was distributed across multiple workers
	activeWorkers := 0
	totalProcessed := 0
	for _, count := range stats {
		totalProcessed += count
		if count > 0 {
			activeWorkers++
		}
	}

	if totalProcessed != 100 {
		t.Errorf("Expected 100 items processed total, got %d", totalProcessed)
	}

	// At least 2 workers should have done work (load distribution)
	if activeWorkers < 2 {
		t.Errorf("Expected at least 2 workers to process items, only %d did work", activeWorkers)
	}
}

func TestWorkerPool_MultipleProducers(t *testing.T) {
	pool := NewWorkerPool(4, 100)

	prod1 := producer.NewIntProducer("p1", 0, 50, 0)
	prod2 := producer.NewIntProducer("p2", 100, 50, 0)
	pool.AddProducer(prod1)
	pool.AddProducer(prod2)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := pool.Run(ctx); err != nil {
		t.Fatalf("Worker pool run failed: %v", err)
	}

	totalProduced := prod1.ProducedCount() + prod2.ProducedCount()
	if totalProduced != 100 {
		t.Errorf("Expected 100 items produced, got %d", totalProduced)
	}
}

func TestWorkerPool_CustomWorker(t *testing.T) {
	pool := NewWorkerPool(4, 50)

	var processedCount int
	var mu sync.Mutex

	processor := func(data interface{}) error {
		mu.Lock()
		processedCount++
		mu.Unlock()
		return nil
	}

	worker := consumer.NewWorkerConsumer("custom-worker", 0, 0, processor)
	pool.SetWorker(worker)

	prod := producer.NewIntProducer("p1", 0, 50, 0)
	pool.AddProducer(prod)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := pool.Run(ctx); err != nil {
		t.Fatalf("Worker pool run failed: %v", err)
	}

	if prod.ProducedCount() != 50 {
		t.Errorf("Expected 50 items produced, got %d", prod.ProducedCount())
	}
}

func TestWorkerPool_DefaultWorkerCount(t *testing.T) {
	pool := NewWorkerPool(0, 50)

	if pool.NumWorkers() != 4 {
		t.Errorf("Expected default 4 workers, got %d", pool.NumWorkers())
	}
}

func TestWorkerPool_NoProducers(t *testing.T) {
	pool := NewWorkerPool(4, 50)

	ctx := context.Background()
	err := pool.Run(ctx)

	if err == nil {
		t.Error("Expected error when no producers configured")
	}
}
