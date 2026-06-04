package patterns

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanskarpan/producer-consumer/consumer"
	"github.com/sanskarpan/producer-consumer/producer"
)

// TestWorkerPool_SetProcessor_CustomProcessorRuns is a regression test for the
// bug where WorkerPool.SetWorker captured the user-supplied consumer but then
// never invoked it. Custom processors supplied via SetProcessor must run for
// every item produced.
func TestWorkerPool_SetProcessor_CustomProcessorRuns(t *testing.T) {
	var processed int64
	pool := NewWorkerPool(2, 16)
	pool.AddProducer(producer.NewIntProducer("p1", 0, 50, 0))
	pool.SetProcessor(func(data interface{}) error {
		atomic.AddInt64(&processed, 1)
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pool.Run(ctx); err != nil {
		t.Fatalf("pool run: %v", err)
	}
	if got := atomic.LoadInt64(&processed); got != 50 {
		t.Fatalf("expected 50 items processed by custom processor, got %d", got)
	}
}

// TestWorkerPool_SetWorker_TemplateConsumerUsed is a regression test verifying
// that the workaround path (passing a template WorkerConsumer to SetWorker)
// actually drives the per-worker processor closure for every item.
func TestWorkerPool_SetWorker_TemplateConsumerUsed(t *testing.T) {
	var processed int64
	template := consumer.NewWorkerConsumer("template", 0, 0, func(data interface{}) error {
		atomic.AddInt64(&processed, 1)
		return nil
	})

	pool := NewWorkerPool(3, 16)
	pool.AddProducer(producer.NewIntProducer("p1", 0, 60, 0))
	pool.SetWorker(template)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pool.Run(ctx); err != nil {
		t.Fatalf("pool run: %v", err)
	}
	if got := atomic.LoadInt64(&processed); got != 60 {
		t.Fatalf("expected 60 items processed, got %d", got)
	}
}
