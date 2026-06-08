package consumer

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestBatchConsumer_AutoFlushOnChannelClose verifies that Consume automatically
// flushes any remaining batched items when the input channel closes, preventing
// silent data loss.
func TestBatchConsumer_AutoFlushOnChannelClose(t *testing.T) {
	var mu sync.Mutex
	var processed []int
	batchSize := 5

	c := NewBatchConsumer("auto-flush", batchSize, 0, func(data []interface{}) error {
		mu.Lock()
		for _, item := range data {
			processed = append(processed, item.(int))
		}
		mu.Unlock()
		return nil
	})

	in := make(chan interface{}, 10)
	// Send 7 items (batchSize=5, so 5 get processed as a batch, 2 remain).
	for i := 0; i < 7; i++ {
		in <- i
	}
	close(in)

	err := c.Consume(context.Background(), in)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(processed) != 7 {
		t.Fatalf("expected all 7 items processed, got %d", len(processed))
	}
}

// TestBatchConsumer_AutoFlushPartialBatch verifies that a partial final batch
// (fewer items than batchSize) is flushed on channel close.
func TestBatchConsumer_AutoFlushPartialBatch(t *testing.T) {
	var mu sync.Mutex
	var processed []int
	batchSize := 10

	c := NewBatchConsumer("partial-flush", batchSize, 0, func(data []interface{}) error {
		mu.Lock()
		for _, item := range data {
			processed = append(processed, item.(int))
		}
		mu.Unlock()
		return nil
	})

	in := make(chan interface{}, 10)
	for i := 0; i < 3; i++ {
		in <- i
	}
	close(in)

	err := c.Consume(context.Background(), in)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(processed) != 3 {
		t.Fatalf("expected 3 items processed, got %d", len(processed))
	}
}

// TestBatchConsumer_AutoFlushEmptyChannel verifies that closing an empty
// channel does not cause errors or panics.
func TestBatchConsumer_AutoFlushEmptyChannel(t *testing.T) {
	c := NewBatchConsumer("empty-flush", 5, 0, func(data []interface{}) error {
		t.Fatal("processor should not be called for empty channel")
		return nil
	})

	in := make(chan interface{})
	close(in)

	err := c.Consume(context.Background(), in)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestBatchConsumer_AutoFlushDoesNotInterfereWithFullBatch verifies that
// full batches are still processed normally before the final flush.
func TestBatchConsumer_AutoFlushDoesNotInterfereWithFullBatch(t *testing.T) {
	var mu sync.Mutex
	var batchCount int
	batchSize := 3

	c := NewBatchConsumer("interleave", batchSize, 0, func(data []interface{}) error {
		mu.Lock()
		batchCount++
		mu.Unlock()
		return nil
	})

	in := make(chan interface{}, 10)
	// Send exactly 6 items = 2 full batches, no remainder.
	for i := 0; i < 6; i++ {
		in <- i
	}
	close(in)

	err := c.Consume(context.Background(), in)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if batchCount != 2 {
		t.Fatalf("expected 2 batch invocations, got %d", batchCount)
	}
}

// TestBatchConsumer_ConcurrentConsumeAndFlush ensures that concurrent calls
// to Consume and Flush do not race.
func TestBatchConsumer_ConcurrentConsumeAndFlush(t *testing.T) {
	var count atomic.Int64
	c := NewBatchConsumer("concurrent", 3, 0, func(data []interface{}) error {
		count.Add(int64(len(data)))
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			in := make(chan interface{}, 5)
			for j := 0; j < 5; j++ {
				in <- id*100 + j
			}
			close(in)
			_ = c.Consume(ctx, in)
		}(i)
	}
	wg.Wait()
	_ = c.Flush()

	if count.Load() < 20 {
		t.Fatalf("expected at least 20 items processed, got %d", count.Load())
	}
}
