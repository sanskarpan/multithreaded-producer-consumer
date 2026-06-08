// Package consumer provides consumer implementations for producer-consumer patterns.
//
// Consumers receive items from an input channel and run application logic on
// them. All built-in consumers honour context cancellation, drain the input
// channel until closed, and expose atomic counters for safe concurrent
// inspection.
package consumer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Consumer defines the interface for data consumers.
//
// Consume MUST return when ctx is cancelled or the input channel is closed.
// Consume MUST NOT close the input channel; the orchestrating pattern owns
// channel lifecycle.
type Consumer interface {
	Consume(ctx context.Context, in <-chan interface{}) error
	ID() string
}

// ProcessFunc is a function that processes consumed data. Returning an error
// records the failure on the consumer; it does not abort the consume loop.
type ProcessFunc func(data interface{}) error

// BaseConsumer implements a basic consumer with structured bookkeeping.
type BaseConsumer struct {
	id        string
	processor ProcessFunc
	delay     time.Duration
	consumed  atomic.Int64
	errors    atomic.Int64

	errMu  sync.Mutex
	errBuf []error
}

// NewBaseConsumer creates a new base consumer. A nil processor is replaced
// with a no-op so callers can opt in to processing later.
func NewBaseConsumer(id string, delay time.Duration, processor ProcessFunc) *BaseConsumer {
	if delay < 0 {
		delay = 0
	}
	if processor == nil {
		processor = func(data interface{}) error { return nil }
	}
	return &BaseConsumer{
		id:        id,
		processor: processor,
		delay:     delay,
		errBuf:    make([]error, 0),
	}
}

// ID returns the consumer's identifier.
func (c *BaseConsumer) ID() string { return c.id }

// Processor returns the ProcessFunc supplied at construction time. The
// returned function is shared by reference; callers must not mutate its
// captured state from outside. Used by WorkerPool.SetWorker to replicate a
// template's logic across all spawned workers.
func (c *BaseConsumer) Processor() ProcessFunc { return c.processor }

// Base returns the embedded BaseConsumer. Wrapper types re-export this so
// callers can access the underlying fields and processor.
func (c *BaseConsumer) Base() *BaseConsumer { return c }

// Consume receives and processes data from the input channel until the input
// channel is closed or the context is cancelled. When the channel closes, any
// remaining items buffered in a BatchConsumer are flushed automatically.
func (c *BaseConsumer) Consume(ctx context.Context, in <-chan interface{}) error {
	if in == nil {
		return errors.New("consumer: input channel is nil")
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case data, ok := <-in:
			if !ok {
				return nil
			}
			if err := c.process(data); err != nil {
				c.recordError(err)
			}
			c.consumed.Add(1)

			if c.delay > 0 {
				t := time.NewTimer(c.delay)
				select {
				case <-t.C:
				case <-ctx.Done():
					t.Stop()
					return ctx.Err()
				}
			}
		}
	}
}

// process invokes the user-supplied processor. Recovers from panics so a
// faulty processor cannot bring down the whole pipeline.
func (c *BaseConsumer) process(data interface{}) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("consumer %s: processor panic: %v", c.id, r)
		}
	}()
	return c.processor(data)
}

// recordError stores an error and bumps the atomic counter. Failures are
// retained up to a soft cap to avoid unbounded memory growth.
const maxRetainedErrors = 1024

func (c *BaseConsumer) recordError(err error) {
	c.errors.Add(1)
	c.errMu.Lock()
	defer c.errMu.Unlock()
	if len(c.errBuf) < maxRetainedErrors {
		c.errBuf = append(c.errBuf, err)
	}
}

// ConsumedCount returns the number of items processed (including failures).
func (c *BaseConsumer) ConsumedCount() int { return int(c.consumed.Load()) }

// ErrorCount returns the number of processor errors observed.
func (c *BaseConsumer) ErrorCount() int { return int(c.errors.Load()) }

// Errors returns a snapshot of the (capped) error log.
func (c *BaseConsumer) Errors() []error {
	c.errMu.Lock()
	defer c.errMu.Unlock()
	out := make([]error, len(c.errBuf))
	copy(out, c.errBuf)
	return out
}

// PrintConsumer prints consumed items to stdout. Useful for examples.
type PrintConsumer struct {
	*BaseConsumer
}

// Base returns the embedded BaseConsumer.
func (c *PrintConsumer) Base() *BaseConsumer { return c.BaseConsumer }

// NewPrintConsumer creates a new print consumer.
func NewPrintConsumer(id string, delay time.Duration) *PrintConsumer {
	processor := func(data interface{}) error {
		fmt.Printf("[Consumer %s] Processing: %v\n", id, data)
		return nil
	}
	return &PrintConsumer{BaseConsumer: NewBaseConsumer(id, delay, processor)}
}

// AggregateConsumer collects all consumed items into an in-memory slice.
type AggregateConsumer struct {
	*BaseConsumer
	mu   sync.Mutex
	data []interface{}
}

// Base returns the embedded BaseConsumer.
func (c *AggregateConsumer) Base() *BaseConsumer { return c.BaseConsumer }

// NewAggregateConsumer creates a new aggregate consumer.
func NewAggregateConsumer(id string, delay time.Duration) *AggregateConsumer {
	ac := &AggregateConsumer{data: make([]interface{}, 0)}
	processor := func(data interface{}) error {
		ac.mu.Lock()
		ac.data = append(ac.data, data)
		ac.mu.Unlock()
		return nil
	}
	ac.BaseConsumer = NewBaseConsumer(id, delay, processor)
	return ac
}

// GetData returns a copy of the aggregated data.
func (c *AggregateConsumer) GetData() []interface{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]interface{}, len(c.data))
	copy(out, c.data)
	return out
}

// WorkerConsumer processes tasks with custom logic and tracks the data it has
// successfully processed (keyed by 0-based ordinal).
type WorkerConsumer struct {
	*BaseConsumer
	workerID int

	mu        sync.RWMutex
	processed map[int]interface{}
	ordinal   atomic.Int64
}

// NewWorkerConsumer creates a new worker consumer. Calls to the user-supplied
// processor are wrapped so successfully processed items are recorded.
func NewWorkerConsumer(id string, workerID int, delay time.Duration, processor ProcessFunc) *WorkerConsumer {
	wc := &WorkerConsumer{
		workerID:  workerID,
		processed: make(map[int]interface{}),
	}
	if processor == nil {
		processor = func(data interface{}) error { return nil }
	}
	wrapped := func(data interface{}) error {
		if err := processor(data); err != nil {
			return err
		}
		idx := int(wc.ordinal.Add(1) - 1)
		wc.mu.Lock()
		wc.processed[idx] = data
		wc.mu.Unlock()
		return nil
	}
	wc.BaseConsumer = NewBaseConsumer(id, delay, wrapped)
	return wc
}

// WorkerID returns the numeric worker identifier supplied at construction time.
func (c *WorkerConsumer) WorkerID() int { return c.workerID }

// Base returns the embedded BaseConsumer.
func (c *WorkerConsumer) Base() *BaseConsumer { return c.BaseConsumer }

// GetProcessed returns a copy of the processed-item map.
func (c *WorkerConsumer) GetProcessed() map[int]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[int]interface{}, len(c.processed))
	for k, v := range c.processed {
		out[k] = v
	}
	return out
}

// BatchConsumer processes items in batches of configurable size.
type BatchConsumer struct {
	*BaseConsumer
	batchSize      int
	batchProcessor func([]interface{}) error

	mu    sync.Mutex
	batch []interface{}
}

// NewBatchConsumer creates a new batch consumer. batchSize<=0 defaults to 10.
// A nil batchProcessor is replaced with a no-op so the consumer remains usable.
func NewBatchConsumer(id string, batchSize int, delay time.Duration, batchProcessor func([]interface{}) error) *BatchConsumer {
	if batchSize <= 0 {
		batchSize = 10
	}
	if batchProcessor == nil {
		batchProcessor = func([]interface{}) error { return nil }
	}
	bc := &BatchConsumer{
		batchSize:      batchSize,
		batch:          make([]interface{}, 0, batchSize),
		batchProcessor: batchProcessor,
	}
	processor := func(data interface{}) error {
		bc.mu.Lock()
		bc.batch = append(bc.batch, data)
		if len(bc.batch) < bc.batchSize {
			bc.mu.Unlock()
			return nil
		}
		toProcess := make([]interface{}, len(bc.batch))
		copy(toProcess, bc.batch)
		bc.batch = bc.batch[:0]
		bc.mu.Unlock()
		return bc.batchProcessor(toProcess)
	}
	bc.BaseConsumer = NewBaseConsumer(id, delay, processor)
	return bc
}

// BatchSize returns the configured maximum batch size.
func (c *BatchConsumer) BatchSize() int { return c.batchSize }

// Base returns the embedded BaseConsumer.
func (c *BatchConsumer) Base() *BaseConsumer { return c.BaseConsumer }

// Consume overrides BaseConsumer.Consume to automatically flush any remaining
// batched items when the input channel closes. This prevents silent data loss
// when the caller forgets to call Flush().
func (c *BatchConsumer) Consume(ctx context.Context, in <-chan interface{}) error {
	err := c.BaseConsumer.Consume(ctx, in)
	// Flush any leftover items regardless of whether Consume returned an error
	// or completed normally.
	if flushErr := c.Flush(); flushErr != nil && err == nil {
		return flushErr
	}
	return err
}

// Flush processes any remaining items in the in-progress batch. Safe to call
// once consumers have terminated.
func (c *BatchConsumer) Flush() error {
	c.mu.Lock()
	if len(c.batch) == 0 {
		c.mu.Unlock()
		return nil
	}
	toProcess := make([]interface{}, len(c.batch))
	copy(toProcess, c.batch)
	c.batch = c.batch[:0]
	c.mu.Unlock()
	return c.batchProcessor(toProcess)
}
