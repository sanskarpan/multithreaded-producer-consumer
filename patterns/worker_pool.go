package patterns

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/sanskarpan/producer-consumer/consumer"
	"github.com/sanskarpan/producer-consumer/producer"
)

// WorkerPool implements a worker pool pattern with a fixed number of workers
// sharing a single task channel for load balancing.
//
// If SetWorker is invoked, the supplied template's processor is reused by
// every spawned worker (the template itself is not added to the pool). If no
// template is supplied, a default no-op processor is used that only updates
// worker statistics.
type WorkerPool struct {
	producers  []producer.Producer
	numWorkers int
	bufferSize int
	taskChan   chan interface{}

	// workerTemplate, when non-nil, supplies the ProcessFunc used by every
	// worker. We capture the ProcessFunc rather than the Consumer instance so
	// each worker maintains independent bookkeeping.
	workerTemplate consumer.ProcessFunc

	workers []consumer.Consumer

	mu          sync.Mutex
	errors      []error
	workerStats map[string]*atomic.Int64
	started     bool
}

const defaultWorkerCount = 4

// NewWorkerPool creates a new worker pool.
func NewWorkerPool(numWorkers int, bufferSize int) *WorkerPool {
	if numWorkers <= 0 {
		numWorkers = defaultWorkerCount
	}
	if bufferSize <= 0 {
		bufferSize = defaultBufferSize
	}
	return &WorkerPool{
		producers:   make([]producer.Producer, 0),
		numWorkers:  numWorkers,
		bufferSize:  bufferSize,
		taskChan:    make(chan interface{}, bufferSize),
		workers:     make([]consumer.Consumer, 0, numWorkers),
		errors:      make([]error, 0),
		workerStats: make(map[string]*atomic.Int64, numWorkers),
	}
}

// AddProducer registers a producer.
func (wp *WorkerPool) AddProducer(p producer.Producer) {
	if p == nil {
		return
	}
	wp.producers = append(wp.producers, p)
}

// SetWorker installs a worker template. The template's processor (extracted
// via consumer.Consumer.Processor()) is used by every spawned worker. This
// keeps backwards compatibility with the historical API while ensuring the
// custom logic is actually executed for every item.
//
// If the supplied consumer does not expose a Processor (e.g. it is a wrapper
// that does not embed *consumer.BaseConsumer), the template is registered
// as-is and run as the single worker in the pool.
func (wp *WorkerPool) SetWorker(c consumer.Consumer) {
	if c == nil {
		return
	}
	if bc, ok := extractBaseConsumer(c); ok {
		if fn := bc.Processor(); fn != nil {
			wp.workerTemplate = fn
			// The template is no longer needed as a worker.
			wp.workers = nil
			return
		}
	}
	// Fallback: run the consumer directly as the only worker. We still keep
	// the original wp.numWorkers intact for stats accuracy via spawnWorkers.
	wp.workers = []consumer.Consumer{c}
}

// extractBaseConsumer digs the *BaseConsumer out of common wrapper types so
// that WorkerConsumer, PrintConsumer, AggregateConsumer, etc. all expose their
// processor to SetWorker. Unknown types return ok=false.
type baseExposer interface{ Base() *consumer.BaseConsumer }

func extractBaseConsumer(c consumer.Consumer) (*consumer.BaseConsumer, bool) {
	if be, ok := c.(baseExposer); ok {
		return be.Base(), true
	}
	return nil, false
}

// SetProcessor registers a ProcessFunc used by every spawned worker. This is
// the simplest way to plug custom logic into the pool.
func (wp *WorkerPool) SetProcessor(fn consumer.ProcessFunc) {
	wp.workerTemplate = fn
}

// Run starts the worker pool.
func (wp *WorkerPool) Run(ctx context.Context) error {
	if err := wp.markStarted(); err != nil {
		return err
	}
	if len(wp.producers) == 0 {
		return errors.New("worker pool: no producers configured")
	}

	wp.spawnWorkers()

	consumerWg := sync.WaitGroup{}
	for _, w := range wp.workers {
		consumerWg.Add(1)
		go func(worker consumer.Consumer) {
			defer consumerWg.Done()
			if err := worker.Consume(ctx, wp.taskChan); err != nil && !isContextErr(err) {
				wp.addError(fmt.Errorf("worker %s: %w", worker.ID(), err))
			}
		}(w)
	}

	producerWg := sync.WaitGroup{}
	for _, p := range wp.producers {
		producerWg.Add(1)
		go func(prod producer.Producer) {
			defer producerWg.Done()
			if err := prod.Produce(ctx, wp.taskChan); err != nil && !isContextErr(err) {
				wp.addError(fmt.Errorf("producer %s: %w", prod.ID(), err))
			}
		}(p)
	}

	producerWg.Wait()
	close(wp.taskChan)
	consumerWg.Wait()

	return wp.summariseErrors("worker pool")
}

// spawnWorkers fabricates wp.numWorkers worker consumers. Order of precedence:
//  1. If a single template consumer was supplied via SetWorker (and we could
//     not extract a ProcessFunc from it), keep it as the only worker.
//  2. Otherwise replicate wp.workerTemplate (set via SetProcessor or extracted
//     from the SetWorker template) across all workers.
//  3. If no processor is configured, use a no-op.
//
// In every case wp.workerStats is populated with a counter per worker.
func (wp *WorkerPool) spawnWorkers() {
	// Case 1: a non-extractable template was registered (custom Consumer
	// type without Base()). Use it as-is.
	if len(wp.workers) == 1 && wp.workerTemplate == nil {
		wp.mu.Lock()
		wp.workerStats[wp.workers[0].ID()] = new(atomic.Int64)
		wp.mu.Unlock()
		return
	}

	processor := wp.workerTemplate
	if processor == nil {
		processor = func(data interface{}) error { return nil }
	}

	// Fresh slice of workers; the template (if any) is replaced.
	wp.workers = make([]consumer.Consumer, 0, wp.numWorkers)
	for i := 0; i < wp.numWorkers; i++ {
		workerID := fmt.Sprintf("worker-%d", i)
		counter := new(atomic.Int64)
		wp.mu.Lock()
		wp.workerStats[workerID] = counter
		wp.mu.Unlock()

		userFn := processor // capture by value
		wrapped := func(data interface{}) error {
			err := userFn(data)
			if err == nil {
				counter.Add(1)
			}
			return err
		}
		wp.workers = append(wp.workers, consumer.NewWorkerConsumer(workerID, i, 0, wrapped))
	}
}

func (wp *WorkerPool) markStarted() error {
	wp.mu.Lock()
	defer wp.mu.Unlock()
	if wp.started {
		return ErrAlreadyRun
	}
	wp.started = true
	return nil
}

func (wp *WorkerPool) addError(err error) {
	wp.mu.Lock()
	defer wp.mu.Unlock()
	if len(wp.errors) < maxRetainedErrors {
		wp.errors = append(wp.errors, err)
	}
}

func (wp *WorkerPool) summariseErrors(name string) error {
	wp.mu.Lock()
	defer wp.mu.Unlock()
	if len(wp.errors) == 0 {
		return nil
	}
	return fmt.Errorf("%s: completed with %d errors: %w", name, len(wp.errors), wp.errors[0])
}

// Errors returns a snapshot of recorded errors.
func (wp *WorkerPool) Errors() []error {
	wp.mu.Lock()
	defer wp.mu.Unlock()
	out := make([]error, len(wp.errors))
	copy(out, wp.errors)
	return out
}

// WorkerStats returns a snapshot of per-worker processed counts. Safe to call
// concurrently with Run.
func (wp *WorkerPool) WorkerStats() map[string]int {
	wp.mu.Lock()
	defer wp.mu.Unlock()
	out := make(map[string]int, len(wp.workerStats))
	for k, v := range wp.workerStats {
		out[k] = int(v.Load())
	}
	return out
}

// NumWorkers returns the number of workers in the pool.
func (wp *WorkerPool) NumWorkers() int { return wp.numWorkers }

// QueueDepth returns the current number of items waiting in the task channel.
func (wp *WorkerPool) QueueDepth() int { return len(wp.taskChan) }

// BufferSize returns the configured task-channel buffer size.
func (wp *WorkerPool) BufferSize() int { return wp.bufferSize }
