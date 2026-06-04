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

// FanOutFanIn implements the fan-out/fan-in pattern: multiple workers process
// items from a shared input channel in parallel, and successfully processed
// items are merged into a single output channel.
//
// The caller is responsible for draining OutputChan(); if the channel fills up
// because no goroutine is reading, workers will block on the send. To use the
// pattern in a "fire-and-forget" mode (Run with no external reader), call
// DiscardOutput before Run. Doing so spawns an internal drain goroutine.
type FanOutFanIn struct {
	producers  []producer.Producer
	numWorkers int
	processor  consumer.ProcessFunc
	inputChan  chan interface{}
	outputChan chan interface{}
	bufferSize int

	mu        sync.Mutex
	errors    []error
	processed atomic.Int64

	discardOutput bool
	started       bool
}

// NewFanOutFanIn creates a new fan-out/fan-in pattern.
func NewFanOutFanIn(numWorkers int, bufferSize int, processor consumer.ProcessFunc) *FanOutFanIn {
	if numWorkers <= 0 {
		numWorkers = defaultWorkerCount
	}
	if bufferSize <= 0 {
		bufferSize = defaultBufferSize
	}
	if processor == nil {
		processor = func(data interface{}) error { return nil }
	}
	return &FanOutFanIn{
		producers:  make([]producer.Producer, 0),
		numWorkers: numWorkers,
		processor:  processor,
		inputChan:  make(chan interface{}, bufferSize),
		outputChan: make(chan interface{}, bufferSize),
		bufferSize: bufferSize,
		errors:     make([]error, 0),
	}
}

// AddProducer registers a producer.
func (f *FanOutFanIn) AddProducer(p producer.Producer) {
	if p == nil {
		return
	}
	f.producers = append(f.producers, p)
}

// DiscardOutput tells the pattern to consume the output channel internally so
// callers can use Run as a self-contained operation.
func (f *FanOutFanIn) DiscardOutput() { f.discardOutput = true }

// Run starts the fan-out/fan-in pattern and blocks until completion.
//
// IMPORTANT: unless DiscardOutput has been called, the caller MUST drain
// OutputChan() in a separate goroutine for Run to complete. The output
// channel is closed automatically when all workers have finished.
func (f *FanOutFanIn) Run(ctx context.Context) error {
	if err := f.markStarted(); err != nil {
		return err
	}
	if len(f.producers) == 0 {
		return errors.New("fan-out/fan-in: no producers configured")
	}

	if f.discardOutput {
		// Internal drain to keep workers unblocked even when callers don't read.
		go func() {
			for range f.outputChan {
			}
		}()
	}

	// Start producers.
	producerWg := sync.WaitGroup{}
	for _, p := range f.producers {
		producerWg.Add(1)
		go func(prod producer.Producer) {
			defer producerWg.Done()
			if err := prod.Produce(ctx, f.inputChan); err != nil && !isContextErr(err) {
				f.addError(fmt.Errorf("producer %s: %w", prod.ID(), err))
			}
		}(p)
	}

	// Close inputChan once all producers are done.
	go func() {
		producerWg.Wait()
		close(f.inputChan)
	}()

	// Workers.
	workerWg := sync.WaitGroup{}
	for i := 0; i < f.numWorkers; i++ {
		workerWg.Add(1)
		go func(workerID int) {
			defer workerWg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case data, ok := <-f.inputChan:
					if !ok {
						return
					}
					if err := f.processSafe(data); err != nil {
						f.addError(fmt.Errorf("worker %d: %w", workerID, err))
						continue
					}
					select {
					case f.outputChan <- data:
						f.processed.Add(1)
					case <-ctx.Done():
						return
					}
				}
			}
		}(i)
	}

	// When all workers finish, close output channel.
	go func() {
		workerWg.Wait()
		close(f.outputChan)
	}()

	workerWg.Wait()

	return f.summariseErrors("fan-out/fan-in")
}

// processSafe wraps the processor so a panicking processor does not crash the worker.
func (f *FanOutFanIn) processSafe(data interface{}) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("processor panic: %v", r)
		}
	}()
	return f.processor(data)
}

// OutputChan returns the channel from which the caller can consume processed
// items. It is closed by the pattern once all workers exit.
func (f *FanOutFanIn) OutputChan() <-chan interface{} { return f.outputChan }

// Processed returns the number of items successfully processed.
func (f *FanOutFanIn) Processed() int { return int(f.processed.Load()) }

// QueueDepth returns the current depth of the inbound work queue.
func (f *FanOutFanIn) QueueDepth() int { return len(f.inputChan) }

// BufferSize returns the configured channel buffer size.
func (f *FanOutFanIn) BufferSize() int { return f.bufferSize }

func (f *FanOutFanIn) markStarted() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.started {
		return ErrAlreadyRun
	}
	f.started = true
	return nil
}

func (f *FanOutFanIn) addError(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.errors) < maxRetainedErrors {
		f.errors = append(f.errors, err)
	}
}

func (f *FanOutFanIn) summariseErrors(name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.errors) == 0 {
		return nil
	}
	return fmt.Errorf("%s: completed with %d errors: %w", name, len(f.errors), f.errors[0])
}

// Errors returns a snapshot of recorded errors.
func (f *FanOutFanIn) Errors() []error {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]error, len(f.errors))
	copy(out, f.errors)
	return out
}
