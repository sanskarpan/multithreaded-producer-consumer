// Package patterns provides various producer-consumer pattern implementations.
//
// All patterns share a common contract:
//   - Producers are started, then consumers, then producers' completion
//     triggers channel closure, which lets consumers drain the remainder and
//     exit cleanly.
//   - Run honours context cancellation: returning early aborts pending work.
//   - Errors emitted by producers/consumers are recorded but do not abort the
//     pattern; Run returns a non-nil error summarising the count.
package patterns

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/sanskarpan/producer-consumer/consumer"
	"github.com/sanskarpan/producer-consumer/producer"
)

// BasicPattern implements the basic unbuffered channel pattern.
//
// Producers and consumers rendezvous directly via an unbuffered channel,
// providing strict ordering with zero buffer overhead at the cost of tight
// coupling and blocking sends/receives.
type BasicPattern struct {
	producers []producer.Producer
	consumers []consumer.Consumer
	dataChan  chan interface{}

	mu     sync.Mutex
	errors []error

	started bool
}

// NewBasicPattern creates a new basic pattern.
func NewBasicPattern() *BasicPattern {
	return &BasicPattern{
		producers: make([]producer.Producer, 0),
		consumers: make([]consumer.Consumer, 0),
		dataChan:  make(chan interface{}),
		errors:    make([]error, 0),
	}
}

// AddProducer registers a producer. Not safe to call concurrently with Run.
func (bp *BasicPattern) AddProducer(p producer.Producer) {
	if p == nil {
		return
	}
	bp.producers = append(bp.producers, p)
}

// AddConsumer registers a consumer. Not safe to call concurrently with Run.
func (bp *BasicPattern) AddConsumer(c consumer.Consumer) {
	if c == nil {
		return
	}
	bp.consumers = append(bp.consumers, c)
}

// Run starts all producers and consumers, blocks until everyone finishes, and
// returns a non-nil error summarising any failures.
//
// Run is single-shot: calling it twice on the same instance is a programming
// error and returns ErrAlreadyRun.
func (bp *BasicPattern) Run(ctx context.Context) error {
	if err := bp.markStarted(); err != nil {
		return err
	}
	if len(bp.producers) == 0 {
		return errors.New("basic pattern: no producers configured")
	}
	if len(bp.consumers) == 0 {
		return errors.New("basic pattern: no consumers configured")
	}

	consumerWg := sync.WaitGroup{}
	for _, c := range bp.consumers {
		consumerWg.Add(1)
		go func(cons consumer.Consumer) {
			defer consumerWg.Done()
			if err := cons.Consume(ctx, bp.dataChan); err != nil && !isContextErr(err) {
				bp.addError(fmt.Errorf("consumer %s: %w", cons.ID(), err))
			}
		}(c)
	}

	producerWg := sync.WaitGroup{}
	for _, p := range bp.producers {
		producerWg.Add(1)
		go func(prod producer.Producer) {
			defer producerWg.Done()
			if err := prod.Produce(ctx, bp.dataChan); err != nil && !isContextErr(err) {
				bp.addError(fmt.Errorf("producer %s: %w", prod.ID(), err))
			}
		}(p)
	}

	producerWg.Wait()
	close(bp.dataChan)
	consumerWg.Wait()

	return bp.summariseErrors("basic pattern")
}

// markStarted ensures Run is invoked at most once.
func (bp *BasicPattern) markStarted() error {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	if bp.started {
		return ErrAlreadyRun
	}
	bp.started = true
	return nil
}

func (bp *BasicPattern) addError(err error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	if len(bp.errors) < maxRetainedErrors {
		bp.errors = append(bp.errors, err)
	}
}

func (bp *BasicPattern) summariseErrors(name string) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	if len(bp.errors) == 0 {
		return nil
	}
	return fmt.Errorf("%s: completed with %d errors: %w", name, len(bp.errors), bp.errors[0])
}

// Errors returns a snapshot of all errors that occurred during execution.
func (bp *BasicPattern) Errors() []error {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	out := make([]error, len(bp.errors))
	copy(out, bp.errors)
	return out
}
