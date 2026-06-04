package patterns

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/sanskarpan/producer-consumer/consumer"
	"github.com/sanskarpan/producer-consumer/producer"
)

// BufferedPattern decouples producers and consumers via a buffered channel so
// transient throughput imbalances are absorbed by the buffer rather than
// blocking producers immediately.
type BufferedPattern struct {
	producers  []producer.Producer
	consumers  []consumer.Consumer
	dataChan   chan interface{}
	bufferSize int

	mu      sync.Mutex
	errors  []error
	started bool
}

// NewBufferedPattern creates a new buffered pattern. A non-positive bufferSize
// is clamped to a sensible default.
func NewBufferedPattern(bufferSize int) *BufferedPattern {
	if bufferSize <= 0 {
		bufferSize = defaultBufferSize
	}
	return &BufferedPattern{
		producers:  make([]producer.Producer, 0),
		consumers:  make([]consumer.Consumer, 0),
		dataChan:   make(chan interface{}, bufferSize),
		bufferSize: bufferSize,
		errors:     make([]error, 0),
	}
}

// AddProducer registers a producer.
func (bp *BufferedPattern) AddProducer(p producer.Producer) {
	if p == nil {
		return
	}
	bp.producers = append(bp.producers, p)
}

// AddConsumer registers a consumer.
func (bp *BufferedPattern) AddConsumer(c consumer.Consumer) {
	if c == nil {
		return
	}
	bp.consumers = append(bp.consumers, c)
}

// Run starts all producers and consumers and blocks until completion.
func (bp *BufferedPattern) Run(ctx context.Context) error {
	if err := bp.markStarted(); err != nil {
		return err
	}
	if len(bp.producers) == 0 {
		return errors.New("buffered pattern: no producers configured")
	}
	if len(bp.consumers) == 0 {
		return errors.New("buffered pattern: no consumers configured")
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

	return bp.summariseErrors("buffered pattern")
}

func (bp *BufferedPattern) markStarted() error {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	if bp.started {
		return ErrAlreadyRun
	}
	bp.started = true
	return nil
}

func (bp *BufferedPattern) addError(err error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	if len(bp.errors) < maxRetainedErrors {
		bp.errors = append(bp.errors, err)
	}
}

func (bp *BufferedPattern) summariseErrors(name string) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	if len(bp.errors) == 0 {
		return nil
	}
	return fmt.Errorf("%s: completed with %d errors: %w", name, len(bp.errors), bp.errors[0])
}

// Errors returns all errors that occurred during execution.
func (bp *BufferedPattern) Errors() []error {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	out := make([]error, len(bp.errors))
	copy(out, bp.errors)
	return out
}

// BufferSize returns the configured buffer size.
func (bp *BufferedPattern) BufferSize() int { return bp.bufferSize }

// ChannelLen returns the current number of items in the buffer. Safe to call
// from any goroutine (len on a buffered channel is atomic in Go).
func (bp *BufferedPattern) ChannelLen() int { return len(bp.dataChan) }

// ChannelCap returns the capacity of the buffer.
func (bp *BufferedPattern) ChannelCap() int { return cap(bp.dataChan) }
