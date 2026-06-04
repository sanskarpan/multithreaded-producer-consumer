package patterns

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sanskarpan/producer-consumer/consumer"
	"github.com/sanskarpan/producer-consumer/producer"
)

// RateLimitedPattern enforces an upper bound on producer and consumer
// throughput. It wraps each producer with a token-bucket-like ticker that
// allows up to producerRate items/sec onto the shared channel, and serves the
// consumer side via a rate-limiting middleware channel that hands items off
// to the registered consumers at the consumer rate.
type RateLimitedPattern struct {
	producers    []producer.Producer
	consumers    []consumer.Consumer
	dataChan     chan interface{}
	bufferSize   int
	producerRate int // items per second across all producers
	consumerRate int // items per second across all consumers

	mu      sync.Mutex
	errors  []error
	started bool

	producedCount atomic.Int64
	consumedCount atomic.Int64
}

const (
	defaultRate = 10
)

// NewRateLimitedPattern creates a new rate-limited pattern.
//
// Non-positive bufferSize / producerRate / consumerRate are clamped to safe
// defaults so the pattern never panics on construction.
func NewRateLimitedPattern(bufferSize, producerRate, consumerRate int) *RateLimitedPattern {
	if bufferSize <= 0 {
		bufferSize = defaultBufferSize
	}
	if producerRate <= 0 {
		producerRate = defaultRate
	}
	if consumerRate <= 0 {
		consumerRate = defaultRate
	}
	return &RateLimitedPattern{
		producers:    make([]producer.Producer, 0),
		consumers:    make([]consumer.Consumer, 0),
		dataChan:     make(chan interface{}, bufferSize),
		bufferSize:   bufferSize,
		producerRate: producerRate,
		consumerRate: consumerRate,
		errors:       make([]error, 0),
	}
}

// AddProducer registers a producer.
func (rl *RateLimitedPattern) AddProducer(p producer.Producer) {
	if p == nil {
		return
	}
	rl.producers = append(rl.producers, p)
}

// AddConsumer registers a consumer. Consumers receive data via their
// Consume method just like in any other pattern.
func (rl *RateLimitedPattern) AddConsumer(c consumer.Consumer) {
	if c == nil {
		return
	}
	rl.consumers = append(rl.consumers, c)
}

// Run starts the rate-limited pattern. It returns when all producers and
// consumers finish (or ctx is cancelled).
func (rl *RateLimitedPattern) Run(ctx context.Context) error {
	if err := rl.markStarted(); err != nil {
		return err
	}
	if len(rl.producers) == 0 {
		return errors.New("rate-limited pattern: no producers configured")
	}
	if len(rl.consumers) == 0 {
		return errors.New("rate-limited pattern: no consumers configured")
	}

	// Throttled channel sits between the rate-limit ticker and the consumers.
	throttled := make(chan interface{}, rl.bufferSize)

	// Consumer fan-out: each consumer drains `throttled` at the consumerRate.
	consumerWg := sync.WaitGroup{}
	for _, c := range rl.consumers {
		consumerWg.Add(1)
		go func(cons consumer.Consumer) {
			defer consumerWg.Done()
			if err := cons.Consume(ctx, throttled); err != nil && !isContextErr(err) {
				rl.addError(fmt.Errorf("consumer %s: %w", cons.ID(), err))
			}
		}(c)
	}

	// One global consumer-rate ticker drains rl.dataChan into the throttled
	// channel. Using a single ticker preserves the global "consumerRate"
	// semantic regardless of how many consumers are attached.
	consumerTickerDone := make(chan struct{})
	go func() {
		defer close(consumerTickerDone)
		interval := time.Second / time.Duration(rl.consumerRate)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				select {
				case data, ok := <-rl.dataChan:
					if !ok {
						return
					}
					select {
					case throttled <- data:
					case <-ctx.Done():
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	// Per-producer rate limiting: each producer feeds a private channel that
	// is then rate-limited into rl.dataChan by its own ticker.
	producerWg := sync.WaitGroup{}
	for _, p := range rl.producers {
		producerWg.Add(1)
		go rl.runProducer(ctx, p, &producerWg)
	}

	producerWg.Wait()
	close(rl.dataChan)
	<-consumerTickerDone
	close(throttled)
	consumerWg.Wait()

	// consumedCount is the count of items the registered consumers actually
	// processed (sum of their ConsumedCount), which is the source of truth.
	rl.consumedCount.Store(0)
	for _, c := range rl.consumers {
		if ac, ok := c.(interface{ ConsumedCount() int }); ok {
			rl.consumedCount.Add(int64(ac.ConsumedCount()))
		}
	}

	return rl.summariseErrors("rate-limited pattern")
}

// runProducer wraps a single producer with rate limiting.
func (rl *RateLimitedPattern) runProducer(ctx context.Context, prod producer.Producer, wg *sync.WaitGroup) {
	defer wg.Done()

	tempChan := make(chan interface{}, rl.bufferSize)
	producerErrChan := make(chan error, 1)
	go func() {
		producerErrChan <- prod.Produce(ctx, tempChan)
		close(tempChan)
	}()

	interval := time.Second / time.Duration(rl.producerRate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			rl.drainProducer(producerErrChan, prod)
			return
		case <-ticker.C:
			select {
			case data, ok := <-tempChan:
				if !ok {
					rl.drainProducer(producerErrChan, prod)
					return
				}
				select {
				case rl.dataChan <- data:
					rl.producedCount.Add(1)
				case <-ctx.Done():
					rl.drainProducer(producerErrChan, prod)
					return
				}
			case <-ctx.Done():
				rl.drainProducer(producerErrChan, prod)
				return
			}
		}
	}
}

// drainProducer waits for the inner producer to finish and records any error.
// Uses an explicit timer (not time.After) so the timer is always released.
func (rl *RateLimitedPattern) drainProducer(producerErrChan chan error, prod producer.Producer) {
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case err := <-producerErrChan:
		if err != nil && !isContextErr(err) {
			rl.addError(fmt.Errorf("producer %s: %w", prod.ID(), err))
		}
	case <-timer.C:
		// Safety bound: the inner producer should respect context cancellation
		// and exit promptly. If it doesn't, surface that as an error.
		rl.addError(fmt.Errorf("producer %s: did not exit within 5s of cancellation", prod.ID()))
	}
}

func (rl *RateLimitedPattern) markStarted() error {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	if rl.started {
		return ErrAlreadyRun
	}
	rl.started = true
	return nil
}

func (rl *RateLimitedPattern) addError(err error) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	if len(rl.errors) < maxRetainedErrors {
		rl.errors = append(rl.errors, err)
	}
}

func (rl *RateLimitedPattern) summariseErrors(name string) error {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	if len(rl.errors) == 0 {
		return nil
	}
	return fmt.Errorf("%s: completed with %d errors: %w", name, len(rl.errors), rl.errors[0])
}

// Errors returns a snapshot of recorded errors.
func (rl *RateLimitedPattern) Errors() []error {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	out := make([]error, len(rl.errors))
	copy(out, rl.errors)
	return out
}

// Stats returns running production / consumption counters.
func (rl *RateLimitedPattern) Stats() (produced, consumed int) {
	return int(rl.producedCount.Load()), int(rl.consumedCount.Load())
}

// ProducerRate returns the configured producer rate.
func (rl *RateLimitedPattern) ProducerRate() int { return rl.producerRate }

// ConsumerRate returns the configured consumer rate.
func (rl *RateLimitedPattern) ConsumerRate() int { return rl.consumerRate }

// QueueDepth returns the current depth of the main data channel.
func (rl *RateLimitedPattern) QueueDepth() int { return len(rl.dataChan) }

// BufferSize returns the configured channel buffer size.
func (rl *RateLimitedPattern) BufferSize() int { return rl.bufferSize }
