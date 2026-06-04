// Package producer provides producer implementations for producer-consumer patterns.
//
// Producers are responsible for generating data items and sending them to an
// output channel. All producer implementations honour context cancellation and
// are safe to start in a single goroutine; the public counter accessors are
// safe to call concurrently from any goroutine via atomic operations.
package producer

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

// ErrInvalidRate is returned by rate-limited constructors when the supplied
// rate is non-positive.
var ErrInvalidRate = errors.New("producer: rate must be > 0")

// Producer defines the interface for data producers.
//
// Produce must close cleanly when ctx is cancelled. Producers MUST NOT close
// the output channel themselves; the orchestrating pattern owns the channel
// lifecycle.
type Producer interface {
	Produce(ctx context.Context, out chan<- interface{}) error
	ID() string
}

// BaseProducer implements the bookkeeping shared by all built-in producers.
//
// The struct is intentionally exported so consumers can embed it when building
// custom producers (see README "Advanced Usage").
type BaseProducer struct {
	id            string
	dataGenerator func(int) interface{}
	count         int
	delay         time.Duration
	produced      atomic.Int64
}

// NewBaseProducer constructs a generic producer. If generator is nil a default
// "data-N" string generator is used. count<0 is treated as 0.
func NewBaseProducer(id string, count int, delay time.Duration, generator func(int) interface{}) *BaseProducer {
	if count < 0 {
		count = 0
	}
	if delay < 0 {
		delay = 0
	}
	if generator == nil {
		generator = func(i int) interface{} {
			return fmt.Sprintf("data-%d", i)
		}
	}
	return &BaseProducer{
		id:            id,
		dataGenerator: generator,
		count:         count,
		delay:         delay,
	}
}

// ID returns the producer's identifier.
func (p *BaseProducer) ID() string {
	return p.id
}

// Produce generates and sends data to the output channel.
//
// The function returns nil on clean completion (all items sent) and
// context.Canceled / context.DeadlineExceeded if the context terminates first.
// It never closes the channel.
func (p *BaseProducer) Produce(ctx context.Context, out chan<- interface{}) error {
	if out == nil {
		return errors.New("producer: output channel is nil")
	}
	for i := 0; i < p.count; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		data := p.dataGenerator(i)
		select {
		case out <- data:
			p.produced.Add(1)
		case <-ctx.Done():
			return ctx.Err()
		}

		if p.delay > 0 {
			t := time.NewTimer(p.delay)
			select {
			case <-t.C:
			case <-ctx.Done():
				t.Stop()
				return ctx.Err()
			}
		}
	}
	return nil
}

// ProducedCount returns the number of items emitted by this producer. Safe to
// call from any goroutine.
func (p *BaseProducer) ProducedCount() int {
	return int(p.produced.Load())
}

// IntProducer produces sequential integer values starting at start.
type IntProducer struct {
	*BaseProducer
}

// NewIntProducer creates a new integer producer.
func NewIntProducer(id string, start, count int, delay time.Duration) *IntProducer {
	generator := func(i int) interface{} {
		return start + i
	}
	return &IntProducer{BaseProducer: NewBaseProducer(id, count, delay, generator)}
}

// StringProducer emits formatted strings of the form "<prefix>-<id>-<i>".
type StringProducer struct {
	*BaseProducer
}

// NewStringProducer creates a new string producer.
func NewStringProducer(id string, prefix string, count int, delay time.Duration) *StringProducer {
	generator := func(i int) interface{} {
		return fmt.Sprintf("%s-%s-%d", prefix, id, i)
	}
	return &StringProducer{BaseProducer: NewBaseProducer(id, count, delay, generator)}
}

// Task represents a unit of work flowing through a pattern.
type Task struct {
	ID       int
	Data     interface{}
	Priority int
}

// TaskProducer produces Task items with monotonically increasing IDs.
type TaskProducer struct {
	*BaseProducer
	priority int
	taskID   atomic.Int64 // shared by the generator closure; atomic for safety
}

// NewTaskProducer creates a new task producer.
//
// Task IDs are 1-based and monotonically increasing per producer instance.
// They are generated atomically so the producer remains race-free if reused.
func NewTaskProducer(id string, count int, priority int, delay time.Duration) *TaskProducer {
	tp := &TaskProducer{priority: priority}
	generator := func(i int) interface{} {
		next := tp.taskID.Add(1)
		return Task{
			ID:       int(next),
			Data:     fmt.Sprintf("task-data-%d", next),
			Priority: priority,
		}
	}
	tp.BaseProducer = NewBaseProducer(id, count, delay, generator)
	return tp
}

// RateLimitedProducer produces data while enforcing a maximum throughput.
type RateLimitedProducer struct {
	*BaseProducer
	rate int // items per second
}

// NewRateLimitedProducer creates a new rate-limited producer.
//
// A non-positive rate would otherwise cause a runtime divide-by-zero when
// computing the ticker interval; we clamp to a sane default and emit a hint
// via the returned error so callers can react.
func NewRateLimitedProducer(id string, count int, rate int, generator func(int) interface{}) *RateLimitedProducer {
	if rate <= 0 {
		rate = 1
	}
	return &RateLimitedProducer{
		BaseProducer: NewBaseProducer(id, count, 0, generator),
		rate:         rate,
	}
}

// Rate returns the configured throughput in items/second.
func (p *RateLimitedProducer) Rate() int { return p.rate }

// Produce generates and sends data with rate limiting.
//
// The ticker is created and stopped per-call so concurrent invocations do
// not share state (and shutdown is deterministic).
func (p *RateLimitedProducer) Produce(ctx context.Context, out chan<- interface{}) error {
	if out == nil {
		return errors.New("producer: output channel is nil")
	}
	if p.rate <= 0 {
		return ErrInvalidRate
	}
	interval := time.Second / time.Duration(p.rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for i := 0; i < p.count; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			data := p.dataGenerator(i)
			select {
			case out <- data:
				p.produced.Add(1)
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return nil
}
