package patterns

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/sanskarpan/producer-consumer/consumer"
	"github.com/sanskarpan/producer-consumer/producer"
)

// Stage represents a processing stage in the pipeline.
//
// Each stage runs Workers goroutines, all pulling from the previous stage's
// output channel and writing to the next stage's input channel.
type Stage struct {
	Name      string
	Processor consumer.ProcessFunc
	Workers   int
}

// Pipeline implements a multi-stage processing pipeline.
//
// Items flow producer -> stage[0] -> stage[1] -> ... -> sink. Each stage owns
// its own buffered channel of size bufferSize.
type Pipeline struct {
	producers  []producer.Producer
	stages     []Stage
	bufferSize int

	mu      sync.Mutex
	errors  []error
	started bool
}

// NewPipeline creates a new pipeline.
func NewPipeline(bufferSize int) *Pipeline {
	if bufferSize <= 0 {
		bufferSize = defaultBufferSize
	}
	return &Pipeline{
		producers:  make([]producer.Producer, 0),
		stages:     make([]Stage, 0),
		bufferSize: bufferSize,
		errors:     make([]error, 0),
	}
}

// AddProducer registers a producer.
func (p *Pipeline) AddProducer(prod producer.Producer) {
	if prod == nil {
		return
	}
	p.producers = append(p.producers, prod)
}

// AddStage appends a processing stage. workers<=0 is clamped to 1.
func (p *Pipeline) AddStage(name string, processor consumer.ProcessFunc, workers int) {
	if workers <= 0 {
		workers = 1
	}
	if processor == nil {
		processor = func(data interface{}) error { return nil }
	}
	p.stages = append(p.stages, Stage{Name: name, Processor: processor, Workers: workers})
}

// Run executes the pipeline and drains the final stage's output so the caller
// does not have to manage the sink channel. It blocks until completion.
func (p *Pipeline) Run(ctx context.Context) error {
	out, err := p.startWithOutput(ctx)
	if err != nil {
		return err
	}
	// Drain the final stage to prevent the last stage's workers from blocking
	// when itemCount > bufferSize.
	for range out {
	}
	return p.summariseErrors("pipeline")
}

// RunWithOutput executes the pipeline and returns the output channel of the
// final stage. The caller MUST drain the returned channel; otherwise the last
// stage's workers will eventually block.
//
// Errors are accumulated and exposed through Errors(). Unlike Run, this method
// returns as soon as the goroutines are launched.
func (p *Pipeline) RunWithOutput(ctx context.Context) (<-chan interface{}, error) {
	return p.startWithOutput(ctx)
}

// startWithOutput sets up channels and goroutines for the pipeline and returns
// the final-stage output channel.
func (p *Pipeline) startWithOutput(ctx context.Context) (<-chan interface{}, error) {
	if err := p.markStarted(); err != nil {
		return nil, err
	}
	if len(p.producers) == 0 {
		return nil, errors.New("pipeline: no producers configured")
	}
	if len(p.stages) == 0 {
		return nil, errors.New("pipeline: no stages configured")
	}

	// channels[0]: producer output. channels[i]: input to stage i (== output of stage i-1).
	// channels[len(stages)]: output of last stage (returned to caller).
	channels := make([]chan interface{}, len(p.stages)+1)
	for i := range channels {
		channels[i] = make(chan interface{}, p.bufferSize)
	}

	// Producers.
	producerWg := sync.WaitGroup{}
	for _, prod := range p.producers {
		producerWg.Add(1)
		go func(pr producer.Producer) {
			defer producerWg.Done()
			if err := pr.Produce(ctx, channels[0]); err != nil && !isContextErr(err) {
				p.addError(fmt.Errorf("producer %s: %w", pr.ID(), err))
			}
		}(prod)
	}
	go func() {
		producerWg.Wait()
		close(channels[0])
	}()

	// Stages.
	for stageIdx, stage := range p.stages {
		inputChan := channels[stageIdx]
		outputChan := channels[stageIdx+1]
		stageWg := &sync.WaitGroup{}
		for w := 0; w < stage.Workers; w++ {
			stageWg.Add(1)
			go p.runStageWorker(ctx, stage, w, inputChan, outputChan, stageWg)
		}
		go func(idx int) {
			stageWg.Wait()
			close(channels[idx+1])
		}(stageIdx)
	}

	return channels[len(p.stages)], nil
}

// runStageWorker is the per-worker loop for a stage.
func (p *Pipeline) runStageWorker(ctx context.Context, stage Stage, workerIdx int, in <-chan interface{}, out chan<- interface{}, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case data, ok := <-in:
			if !ok {
				return
			}
			if err := p.processSafe(stage, data); err != nil {
				p.addError(fmt.Errorf("stage %s worker %d: %w", stage.Name, workerIdx, err))
				continue
			}
			select {
			case out <- data:
			case <-ctx.Done():
				return
			}
		}
	}
}

// processSafe wraps a stage's processor in panic recovery so a faulty
// processor cannot kill the whole pipeline.
func (p *Pipeline) processSafe(stage Stage, data interface{}) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("processor panic: %v", r)
		}
	}()
	return stage.Processor(data)
}

func (p *Pipeline) markStarted() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.started {
		return ErrAlreadyRun
	}
	p.started = true
	return nil
}

func (p *Pipeline) addError(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.errors) < maxRetainedErrors {
		p.errors = append(p.errors, err)
	}
}

func (p *Pipeline) summariseErrors(name string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.errors) == 0 {
		return nil
	}
	return fmt.Errorf("%s: completed with %d errors: %w", name, len(p.errors), p.errors[0])
}

// Errors returns a snapshot of recorded errors.
func (p *Pipeline) Errors() []error {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]error, len(p.errors))
	copy(out, p.errors)
	return out
}

// BufferSize returns the configured per-stage buffer size.
func (p *Pipeline) BufferSize() int { return p.bufferSize }

// NumStages returns the number of stages currently registered.
func (p *Pipeline) NumStages() int { return len(p.stages) }
