# Multithreaded Producer-Consumer Patterns in Go

A comprehensive implementation of producer-consumer concurrency patterns in Go, demonstrating various approaches to coordinating concurrent producers and consumers.

## Features

### 6 Concurrency Patterns

1. **Basic (Unbuffered Channel)** - Direct hand-off between producers and consumers
2. **Buffered Channel** - Decoupling with buffered channels for throughput
3. **Worker Pool** - Fixed number of workers processing from a shared queue
4. **Fan-Out/Fan-In** - Parallel processing with result aggregation
5. **Pipeline** - Multi-stage data processing
6. **Rate-Limited** - Throughput control with rate limiting

### Core Features

- **Type-safe abstractions** for producers and consumers
- **Context-based cancellation** for graceful shutdown
- **Thread-safe** implementations with proper synchronization
- **Comprehensive metrics** and statistics tracking
- **Flexible configuration** for different use cases
- **Production-ready** error handling

### Web UI & Visualization 🎨

- **Real-time web dashboard** with live metrics
- **Interactive pattern selector** with parameter controls
- **WebSocket-based** real-time updates
- **Chart.js visualizations** showing throughput, queue depth, and buffer utilization
- **Pattern comparison** side-by-side

### Performance Testing 📊

- **17 comprehensive benchmarks** covering all patterns
- **Parallel benchmarks** for concurrency testing
- **Memory allocation tracking**
- **Throughput measurements** with different configurations

## Installation

```bash
# Clone the repository
git clone <repository-url>
cd Multithreaded-producer-consumer

# Install dependencies
go mod download

# Run tests
go test ./... -v

# Run interactive demo
go run cmd/demo/main.go
```

## Quick Start

### 🌐 Web UI (Recommended)

The easiest way to explore the patterns is through the interactive web UI:

```bash
# Build and run the web UI
go run cmd/webui/main.go

# Or build and run separately
go build -o webui cmd/webui/main.go
./webui
```

Then open http://localhost:8080 in your browser to:
- Select any of the 6 patterns
- Adjust parameters (buffer size, worker count, rates)
- Watch real-time metrics and charts
- See throughput, queue depth, and buffer utilization live

### 💻 CLI Demo

```bash
# Interactive CLI demo
go run cmd/demo/main.go
```

### 📝 Code Usage

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/sanskarpan/producer-consumer/consumer"
    "github.com/sanskarpan/producer-consumer/patterns"
    "github.com/sanskarpan/producer-consumer/producer"
)

func main() {
    // Create pattern
    pattern := patterns.NewBasicPattern()

    // Add producer and consumer
    prod := producer.NewIntProducer("P1", 0, 10, 0)
    cons := consumer.NewPrintConsumer("C1", 0)
    pattern.AddProducer(prod)
    pattern.AddConsumer(cons)

    // Run
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    if err := pattern.Run(ctx); err != nil {
        fmt.Printf("Error: %v\n", err)
    }
}
```

## Project Structure

```
Multithreaded-producer-consumer/
├── producer/              # Producer implementations
│   └── producer.go       # Base, Int, String, Task, RateLimited producers
├── consumer/             # Consumer implementations
│   └── consumer.go       # Base, Print, Aggregate, Worker, Batch consumers
├── patterns/             # Pattern implementations and tests
│   ├── basic.go          # Unbuffered channel pattern
│   ├── basic_test.go     # Tests for basic pattern
│   ├── buffered.go       # Buffered channel pattern
│   ├── buffered_test.go  # Tests for buffered pattern
│   ├── worker_pool.go    # Worker pool pattern
│   ├── worker_pool_test.go
│   ├── fan_out_fan_in.go # Fan-out/fan-in pattern
│   ├── fan_out_fan_in_test.go
│   ├── pipeline.go       # Pipeline pattern
│   ├── pipeline_test.go
│   ├── rate_limited.go   # Rate-limited pattern
│   ├── rate_limited_test.go
│   └── benchmark_test.go # Performance benchmarks (17 benchmarks)
├── metrics/              # Real-time metrics collection
│   └── metrics.go       # Metrics collector with pub/sub
├── web/                  # Web UI
│   ├── server/          # HTTP & WebSocket server
│   │   ├── server.go    # REST API & pattern execution
│   │   └── websocket.go # WebSocket for real-time updates
│   └── static/          # Frontend assets
│       ├── index.html   # Main UI
│       ├── style.css    # Styling
│       └── app.js       # Interactive visualizations
├── examples/             # Working examples (6 patterns)
│   ├── basic/
│   ├── buffered/
│   ├── worker_pool/
│   ├── fan_out_fan_in/
│   ├── pipeline/
│   └── rate_limited/
├── cmd/
│   ├── demo/            # Interactive CLI demo
│   └── webui/           # Web UI server entry point
├── go.mod               # Go module definition
├── .gitignore          # Git ignore rules
└── README.md           # This file
```

## Patterns

### 1. Basic (Unbuffered Channel)

Direct synchronization between producers and consumers using unbuffered channels.

**When to use:**
- Need strict ordering guarantees
- Want minimal memory overhead
- Direct communication is acceptable

**Example:**
```go
pattern := patterns.NewBasicPattern()
pattern.AddProducer(producer.NewIntProducer("P1", 0, 100, 0))
pattern.AddConsumer(consumer.NewPrintConsumer("C1", 0))
pattern.Run(ctx)
```

**Characteristics:**
- ✅ Zero buffer overhead
- ✅ Guaranteed delivery
- ⚠️ Blocking sends/receives
- ⚠️ Tight coupling

### 2. Buffered Channel

Decouples producers and consumers with a buffer to smooth out rate differences.

**When to use:**
- Producers and consumers have different rates
- Want to reduce blocking
- Can tolerate buffering delay

**Example:**
```go
pattern := patterns.NewBufferedPattern(50) // Buffer size: 50
pattern.AddProducer(producer.NewIntProducer("P1", 0, 100, 0))
pattern.AddConsumer(consumer.NewAggregateConsumer("C1", 0))
pattern.Run(ctx)
```

**Characteristics:**
- ✅ Decouples producer/consumer rates
- ✅ Reduces blocking
- ⚠️ Memory overhead
- ⚠️ Potential buffering delays

### 3. Worker Pool

Fixed number of workers process tasks from a shared queue.

**When to use:**
- Need to limit concurrent processing
- Tasks can be processed independently
- Want load balancing across workers

**Example:**
```go
pool := patterns.NewWorkerPool(4, 100) // 4 workers, buffer: 100
pool.AddProducer(producer.NewTaskProducer("P1", 100, 1, 0))
pool.Run(ctx)

// Get worker statistics
stats := pool.WorkerStats()
```

**Characteristics:**
- ✅ Bounded concurrency
- ✅ Automatic load balancing
- ✅ Efficient resource usage
- ⚠️ May underutilize with few tasks

### 4. Fan-Out/Fan-In

Multiple workers process items in parallel, results are merged into a single output channel.

**When to use:**
- Need parallel processing
- Want to collect all results
- CPU-bound processing

**Example:**
```go
processor := func(data interface{}) error {
    // Process data
    return nil
}

pattern := patterns.NewFanOutFanIn(4, 100, processor)
pattern.AddProducer(producer.NewIntProducer("P1", 0, 100, 0))

// Either drain the output channel yourself, OR call DiscardOutput() to
// tell the pattern to consume it internally.
pattern.DiscardOutput()
if err := pattern.Run(ctx); err != nil {
    log.Fatal(err)
}

// Or, for caller-driven drain:
pattern2 := patterns.NewFanOutFanIn(4, 100, processor)
pattern2.AddProducer(producer.NewIntProducer("P1", 0, 100, 0))
go func() { _ = pattern2.Run(ctx) }()
for result := range pattern2.OutputChan() {
    fmt.Println(result)
}
```

**Characteristics:**
- ✅ Parallel processing
- ✅ Result aggregation
- ✅ High throughput
- ⚠️ Unordered results

### 5. Pipeline

Multi-stage processing where each stage transforms data.

**When to use:**
- Data needs multiple transformations
- Stages have different processing rates
- Want to parallelize stages

**Example:**
```go
pipeline := patterns.NewPipeline(50)
pipeline.AddProducer(producer.NewStringProducer("P1", "data", 100, 0))

// Add processing stages
pipeline.AddStage("Uppercase", func(data interface{}) error {
    // Transform data
    return nil
}, 2) // 2 workers for this stage

pipeline.AddStage("Validate", func(data interface{}) error {
    // Validate data
    return nil
}, 4) // 4 workers for this stage

pipeline.Run(ctx)
```

**Characteristics:**
- ✅ Clear separation of concerns
- ✅ Parallelism within stages
- ✅ Flexible stage configuration
- ⚠️ Complexity increases with stages

### 6. Rate-Limited

Controls throughput with configurable rate limiting.

**When to use:**
- Need to limit API calls
- Prevent resource exhaustion
- Comply with rate limits

**Example:**
```go
// 10 items/sec for producer, 15 items/sec for consumer
pattern := patterns.NewRateLimitedPattern(50, 10, 15)
pattern.AddProducer(producer.NewIntProducer("P1", 0, 100, 0))
pattern.AddConsumer(consumer.NewAggregateConsumer("C1", 0))
pattern.Run(ctx)

produced, consumed := pattern.Stats()
```

**Characteristics:**
- ✅ Precise rate control
- ✅ Prevents overwhelming downstream
- ✅ Configurable rates
- ⚠️ May reduce overall throughput

## API Documentation

### Producers

#### BaseProducer
```go
type Producer interface {
    Produce(ctx context.Context, out chan<- interface{}) error
    ID() string
}
```

#### Built-in Producers

**IntProducer** - Generates integer sequences
```go
prod := producer.NewIntProducer(id string, start int, count int, delay time.Duration)
```

**StringProducer** - Generates string data
```go
prod := producer.NewStringProducer(id string, prefix string, count int, delay time.Duration)
```

**TaskProducer** - Generates task items with priority
```go
prod := producer.NewTaskProducer(id string, count int, priority int, delay time.Duration)
```

**RateLimitedProducer** - Produces with rate limiting
```go
prod := producer.NewRateLimitedProducer(id string, count int, rate int, generator func(int) interface{})
```

### Consumers

#### BaseConsumer
```go
type Consumer interface {
    Consume(ctx context.Context, in <-chan interface{}) error
    ID() string
}
```

#### Built-in Consumers

**PrintConsumer** - Prints consumed items
```go
cons := consumer.NewPrintConsumer(id string, delay time.Duration)
```

**AggregateConsumer** - Collects all consumed items
```go
cons := consumer.NewAggregateConsumer(id string, delay time.Duration)
data := cons.GetData()
```

**WorkerConsumer** - Custom processing with tracking
```go
processor := func(data interface{}) error { return nil }
cons := consumer.NewWorkerConsumer(id string, workerID int, delay time.Duration, processor ProcessFunc)
```

**BatchConsumer** - Processes items in batches
```go
batchProcessor := func(batch []interface{}) error { return nil }
cons := consumer.NewBatchConsumer(id string, batchSize int, delay time.Duration, batchProcessor func([]interface{}) error)
```

## Examples

All examples are in the `examples/` directory:

```bash
# Run individual examples
go run examples/basic/main.go
go run examples/buffered/main.go
go run examples/worker_pool/main.go
go run examples/fan_out_fan_in/main.go
go run examples/pipeline/main.go
go run examples/rate_limited/main.go

# Run interactive demo
go run cmd/demo/main.go
```

## Testing

Comprehensive test suite covering all patterns. The exact test count is the
result of `go test ./... -v`; new regression tests are added as bugs are found
and fixed. To see the current count:

```bash
go test ./... -v 2>&1 | grep -c "^=== RUN"
```

```bash
# Run all tests
go test ./... -v

# Run with coverage
go test ./... -cover

# Run with race detector
go test ./... -race

# Run specific pattern tests
go test ./patterns -run=TestBasic -v
go test ./patterns -run=TestBuffered -v
go test ./patterns -run=TestWorkerPool -v
go test ./patterns -run=TestFanOutFanIn -v
go test ./patterns -run=TestPipeline -v
go test ./patterns -run=TestRateLimited -v
```

## Benchmarking

Run performance benchmarks to measure throughput and efficiency:

```bash
# Run all benchmarks
go test ./patterns -bench=. -benchmem

# Run specific pattern benchmarks
go test ./patterns -bench=BenchmarkBasicPattern
go test ./patterns -bench=BenchmarkWorkerPool
go test ./patterns -bench=BenchmarkThroughput

# Compare different configurations
go test ./patterns -bench=BenchmarkBufferedPattern

# Example output:
# BenchmarkBasicPattern-11                     19280    32015 ns/op
# BenchmarkBufferedPattern-11                  32964    18249 ns/op
# BenchmarkWorkerPool_4Workers-11              15770    37034 ns/op
# BenchmarkThroughput_Buffered_1000Items-11     3754   145954 ns/op
```

### Benchmark Results

Sample benchmark numbers from one development machine. Actual numbers vary by
hardware, OS, Go version, and workload; run the benchmarks on your target
platform for representative figures.

| Pattern | Operations | Time/op | Throughput |
|---------|-----------|---------|------------|
| Basic | ~20K | ~30 μs | ~33K ops/sec |
| Buffered | ~30K | ~20 μs | ~50K ops/sec |
| Worker Pool (4) | ~15K | ~40 μs | ~25K ops/sec |
| Fan-Out/Fan-In (4) | ~15K | ~40 μs | ~25K ops/sec |
| Pipeline (3 stages) | ~8K | ~70 μs | ~14K ops/sec |

## Pattern Comparison

| Pattern | Throughput | Latency | Memory | Complexity | Best For |
|---------|-----------|---------|--------|------------|----------|
| Basic | Low | Low | Very Low | Low | Simple synchronization |
| Buffered | Medium | Medium | Medium | Low | Rate mismatch handling |
| Worker Pool | High | Medium | Medium | Medium | Bounded concurrency |
| Fan-Out/Fan-In | Very High | Medium | High | High | Parallel processing |
| Pipeline | High | High | High | High | Multi-stage processing |
| Rate-Limited | Low | High | Medium | Medium | Rate control |

## Performance Tips

1. **Buffer Sizing**: Start with `2 * numProducers * avgBatchSize` and adjust based on profiling
2. **Worker Count**: Set to `runtime.NumCPU()` for CPU-bound work, higher for I/O-bound
3. **Context Cancellation**: Always use contexts with timeouts for production code
4. **Resource Cleanup**: Ensure goroutines exit cleanly to prevent leaks
5. **Backpressure**: Use buffered channels or rate limiting to handle slow consumers

## Advanced Usage

### Custom Producers

```go
type CustomProducer struct {
    *producer.BaseProducer
}

func NewCustomProducer(id string) *CustomProducer {
    generator := func(i int) interface{} {
        return customDataType{ID: i}
    }
    return &CustomProducer{
        BaseProducer: producer.NewBaseProducer(id, 100, 0, generator),
    }
}
```

### Custom Consumers

```go
processor := func(data interface{}) error {
    // Custom processing logic
    return nil
}

cons := consumer.NewBaseConsumer("custom", 0, processor)
```

### Graceful Shutdown

```go
ctx, cancel := context.WithCancel(context.Background())

// Handle shutdown signal
go func() {
    <-shutdownSignal
    cancel() // Triggers graceful shutdown
}()

pattern.Run(ctx)
```

## Common Pitfalls

1. **Goroutine Leaks**: Always ensure goroutines can exit (use contexts)
2. **Channel Deadlocks**: Close channels from producer side only
3. **Race Conditions**: Use proper synchronization for shared state
4. **Unbounded Buffers**: Always set reasonable buffer limits
5. **Missing Error Handling**: Check and handle errors from Run() methods

## Contributing

Contributions are welcome! Please ensure:
- All tests pass (`go test ./...`)
- Code is formatted (`go fmt ./...`)
- No race conditions (`go test -race ./...`)
- Documentation is updated

## License

MIT License - see LICENSE file for details

## Acknowledgments

Built as a comprehensive reference for Go concurrency patterns, demonstrating:
- Goroutines and channels
- Context-based cancellation
- Synchronization primitives
- Real-world concurrency patterns
- Production-ready error handling

## Author

Educational project demonstrating advanced Go concurrency patterns and best practices.

## Recent Improvements

This codebase has undergone a thorough correctness and production-readiness audit. Key fixes:

**Critical bugs (P0) — all fixed with regression tests:**
- `WorkerPool.SetWorker` captured the user-supplied consumer but discarded it; the new `SetProcessor(fn)` is the recommended path.
- `RateLimitedPattern` captured its consumer but never invoked it; consumers now actually receive data.
- `FanOutFanIn` would deadlock if no goroutine drained `OutputChan()`; call `DiscardOutput()` to avoid it.
- `Pipeline.Run()` would deadlock when `itemCount > bufferSize`; it now drains the final stage internally. `RunWithOutput()` is the caller-drains variant.
- WebSocket implementation rewrote using `http.Hijacker` and full RFC 6455 framing.
- `http.DefaultServeMux` (panic on duplicate registration) replaced with a per-server mux.
- `/api/start` now validates input (rejects negative item counts, non-positive rates, unknown patterns, etc.).
- Static path resolved to absolute via `runtime.Caller` so the binary works from any CWD.
- All counters migrated to `atomic.Int64`; all errors recorded under a mutex with a 1024-entry cap.
- Producer/consumer panics no longer kill the pattern.
- Pattern lifecycle is single-shot (`ErrAlreadyRun`) to prevent double-close panics.
- `drainProducer` uses an explicit timer (no `time.After` leak).

**Production hardening:**
- SIGINT/SIGTERM handler in `cmd/webui` with a 10s graceful-shutdown deadline.
- Structured logging via `log/slog` (`internal/logging`).
- All REST errors returned as JSON `{"error": "..."}` so the dashboard can surface them.
- WebSocket exponential backoff (1s, 2s, 4s, … capped at 30s) on reconnect.
- Fast-completing patterns no longer show 0/0 in the final metrics (synchronous final snapshot per pattern).
