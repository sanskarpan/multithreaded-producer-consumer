package patterns

import (
	"context"
	"testing"
	"time"

	"github.com/sanskarpan/producer-consumer/consumer"
	"github.com/sanskarpan/producer-consumer/producer"
)

// Benchmark Basic Pattern
func BenchmarkBasicPattern(b *testing.B) {
	for i := 0; i < b.N; i++ {
		pattern := NewBasicPattern()
		prod := producer.NewIntProducer("p1", 0, 100, 0)
		cons := consumer.NewAggregateConsumer("c1", 0)
		pattern.AddProducer(prod)
		pattern.AddConsumer(cons)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = pattern.Run(ctx)
		cancel()
	}
}

func BenchmarkBasicPattern_Parallel(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			pattern := NewBasicPattern()
			prod := producer.NewIntProducer("p1", 0, 50, 0)
			cons := consumer.NewAggregateConsumer("c1", 0)
			pattern.AddProducer(prod)
			pattern.AddConsumer(cons)

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			_ = pattern.Run(ctx)
			cancel()
		}
	})
}

// Benchmark Buffered Pattern
func BenchmarkBufferedPattern(b *testing.B) {
	for i := 0; i < b.N; i++ {
		pattern := NewBufferedPattern(100)
		prod := producer.NewIntProducer("p1", 0, 100, 0)
		cons := consumer.NewAggregateConsumer("c1", 0)
		pattern.AddProducer(prod)
		pattern.AddConsumer(cons)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = pattern.Run(ctx)
		cancel()
	}
}

func BenchmarkBufferedPattern_SmallBuffer(b *testing.B) {
	for i := 0; i < b.N; i++ {
		pattern := NewBufferedPattern(10)
		prod := producer.NewIntProducer("p1", 0, 100, 0)
		cons := consumer.NewAggregateConsumer("c1", 0)
		pattern.AddProducer(prod)
		pattern.AddConsumer(cons)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = pattern.Run(ctx)
		cancel()
	}
}

func BenchmarkBufferedPattern_LargeBuffer(b *testing.B) {
	for i := 0; i < b.N; i++ {
		pattern := NewBufferedPattern(1000)
		prod := producer.NewIntProducer("p1", 0, 100, 0)
		cons := consumer.NewAggregateConsumer("c1", 0)
		pattern.AddProducer(prod)
		pattern.AddConsumer(cons)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = pattern.Run(ctx)
		cancel()
	}
}

// Benchmark Worker Pool
func BenchmarkWorkerPool_2Workers(b *testing.B) {
	for i := 0; i < b.N; i++ {
		pool := NewWorkerPool(2, 100)
		prod := producer.NewIntProducer("p1", 0, 100, 0)
		pool.AddProducer(prod)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = pool.Run(ctx)
		cancel()
	}
}

func BenchmarkWorkerPool_4Workers(b *testing.B) {
	for i := 0; i < b.N; i++ {
		pool := NewWorkerPool(4, 100)
		prod := producer.NewIntProducer("p1", 0, 100, 0)
		pool.AddProducer(prod)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = pool.Run(ctx)
		cancel()
	}
}

func BenchmarkWorkerPool_8Workers(b *testing.B) {
	for i := 0; i < b.N; i++ {
		pool := NewWorkerPool(8, 100)
		prod := producer.NewIntProducer("p1", 0, 100, 0)
		pool.AddProducer(prod)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = pool.Run(ctx)
		cancel()
	}
}

// Benchmark Fan-Out/Fan-In
func BenchmarkFanOutFanIn_2Workers(b *testing.B) {
	processor := func(data interface{}) error {
		return nil
	}

	for i := 0; i < b.N; i++ {
		pattern := NewFanOutFanIn(2, 100, processor)
		prod := producer.NewIntProducer("p1", 0, 100, 0)
		pattern.AddProducer(prod)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		go pattern.Run(ctx)
		for range pattern.OutputChan() {
		}
		cancel()
	}
}

func BenchmarkFanOutFanIn_4Workers(b *testing.B) {
	processor := func(data interface{}) error {
		return nil
	}

	for i := 0; i < b.N; i++ {
		pattern := NewFanOutFanIn(4, 100, processor)
		prod := producer.NewIntProducer("p1", 0, 100, 0)
		pattern.AddProducer(prod)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		go pattern.Run(ctx)
		for range pattern.OutputChan() {
		}
		cancel()
	}
}

// Benchmark Pipeline
func BenchmarkPipeline_1Stage(b *testing.B) {
	for i := 0; i < b.N; i++ {
		pipeline := NewPipeline(100)
		prod := producer.NewIntProducer("p1", 0, 100, 0)
		pipeline.AddProducer(prod)
		pipeline.AddStage("stage1", func(data interface{}) error { return nil }, 2)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = pipeline.Run(ctx)
		cancel()
	}
}

func BenchmarkPipeline_3Stages(b *testing.B) {
	for i := 0; i < b.N; i++ {
		pipeline := NewPipeline(100)
		prod := producer.NewIntProducer("p1", 0, 100, 0)
		pipeline.AddProducer(prod)
		pipeline.AddStage("stage1", func(data interface{}) error { return nil }, 2)
		pipeline.AddStage("stage2", func(data interface{}) error { return nil }, 2)
		pipeline.AddStage("stage3", func(data interface{}) error { return nil }, 2)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = pipeline.Run(ctx)
		cancel()
	}
}

// Benchmark Rate Limited
func BenchmarkRateLimited_10PerSec(b *testing.B) {
	for i := 0; i < b.N; i++ {
		pattern := NewRateLimitedPattern(50, 10, 20)
		prod := producer.NewIntProducer("p1", 0, 20, 0)
		cons := consumer.NewAggregateConsumer("c1", 0)
		pattern.AddProducer(prod)
		pattern.AddConsumer(cons)

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_ = pattern.Run(ctx)
		cancel()
	}
}

func BenchmarkRateLimited_50PerSec(b *testing.B) {
	for i := 0; i < b.N; i++ {
		pattern := NewRateLimitedPattern(100, 50, 60)
		prod := producer.NewIntProducer("p1", 0, 50, 0)
		cons := consumer.NewAggregateConsumer("c1", 0)
		pattern.AddProducer(prod)
		pattern.AddConsumer(cons)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = pattern.Run(ctx)
		cancel()
	}
}

// Throughput benchmarks
func BenchmarkThroughput_Basic_1000Items(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		pattern := NewBasicPattern()
		prod := producer.NewIntProducer("p1", 0, 1000, 0)
		cons := consumer.NewAggregateConsumer("c1", 0)
		pattern.AddProducer(prod)
		pattern.AddConsumer(cons)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_ = pattern.Run(ctx)
		cancel()
	}
}

func BenchmarkThroughput_Buffered_1000Items(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		pattern := NewBufferedPattern(500)
		prod := producer.NewIntProducer("p1", 0, 1000, 0)
		cons := consumer.NewAggregateConsumer("c1", 0)
		pattern.AddProducer(prod)
		pattern.AddConsumer(cons)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_ = pattern.Run(ctx)
		cancel()
	}
}

func BenchmarkThroughput_WorkerPool_1000Items(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		pool := NewWorkerPool(4, 500)
		prod := producer.NewIntProducer("p1", 0, 1000, 0)
		pool.AddProducer(prod)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_ = pool.Run(ctx)
		cancel()
	}
}
