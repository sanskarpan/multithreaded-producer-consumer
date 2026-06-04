package patterns

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanskarpan/producer-consumer/producer"
)

// TestPipeline_ItemCountExceedsBufferSize is a regression test for the bug
// where Pipeline.Run() would deadlock if the number of items produced was
// strictly greater than the buffer size, because the final stage's output
// channel was never drained. Run() must now drain internally.
func TestPipeline_ItemCountExceedsBufferSize(t *testing.T) {
	pipeline := NewPipeline(5) // small buffer
	pipeline.AddProducer(producer.NewIntProducer("p", 0, 50, 0))

	var processed int64
	pipeline.AddStage("noop", func(data interface{}) error {
		atomic.AddInt64(&processed, 1)
		return nil
	}, 2)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- pipeline.Run(ctx) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("pipeline run: %v", err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("pipeline did not finish within 4s; likely a deadlock on the final channel")
	}
	if got := atomic.LoadInt64(&processed); got != 50 {
		t.Fatalf("expected 50 items processed, got %d", got)
	}
}
