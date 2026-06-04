package patterns

import (
	"context"
	"testing"
	"time"

	"github.com/sanskarpan/producer-consumer/producer"
)

// TestFanOutFanIn_DiscardOutput_NoDeadlock is a regression test for the bug
// where workers would block on send to outputChan if no one read from
// OutputChan. With DiscardOutput() enabled, the pattern must complete
// regardless of consumer count.
func TestFanOutFanIn_DiscardOutput_NoDeadlock(t *testing.T) {
	pattern := NewFanOutFanIn(4, 16, func(data interface{}) error { return nil })
	pattern.DiscardOutput()
	pattern.AddProducer(producer.NewIntProducer("p", 0, 100, 0))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- pattern.Run(ctx) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("pattern run: %v", err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("pattern did not finish within 4s; likely a deadlock on outputChan")
	}
	if got := pattern.Processed(); got != 100 {
		t.Fatalf("expected 100 processed, got %d", got)
	}
}
