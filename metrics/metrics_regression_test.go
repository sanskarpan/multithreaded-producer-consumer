package metrics

import (
	"sync"
	"testing"
	"time"
)

// TestCollector_ConcurrentRecordAndSubscribe stresses the metrics collector
// with concurrent producers, subscribers, and lifecycle updates. Run with
// -race: must not detect any data race.
func TestCollector_ConcurrentRecordAndSubscribe(t *testing.T) {
	c := NewCollector()
	c.InitMetric("p", 16, 4)

	const (
		writers        = 8
		subscribers    = 8
		updatesPerGor  = 200
		patterns       = 4
	)

	var wg sync.WaitGroup
	wg.Add(writers + subscribers)

	// Writers
	for i := 0; i < writers; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < updatesPerGor; j++ {
				name := "p"
				if (id+j)%patterns == 0 {
					name = "alt"
					c.InitMetric(name, 32, 2)
				}
				m := c.GetMetrics(name)
				if m == nil {
					continue
				}
				m.ItemsProduced = j
				m.ItemsConsumed = j
				c.RecordMetric(m)
			}
		}(i)
	}

	// Subscribers
	for i := 0; i < subscribers; i++ {
		go func() {
			defer wg.Done()
			ch := c.Subscribe()
			defer c.Unsubscribe(ch)
			timeout := time.After(2 * time.Second)
			for {
				select {
				case _, ok := <-ch:
					if !ok {
						return
					}
				case <-timeout:
					return
				}
			}
		}()
	}

	wg.Wait()
}

// TestCollector_UnsubscribeUnknown ensures Unsubscribe on an unknown channel
// is a safe no-op.
func TestCollector_UnsubscribeUnknown(t *testing.T) {
	c := NewCollector()
	ch := make(chan *Metrics, 1)
	// Should not panic.
	c.Unsubscribe(ch)
}

// TestCollector_RecordNil guards against RecordMetric(nil).
func TestCollector_RecordNil(t *testing.T) {
	c := NewCollector()
	// Should not panic.
	c.RecordMetric(nil)
	c.RecordMetric(&Metrics{})
}

// TestCollector_EvictCompleted verifies that EvictCompleted removes old
// completed/stopped metrics and leaves active ones intact.
func TestCollector_EvictCompleted(t *testing.T) {
	c := NewCollector()

	// Create metrics at different times.
	c.InitMetric("running", 16, 2)
	c.InitMetric("completed-old", 16, 2)
	c.InitMetric("stopped-old", 16, 2)
	c.InitMetric("completed-recent", 16, 2)

	// Simulate completion with old timestamps.
	old := c.GetMetrics("completed-old")
	old.StartTime = time.Now().Add(-10 * time.Minute)
	old.Status = "completed"
	c.RecordMetric(old)

	stopped := c.GetMetrics("stopped-old")
	stopped.StartTime = time.Now().Add(-10 * time.Minute)
	stopped.Status = "stopped"
	c.RecordMetric(stopped)

	// completed-recent is fresh (not old enough to evict).
	recent := c.GetMetrics("completed-recent")
	recent.Status = "completed"
	c.RecordMetric(recent)

	// running is active.
	running := c.GetMetrics("running")
	running.Status = "running"
	c.RecordMetric(running)

	c.EvictCompleted(5 * time.Minute)

	// Old completed/stopped should be evicted.
	if c.GetMetrics("completed-old") != nil {
		t.Fatal("expected completed-old to be evicted")
	}
	if c.GetMetrics("stopped-old") != nil {
		t.Fatal("expected stopped-old to be evicted")
	}

	// Recent completed should still exist.
	if c.GetMetrics("completed-recent") == nil {
		t.Fatal("expected completed-recent to survive eviction")
	}

	// Running should still exist.
	if c.GetMetrics("running") == nil {
		t.Fatal("expected running to survive eviction")
	}
}

// TestCollector_EvictCompletedNoOp verifies eviction with no metrics is safe.
func TestCollector_EvictCompletedNoOp(t *testing.T) {
	c := NewCollector()
	// Should not panic.
	c.EvictCompleted(time.Hour)
}

// TestCollector_EvictCompletedOnlyOldActive verifies active metrics with old
// timestamps are NOT evicted (only completed/stopped are).
func TestCollector_EvictCompletedOnlyOldActive(t *testing.T) {
	c := NewCollector()
	c.InitMetric("old-active", 16, 2)

	m := c.GetMetrics("old-active")
	m.StartTime = time.Now().Add(-10 * time.Hour)
	m.Status = "running"
	c.RecordMetric(m)

	c.EvictCompleted(time.Minute)

	if c.GetMetrics("old-active") == nil {
		t.Fatal("active metrics should not be evicted regardless of age")
	}
}
