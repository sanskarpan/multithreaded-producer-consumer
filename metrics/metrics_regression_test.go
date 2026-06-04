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
