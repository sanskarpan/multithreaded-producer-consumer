// Package metrics provides real-time metrics collection and reporting with a
// thread-safe publish/subscribe model.
//
// The Collector stores a snapshot per pattern under a single RWMutex. All
// reads and broadcasts work on defensive copies so subscribers never observe
// partially-updated state.
package metrics

import (
	"sync"
	"time"
)

// Metrics represents the real-time metrics of a pattern execution. JSON tags
// mirror the legacy field names so the web frontend continues to work.
type Metrics struct {
	PatternName       string    `json:"pattern_name"`
	ItemsProduced     int       `json:"items_produced"`
	ItemsConsumed     int       `json:"items_consumed"`
	ItemsProcessed    int       `json:"items_processed"`
	QueueDepth        int       `json:"queue_depth"`
	Throughput        float64   `json:"throughput"`         // items per second
	Latency           float64   `json:"latency"`            // milliseconds
	WorkerCount       int       `json:"worker_count"`
	ActiveWorkers     int       `json:"active_workers"`
	BufferSize        int       `json:"buffer_size"`
	BufferUtilization float64   `json:"buffer_utilization"` // percentage 0..100
	ErrorCount        int       `json:"error_count"`
	StartTime         time.Time `json:"start_time"`
	Duration          float64   `json:"duration"` // seconds
	Status            string    `json:"status"`   // running, completed, error
}

// clone returns a deep copy of m. time.Time is a value type so a shallow copy
// of the struct is sufficient.
func (m *Metrics) clone() *Metrics {
	if m == nil {
		return nil
	}
	c := *m
	return &c
}

// Collector collects and aggregates metrics with safe pub/sub semantics.
type Collector struct {
	mu      sync.RWMutex
	metrics map[string]*Metrics

	subMu       sync.RWMutex
	subscribers map[chan *Metrics]struct{}
}

// NewCollector creates a new metrics collector.
func NewCollector() *Collector {
	return &Collector{
		metrics:     make(map[string]*Metrics),
		subscribers: make(map[chan *Metrics]struct{}),
	}
}

// RecordMetric stores a snapshot of m for its pattern and notifies subscribers.
// The caller may reuse m after this call returns; only a copy is retained.
func (c *Collector) RecordMetric(m *Metrics) {
	if m == nil || m.PatternName == "" {
		return
	}
	snap := m.clone()
	c.mu.Lock()
	c.metrics[snap.PatternName] = snap
	c.mu.Unlock()
	c.broadcast(snap)
}

// UpdateProduced increments items produced and refreshes derived fields.
func (c *Collector) UpdateProduced(patternName string, count int) {
	c.mu.Lock()
	if m, ok := c.metrics[patternName]; ok {
		m.ItemsProduced += count
		c.recomputeThroughput(m)
	}
	c.mu.Unlock()
}

// UpdateConsumed increments items consumed and refreshes derived fields.
func (c *Collector) UpdateConsumed(patternName string, count int) {
	c.mu.Lock()
	if m, ok := c.metrics[patternName]; ok {
		m.ItemsConsumed += count
		c.recomputeThroughput(m)
	}
	c.mu.Unlock()
}

// UpdateQueueDepth updates the current queue depth (and utilization).
func (c *Collector) UpdateQueueDepth(patternName string, depth int) {
	c.mu.Lock()
	if m, ok := c.metrics[patternName]; ok {
		m.QueueDepth = depth
		if m.BufferSize > 0 {
			m.BufferUtilization = float64(depth) / float64(m.BufferSize) * 100
		}
	}
	c.mu.Unlock()
}

// recomputeThroughput updates the per-second throughput and elapsed duration.
// Caller must hold c.mu.
func (c *Collector) recomputeThroughput(m *Metrics) {
	elapsed := time.Since(m.StartTime).Seconds()
	if elapsed > 0 {
		m.Duration = elapsed
		m.Throughput = float64(m.ItemsProduced) / elapsed
	}
}

// GetMetrics returns a copy of the current metrics for a pattern, or nil.
func (c *Collector) GetMetrics(patternName string) *Metrics {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.metrics[patternName].clone()
}

// GetAllMetrics returns copies of all current metrics keyed by pattern name.
func (c *Collector) GetAllMetrics() map[string]*Metrics {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[string]*Metrics, len(c.metrics))
	for k, v := range c.metrics {
		out[k] = v.clone()
	}
	return out
}

// Subscribe registers a new subscriber and returns a buffered channel that
// will receive metric snapshots. Each call returns a distinct channel.
func (c *Collector) Subscribe() chan *Metrics {
	ch := make(chan *Metrics, 256)
	c.subMu.Lock()
	c.subscribers[ch] = struct{}{}
	c.subMu.Unlock()
	return ch
}

// Unsubscribe removes a subscriber and closes its channel. Calling Unsubscribe
// on a channel that was not produced by Subscribe is a no-op.
func (c *Collector) Unsubscribe(ch chan *Metrics) {
	c.subMu.Lock()
	if _, ok := c.subscribers[ch]; !ok {
		c.subMu.Unlock()
		return
	}
	delete(c.subscribers, ch)
	c.subMu.Unlock()
	close(ch)
}

// broadcast sends a snapshot to every subscriber. A subscriber's channel that
// is currently full has the message dropped (best-effort delivery).
func (c *Collector) broadcast(snap *Metrics) {
	c.subMu.RLock()
	defer c.subMu.RUnlock()
	for ch := range c.subscribers {
		// Make a private copy per subscriber so subscribers can't mutate
		// each other's view.
		s := *snap
		select {
		case ch <- &s:
		default:
			// Slow subscriber; drop and move on.
		}
	}
}

// Clear removes all metrics.
func (c *Collector) Clear() {
	c.mu.Lock()
	c.metrics = make(map[string]*Metrics)
	c.mu.Unlock()
}

// InitMetric initializes a new metric entry for a pattern.
func (c *Collector) InitMetric(patternName string, bufferSize, workerCount int) {
	c.mu.Lock()
	c.metrics[patternName] = &Metrics{
		PatternName:   patternName,
		StartTime:     time.Now(),
		Status:        "running",
		BufferSize:    bufferSize,
		WorkerCount:   workerCount,
		ActiveWorkers: workerCount,
	}
	snap := c.metrics[patternName].clone()
	c.mu.Unlock()
	c.broadcast(snap)
}

// CompleteMetric marks a pattern as completed and recomputes its throughput.
func (c *Collector) CompleteMetric(patternName string) {
	c.mu.Lock()
	if m, ok := c.metrics[patternName]; ok {
		m.Status = "completed"
		c.recomputeThroughput(m)
		snap := m.clone()
		c.mu.Unlock()
		c.broadcast(snap)
		return
	}
	c.mu.Unlock()
}

// ErrorMetric marks a pattern with an error count.
func (c *Collector) ErrorMetric(patternName string, errCount int) {
	c.mu.Lock()
	if m, ok := c.metrics[patternName]; ok {
		m.Status = "error"
		m.ErrorCount += errCount
		snap := m.clone()
		c.mu.Unlock()
		c.broadcast(snap)
		return
	}
	c.mu.Unlock()
}

// StopMetric marks a pattern as stopped by external request.
func (c *Collector) StopMetric(patternName string) {
	c.mu.Lock()
	if m, ok := c.metrics[patternName]; ok {
		m.Status = "stopped"
		c.recomputeThroughput(m)
		snap := m.clone()
		c.mu.Unlock()
		c.broadcast(snap)
		return
	}
	c.mu.Unlock()
}

// PerformanceSnapshot represents a single point-in-time performance reading.
type PerformanceSnapshot struct {
	Timestamp         time.Time `json:"timestamp"`
	PatternName       string    `json:"pattern_name"`
	Throughput        float64   `json:"throughput"`
	Latency           float64   `json:"latency"`
	QueueDepth        int       `json:"queue_depth"`
	BufferUtilization float64   `json:"buffer_utilization"`
}

// TakeSnapshot returns a performance snapshot for the named pattern, or nil
// when there is no entry.
func (c *Collector) TakeSnapshot(patternName string) *PerformanceSnapshot {
	m := c.GetMetrics(patternName)
	if m == nil {
		return nil
	}
	return &PerformanceSnapshot{
		Timestamp:         time.Now(),
		PatternName:       patternName,
		Throughput:        m.Throughput,
		Latency:           m.Latency,
		QueueDepth:        m.QueueDepth,
		BufferUtilization: m.BufferUtilization,
	}
}
