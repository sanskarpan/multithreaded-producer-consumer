// Package server provides the HTTP/WebSocket server for the real-time
// pattern visualization dashboard.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/sanskarpan/producer-consumer/consumer"
	"github.com/sanskarpan/producer-consumer/internal/logging"
	"github.com/sanskarpan/producer-consumer/metrics"
	"github.com/sanskarpan/producer-consumer/patterns"
	"github.com/sanskarpan/producer-consumer/producer"
)

// Server represents the web server hosting the dashboard and pattern execution.
type Server struct {
	collector  *metrics.Collector
	mux        *http.ServeMux
	httpServer *http.Server
	staticDir  string

	mu            sync.RWMutex
	activeCancel  context.CancelFunc
	activePattern string
	patternStatus string
	patternDone   chan struct{}
	patternWg     sync.WaitGroup // tracks running pattern goroutines
}

// Option configures a Server at construction time.
type Option func(*Server)

// WithStaticDir overrides the directory that serves the dashboard's static
// files. Useful for tests and non-standard deploys.
func WithStaticDir(dir string) Option {
	return func(s *Server) { s.staticDir = dir }
}

// NewServer creates a new web server.
func NewServer(opts ...Option) *Server {
	s := &Server{
		collector:     metrics.NewCollector(),
		mux:           http.NewServeMux(),
		patternStatus: "idle",
		staticDir:     resolveDefaultStaticDir(),
	}
	for _, opt := range opts {
		opt(s)
	}
	s.registerRoutes()
	return s
}

// resolveDefaultStaticDir looks for ./web/static relative to the current
// working directory and then relative to the executable. This makes the
// server work both from `go run` (CWD == project root) and from a built
// binary placed elsewhere.
func resolveDefaultStaticDir() string {
	candidates := []string{"./web/static"}
	if exe, err := os.Executable(); err == nil {
		dir := filepath.Dir(exe)
		candidates = append(candidates, filepath.Join(dir, "web", "static"))
		candidates = append(candidates, filepath.Join(dir, "..", "web", "static"))
	}
	if _, file, _, ok := runtime.Caller(0); ok {
		// web/server/server.go is two levels deep from the project root.
		root := filepath.Join(filepath.Dir(file), "..", "..")
		candidates = append(candidates, filepath.Join(root, "web", "static"))
	}
	for _, c := range candidates {
		if abs, err := filepath.Abs(c); err == nil {
			if info, err := os.Stat(abs); err == nil && info.IsDir() {
				return abs
			}
		}
	}
	return "./web/static"
}

func (s *Server) registerRoutes() {
	s.mux.HandleFunc("/api/patterns", s.handlePatterns)
	s.mux.HandleFunc("/api/start", s.handleStart)
	s.mux.HandleFunc("/api/stop", s.handleStop)
	s.mux.HandleFunc("/api/metrics", s.handleMetrics)
	s.mux.HandleFunc("/api/health", s.handleHealth)
	s.mux.HandleFunc("/api/ws", s.handleWebSocket)
	s.mux.Handle("/", http.FileServer(http.Dir(s.staticDir)))
}

// securityHeaders wraps an http.Handler to add production-grade security
// headers. This is a lightweight middleware that doesn't affect performance.
func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Referrer-Policy", "strict-origin-when-cross-origin")
		next.ServeHTTP(w, r)
	})
}

// Handler returns the mux wrapped with security headers for testability.
func (s *Server) Handler() http.Handler { return securityHeaders(s.mux) }

// Start starts the HTTP server. It blocks until the server stops.
func (s *Server) Start(addr string) error {
	s.httpServer = &http.Server{
		Addr:              addr,
		Handler:           s.mux,
		ReadHeaderTimeout: 10 * time.Second,
		// We deliberately do NOT set WriteTimeout because the WebSocket
		// handler hijacks the connection and that timeout would be propagated.
	}
	logging.L().Info("server starting", "addr", addr, "static_dir", s.staticDir)
	err := s.httpServer.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Shutdown stops any running pattern and gracefully shuts the HTTP server down
// using ctx as a deadline.
func (s *Server) Shutdown(ctx context.Context) error {
	s.stopPattern()
	if s.httpServer == nil {
		return nil
	}
	return s.httpServer.Shutdown(ctx)
}

// ----------------------------------------------------------------------------
// REST handlers
// ----------------------------------------------------------------------------

// handlePatterns lists supported patterns and the current server status.
func (s *Server) handlePatterns(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"patterns": []string{"basic", "buffered", "worker_pool", "fan_out_fan_in", "pipeline", "rate_limited"},
		"status":   s.getStatus(),
	})
}

// handleHealth is a lightweight liveness/readiness endpoint suitable for k8s.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"status": "ok",
		"time":   time.Now().UTC(),
	})
}

// PatternConfig is the JSON body accepted by /api/start.
type PatternConfig struct {
	Pattern      string `json:"pattern"`
	ItemCount    int    `json:"item_count"`
	BufferSize   int    `json:"buffer_size"`
	WorkerCount  int    `json:"worker_count"`
	ProducerRate int    `json:"producer_rate"`
	ConsumerRate int    `json:"consumer_rate"`
}

// validate normalises and sanity-checks a PatternConfig. Returns an error with
// a human-readable message suitable for direct API responses.
func (c *PatternConfig) validate() error {
	allowed := map[string]bool{
		"basic": true, "buffered": true, "worker_pool": true,
		"fan_out_fan_in": true, "pipeline": true, "rate_limited": true,
	}
	if !allowed[c.Pattern] {
		return fmt.Errorf("unknown pattern %q", c.Pattern)
	}
	if c.ItemCount < 0 {
		return fmt.Errorf("item_count must be >= 0 (0 selects the default)")
	}
	if c.ItemCount == 0 {
		c.ItemCount = 100
	}
	if c.ItemCount > 1_000_000 {
		return fmt.Errorf("item_count too large (max 1,000,000)")
	}
	if c.BufferSize < 0 {
		return fmt.Errorf("buffer_size must be >= 0")
	}
	if c.BufferSize == 0 {
		c.BufferSize = 100
	}
	if c.BufferSize > 100_000 {
		return fmt.Errorf("buffer_size too large (max 100,000)")
	}
	if c.WorkerCount < 0 {
		return fmt.Errorf("worker_count must be >= 0")
	}
	if c.WorkerCount == 0 {
		c.WorkerCount = 4
	}
	if c.WorkerCount > 1024 {
		return fmt.Errorf("worker_count too large (max 1024)")
	}
	if c.ProducerRate < 0 || c.ConsumerRate < 0 {
		return fmt.Errorf("rates must be >= 0")
	}
	if c.ProducerRate == 0 {
		c.ProducerRate = 50
	}
	if c.ConsumerRate == 0 {
		c.ConsumerRate = 50
	}
	if c.ProducerRate > 1_000_000 || c.ConsumerRate > 1_000_000 {
		return fmt.Errorf("rates too large (max 1,000,000)")
	}
	return nil
}

// handleStart launches a pattern with the supplied configuration. Any
// previously running pattern is stopped first.
func (s *Server) handleStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var config PatternConfig
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&config); err != nil {
		writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON: %v", err))
		return
	}
	if err := config.validate(); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	s.stopPattern()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	s.mu.Lock()
	s.activeCancel = cancel
	s.activePattern = config.Pattern
	s.patternStatus = "running"
	s.patternDone = done
	s.patternWg.Add(1)
	s.mu.Unlock()

	go func() {
		defer s.patternWg.Done()
		s.runPattern(ctx, &config, done)
	}()

	writeJSON(w, http.StatusOK, map[string]string{
		"status":  "started",
		"pattern": config.Pattern,
	})
}

// handleStop stops the currently running pattern, if any.
func (s *Server) handleStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	s.stopPattern()
	writeJSON(w, http.StatusOK, map[string]string{"status": "stopped"})
}

// handleMetrics returns the latest metrics snapshot for every pattern.
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, s.collector.GetAllMetrics())
}

// handleWebSocket upgrades the connection and streams metric snapshots until
// the client disconnects or the server shuts down.
func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgradeConnection(w, r)
	if err != nil {
		logging.L().Warn("websocket upgrade failed", "err", err)
		return
	}
	defer conn.Close()

	// Send an initial snapshot of all current metrics so newly-connected
	// clients don't have to wait for the next update tick.
	for _, m := range s.collector.GetAllMetrics() {
		if data, err := json.Marshal(m); err == nil {
			_ = conn.WriteMessage(opText, data)
		}
	}

	metricsChan := s.collector.Subscribe()
	defer s.collector.Unsubscribe(metricsChan)

	for {
		select {
		case <-conn.Done():
			return
		case m, ok := <-metricsChan:
			if !ok {
				return
			}
			data, err := json.Marshal(m)
			if err != nil {
				logging.L().Warn("websocket marshal failed", "err", err)
				continue
			}
			if err := conn.WriteMessage(opText, data); err != nil {
				logging.L().Debug("websocket write failed", "err", err)
				return
			}
		}
	}
}

// ----------------------------------------------------------------------------
// Pattern execution
// ----------------------------------------------------------------------------

// runPattern executes the configured pattern and emits metrics throughout.
func (s *Server) runPattern(ctx context.Context, config *PatternConfig, done chan struct{}) {
	defer close(done)
	// panicked is captured into the closure so the panic branch can pick the
	// correct terminal status instead of being overwritten by CompleteMetric.
	panicked := false
	defer func() {
		if r := recover(); r != nil {
			logging.L().Error("pattern panicked", "pattern", config.Pattern, "panic", r)
			s.collector.ErrorMetric(config.Pattern, 1)
			panicked = true
		}
		s.mu.Lock()
		// Only flip status if it wasn't already changed to "stopped" by an
		// external request via stopPattern.
		if s.patternStatus == "running" {
			if panicked {
				s.patternStatus = "error"
			} else {
				s.patternStatus = "completed"
			}
		}
		final := s.patternStatus
		s.mu.Unlock()

		// Decide which terminal status to report to the metrics collector.
		// We re-check status under lock to avoid races with stopPattern.
		switch final {
		case "stopped":
			s.collector.StopMetric(config.Pattern)
		case "error":
			// Status was already set to "error" by ErrorMetric above.
		default:
			s.collector.CompleteMetric(config.Pattern)
		}
		// Evict old completed metrics to prevent unbounded memory growth.
		s.collector.EvictCompleted(5 * time.Minute)
	}()

	s.collector.InitMetric(config.Pattern, config.BufferSize, config.WorkerCount)
	logging.L().Info("pattern starting", "pattern", config.Pattern,
		"items", config.ItemCount, "buffer", config.BufferSize, "workers", config.WorkerCount)

	switch config.Pattern {
	case "basic":
		s.runBasicPattern(ctx, config, done)
	case "buffered":
		s.runBufferedPattern(ctx, config, done)
	case "worker_pool":
		s.runWorkerPoolPattern(ctx, config, done)
	case "fan_out_fan_in":
		s.runFanOutFanInPattern(ctx, config, done)
	case "pipeline":
		s.runPipelinePattern(ctx, config, done)
	case "rate_limited":
		s.runRateLimitedPattern(ctx, config, done)
	default:
		logging.L().Warn("unknown pattern", "pattern", config.Pattern)
	}
}

func (s *Server) runBasicPattern(ctx context.Context, config *PatternConfig, done <-chan struct{}) {
	pattern := patterns.NewBasicPattern()
	prod := producer.NewIntProducer("p1", 0, config.ItemCount, 0)
	cons := consumer.NewAggregateConsumer("c1", 0)
	pattern.AddProducer(prod)
	pattern.AddConsumer(cons)

	go s.trackBasicMetrics(ctx, done, config.Pattern, prod, cons)

	err := pattern.Run(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		logging.L().Warn("basic pattern error", "err", err)
		s.collector.ErrorMetric(config.Pattern, 1)
	}
	// Record the final state synchronously so the dashboard sees the actual
	// counts even when the pattern completed before the first metric tick.
	s.recordFinalBasic(config.Pattern, prod, cons)
}

func (s *Server) recordFinalBasic(name string, prod *producer.IntProducer, cons *consumer.AggregateConsumer) {
	m := s.collector.GetMetrics(name)
	if m == nil {
		return
	}
	m.ItemsProduced = prod.ProducedCount()
	if cons != nil {
		m.ItemsConsumed = cons.ConsumedCount()
	}
	s.collector.RecordMetric(m)
}

func (s *Server) runBufferedPattern(ctx context.Context, config *PatternConfig, done <-chan struct{}) {
	pattern := patterns.NewBufferedPattern(config.BufferSize)
	prod := producer.NewIntProducer("p1", 0, config.ItemCount, 0)
	cons := consumer.NewAggregateConsumer("c1", 0)
	pattern.AddProducer(prod)
	pattern.AddConsumer(cons)

	go s.trackBufferedMetrics(ctx, done, config.Pattern, pattern, prod, cons)

	err := pattern.Run(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		logging.L().Warn("buffered pattern error", "err", err)
		s.collector.ErrorMetric(config.Pattern, 1)
	}
	s.recordFinalBuffered(config.Pattern, pattern, prod, cons)
}

func (s *Server) recordFinalBuffered(name string, pattern *patterns.BufferedPattern, prod *producer.IntProducer, cons *consumer.AggregateConsumer) {
	m := s.collector.GetMetrics(name)
	if m == nil {
		return
	}
	m.ItemsProduced = prod.ProducedCount()
	m.ItemsConsumed = cons.ConsumedCount()
	m.QueueDepth = pattern.ChannelLen()
	if m.BufferSize > 0 {
		m.BufferUtilization = float64(m.QueueDepth) / float64(m.BufferSize) * 100
	}
	s.collector.RecordMetric(m)
}

func (s *Server) runWorkerPoolPattern(ctx context.Context, config *PatternConfig, done <-chan struct{}) {
	pool := patterns.NewWorkerPool(config.WorkerCount, config.BufferSize)
	prod := producer.NewIntProducer("p1", 0, config.ItemCount, 0)
	pool.AddProducer(prod)

	go s.trackWorkerPoolMetrics(ctx, done, config.Pattern, pool, prod)

	err := pool.Run(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		logging.L().Warn("worker pool error", "err", err)
		s.collector.ErrorMetric(config.Pattern, 1)
	}
	s.recordFinalWorkerPool(config.Pattern, pool, prod)
}

func (s *Server) recordFinalWorkerPool(name string, pool *patterns.WorkerPool, prod *producer.IntProducer) {
	m := s.collector.GetMetrics(name)
	if m == nil {
		return
	}
	m.ItemsProduced = prod.ProducedCount()
	total := 0
	for _, c := range pool.WorkerStats() {
		total += c
	}
	m.ItemsProcessed = total
	m.ItemsConsumed = total
	m.QueueDepth = pool.QueueDepth()
	if m.BufferSize > 0 {
		m.BufferUtilization = float64(m.QueueDepth) / float64(m.BufferSize) * 100
	}
	s.collector.RecordMetric(m)
}

func (s *Server) runFanOutFanInPattern(ctx context.Context, config *PatternConfig, done <-chan struct{}) {
	processor := func(data interface{}) error { return nil }
	pattern := patterns.NewFanOutFanIn(config.WorkerCount, config.BufferSize, processor)
	pattern.DiscardOutput() // The dashboard does not consume the output channel.

	prod := producer.NewIntProducer("p1", 0, config.ItemCount, 0)
	pattern.AddProducer(prod)

	go s.trackFanOutFanInMetrics(ctx, done, config.Pattern, pattern, prod)

	err := pattern.Run(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		logging.L().Warn("fan-out/fan-in error", "err", err)
		s.collector.ErrorMetric(config.Pattern, 1)
	}
	s.recordFinalFanOutFanIn(config.Pattern, pattern, prod)
}

func (s *Server) recordFinalFanOutFanIn(name string, pattern *patterns.FanOutFanIn, prod *producer.IntProducer) {
	m := s.collector.GetMetrics(name)
	if m == nil {
		return
	}
	m.ItemsProduced = prod.ProducedCount()
	m.ItemsProcessed = pattern.Processed()
	m.ItemsConsumed = pattern.Processed()
	m.QueueDepth = pattern.QueueDepth()
	if m.BufferSize > 0 {
		m.BufferUtilization = float64(m.QueueDepth) / float64(m.BufferSize) * 100
	}
	s.collector.RecordMetric(m)
}

func (s *Server) runPipelinePattern(ctx context.Context, config *PatternConfig, done <-chan struct{}) {
	pipeline := patterns.NewPipeline(config.BufferSize)
	prod := producer.NewIntProducer("p1", 0, config.ItemCount, 0)
	pipeline.AddProducer(prod)
	pipeline.AddStage("stage1", func(data interface{}) error { return nil }, 2)
	pipeline.AddStage("stage2", func(data interface{}) error { return nil }, 2)

	go s.trackPipelineMetrics(ctx, done, config.Pattern, prod)

	err := pipeline.Run(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		logging.L().Warn("pipeline error", "err", err)
		s.collector.ErrorMetric(config.Pattern, 1)
	}
	s.recordFinalPipeline(config.Pattern, prod)
}

func (s *Server) recordFinalPipeline(name string, prod *producer.IntProducer) {
	m := s.collector.GetMetrics(name)
	if m == nil {
		return
	}
	m.ItemsProduced = prod.ProducedCount()
	s.collector.RecordMetric(m)
}

func (s *Server) runRateLimitedPattern(ctx context.Context, config *PatternConfig, done <-chan struct{}) {
	pattern := patterns.NewRateLimitedPattern(config.BufferSize, config.ProducerRate, config.ConsumerRate)
	prod := producer.NewIntProducer("p1", 0, config.ItemCount, 0)
	cons := consumer.NewAggregateConsumer("c1", 0)
	pattern.AddProducer(prod)
	pattern.AddConsumer(cons)

	go s.trackRateLimitedMetrics(ctx, done, config.Pattern, pattern)

	err := pattern.Run(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		logging.L().Warn("rate-limited error", "err", err)
		s.collector.ErrorMetric(config.Pattern, 1)
	}
	s.recordFinalRateLimited(config.Pattern, pattern)
}

func (s *Server) recordFinalRateLimited(name string, pattern *patterns.RateLimitedPattern) {
	m := s.collector.GetMetrics(name)
	if m == nil {
		return
	}
	produced, consumed := pattern.Stats()
	m.ItemsProduced = produced
	m.ItemsConsumed = consumed
	m.QueueDepth = pattern.QueueDepth()
	if m.BufferSize > 0 {
		m.BufferUtilization = float64(m.QueueDepth) / float64(m.BufferSize) * 100
	}
	s.collector.RecordMetric(m)
}

// ----------------------------------------------------------------------------
// Metric tracking helpers
// ----------------------------------------------------------------------------

const metricTick = 100 * time.Millisecond

// trackUntil is the common select body: exit when ctx is cancelled OR when the
// pattern's done channel is closed (which means the pattern has finished, the
// defer has fired, and the metrics entry has its terminal status set).
//
// We invoke fn() once immediately so fast-completing patterns (where Run
// returns in well under one metricTick) still get their final state recorded.
func trackUntil(ctx context.Context, done <-chan struct{}, fn func()) {
	ticker := time.NewTicker(metricTick)
	defer ticker.Stop()
	for {
		fn()
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		case <-ticker.C:
		}
	}
}

func (s *Server) trackBasicMetrics(ctx context.Context, done <-chan struct{}, name string, prod *producer.IntProducer, cons *consumer.AggregateConsumer) {
	trackUntil(ctx, done, func() {
		m := s.collector.GetMetrics(name)
		if m == nil {
			return
		}
		m.ItemsProduced = prod.ProducedCount()
		if cons != nil {
			m.ItemsConsumed = cons.ConsumedCount()
		}
		s.collector.RecordMetric(m)
	})
}

func (s *Server) trackBufferedMetrics(ctx context.Context, done <-chan struct{}, name string, pattern *patterns.BufferedPattern, prod *producer.IntProducer, cons *consumer.AggregateConsumer) {
	trackUntil(ctx, done, func() {
		m := s.collector.GetMetrics(name)
		if m == nil {
			return
		}
		m.ItemsProduced = prod.ProducedCount()
		m.ItemsConsumed = cons.ConsumedCount()
		m.QueueDepth = pattern.ChannelLen()
		if m.BufferSize > 0 {
			m.BufferUtilization = float64(m.QueueDepth) / float64(m.BufferSize) * 100
		}
		s.collector.RecordMetric(m)
	})
}

func (s *Server) trackWorkerPoolMetrics(ctx context.Context, done <-chan struct{}, name string, pool *patterns.WorkerPool, prod *producer.IntProducer) {
	trackUntil(ctx, done, func() {
		m := s.collector.GetMetrics(name)
		if m == nil {
			return
		}
		m.ItemsProduced = prod.ProducedCount()
		stats := pool.WorkerStats()
		total := 0
		for _, c := range stats {
			total += c
		}
		m.ItemsProcessed = total
		m.ItemsConsumed = total
		m.QueueDepth = pool.QueueDepth()
		if m.BufferSize > 0 {
			m.BufferUtilization = float64(m.QueueDepth) / float64(m.BufferSize) * 100
		}
		s.collector.RecordMetric(m)
	})
}

func (s *Server) trackFanOutFanInMetrics(ctx context.Context, done <-chan struct{}, name string, pattern *patterns.FanOutFanIn, prod *producer.IntProducer) {
	trackUntil(ctx, done, func() {
		m := s.collector.GetMetrics(name)
		if m == nil {
			return
		}
		m.ItemsProduced = prod.ProducedCount()
		m.ItemsProcessed = pattern.Processed()
		m.ItemsConsumed = pattern.Processed()
		m.QueueDepth = pattern.QueueDepth()
		if m.BufferSize > 0 {
			m.BufferUtilization = float64(m.QueueDepth) / float64(m.BufferSize) * 100
		}
		s.collector.RecordMetric(m)
	})
}

func (s *Server) trackPipelineMetrics(ctx context.Context, done <-chan struct{}, name string, prod *producer.IntProducer) {
	trackUntil(ctx, done, func() {
		m := s.collector.GetMetrics(name)
		if m == nil {
			return
		}
		m.ItemsProduced = prod.ProducedCount()
		s.collector.RecordMetric(m)
	})
}

func (s *Server) trackRateLimitedMetrics(ctx context.Context, done <-chan struct{}, name string, pattern *patterns.RateLimitedPattern) {
	trackUntil(ctx, done, func() {
		m := s.collector.GetMetrics(name)
		if m == nil {
			return
		}
		produced, consumed := pattern.Stats()
		m.ItemsProduced = produced
		m.ItemsConsumed = consumed
		m.QueueDepth = pattern.QueueDepth()
		if m.BufferSize > 0 {
			m.BufferUtilization = float64(m.QueueDepth) / float64(m.BufferSize) * 100
		}
		s.collector.RecordMetric(m)
	})
}

// ----------------------------------------------------------------------------
// Internal helpers
// ----------------------------------------------------------------------------

// stopPattern cancels and waits for the active pattern, if any.
func (s *Server) stopPattern() {
	s.mu.Lock()
	cancel := s.activeCancel
	done := s.patternDone
	if cancel == nil {
		s.mu.Unlock()
		return
	}
	s.activeCancel = nil
	s.patternStatus = "stopped"
	s.mu.Unlock()

	cancel()
	if done != nil {
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			logging.L().Warn("pattern did not stop within 10s of cancellation")
		}
	}
	// Wait for the goroutine to actually exit so we don't leak.
	doneCh := make(chan struct{})
	go func() {
		s.patternWg.Wait()
		close(doneCh)
	}()
	select {
	case <-doneCh:
	case <-time.After(2 * time.Second):
		logging.L().Warn("pattern goroutine did not exit within 2s of done signal")
	}
}

// getStatus returns current server status.
func (s *Server) getStatus() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.patternStatus
}

// writeJSON marshals v and writes it with the given status code.
func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeJSONError writes a JSON error response with a consistent shape.
func writeJSONError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
