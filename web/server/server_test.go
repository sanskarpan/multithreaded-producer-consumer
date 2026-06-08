package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// newTestServer builds a Server with a known static dir.
func newTestServer(t *testing.T) *Server {
	t.Helper()
	return NewServer(WithStaticDir(t.TempDir()))
}

func TestServer_Health(t *testing.T) {
	s := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/api/health", nil)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	var body map[string]interface{}
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["status"] != "ok" {
		t.Fatalf("body status = %v, want ok", body["status"])
	}
}

func TestServer_PatternsListsAll(t *testing.T) {
	s := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/api/patterns", nil)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d", rr.Code)
	}
	var body map[string]interface{}
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	patterns, _ := body["patterns"].([]interface{})
	if len(patterns) != 6 {
		t.Fatalf("got %d patterns, want 6", len(patterns))
	}
}

func TestServer_StartRejectsInvalidPattern(t *testing.T) {
	s := newTestServer(t)
	body := strings.NewReader(`{"pattern":"bogus","item_count":10}`)
	req := httptest.NewRequest(http.MethodPost, "/api/start", body)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
}

func TestServer_StartRejectsBadJSON(t *testing.T) {
	s := newTestServer(t)
	req := httptest.NewRequest(http.MethodPost, "/api/start", bytes.NewReader([]byte("not-json")))
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
}

func TestServer_StartRejectsNegativeItemCount(t *testing.T) {
	s := newTestServer(t)
	body := strings.NewReader(`{"pattern":"basic","item_count":-5}`)
	req := httptest.NewRequest(http.MethodPost, "/api/start", body)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
}

func TestServer_StartRejectsExcessiveItemCount(t *testing.T) {
	s := newTestServer(t)
	body := strings.NewReader(`{"pattern":"basic","item_count":10000000}`)
	req := httptest.NewRequest(http.MethodPost, "/api/start", body)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
}

func TestServer_StartRejectsZeroRate(t *testing.T) {
	s := newTestServer(t)
	// 0 is allowed (defaults applied); explicit negative must be rejected.
	body := strings.NewReader(`{"pattern":"rate_limited","producer_rate":-1}`)
	req := httptest.NewRequest(http.MethodPost, "/api/start", body)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
}

func TestServer_StartRejectsWrongMethod(t *testing.T) {
	s := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/api/start", nil)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rr.Code)
	}
}

func TestServer_StartRunsAndStopsPattern(t *testing.T) {
	s := newTestServer(t)

	// Start a small basic pattern.
	body := strings.NewReader(`{"pattern":"basic","item_count":10}`)
	req := httptest.NewRequest(http.MethodPost, "/api/start", body)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("start status = %d, body = %s", rr.Code, rr.Body.String())
	}

	// Give it a moment, then stop it.
	time.Sleep(200 * time.Millisecond)

	req2 := httptest.NewRequest(http.MethodPost, "/api/stop", nil)
	rr2 := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusOK {
		t.Fatalf("stop status = %d", rr2.Code)
	}

	// Wait for the pattern to actually finish.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if s.getStatus() == "stopped" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Metrics endpoint should return a metric for "basic".
	req3 := httptest.NewRequest(http.MethodGet, "/api/metrics", nil)
	rr3 := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr3, req3)
	if rr3.Code != http.StatusOK {
		t.Fatalf("metrics status = %d", rr3.Code)
	}
	var metrics map[string]*struct {
		PatternName string `json:"pattern_name"`
		Status      string `json:"status"`
	}
	if err := json.NewDecoder(rr3.Body).Decode(&metrics); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, ok := metrics["basic"]; !ok {
		t.Fatalf("expected 'basic' metric, got keys: %v", metrics)
	}
}

// TestServer_FinalMetricCapturedAfterFastCompletion is a regression test for
// the bug where a pattern that completes faster than the 100ms metric tick
// would have all-zero ItemsProduced/ItemsConsumed in the final metrics,
// because the track goroutine never got a chance to tick before the pattern
// finished. The track helper must record a final snapshot on exit.
func TestServer_FinalMetricCapturedAfterFastCompletion(t *testing.T) {
	s := newTestServer(t)

	// 20 items with no producer delay completes in microseconds - far below
	// the 100ms metric tick. With the bug, the final metrics would show 0/0.
	body := strings.NewReader(`{"pattern":"buffered","item_count":20,"buffer_size":5}`)
	req := httptest.NewRequest(http.MethodPost, "/api/start", body)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("start status = %d, body = %s", rr.Code, rr.Body.String())
	}

	// Wait for the pattern to finish naturally.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if s.getStatus() == "completed" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	req2 := httptest.NewRequest(http.MethodGet, "/api/metrics", nil)
	rr2 := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr2, req2)
	var body2 struct {
		Buffered struct {
			ItemsProduced int    `json:"items_produced"`
			ItemsConsumed int    `json:"items_consumed"`
			Status        string `json:"status"`
		} `json:"buffered"`
	}
	if err := json.NewDecoder(rr2.Body).Decode(&body2); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body2.Buffered.ItemsProduced != 20 {
		t.Errorf("expected 20 produced, got %d", body2.Buffered.ItemsProduced)
	}
	if body2.Buffered.ItemsConsumed != 20 {
		t.Errorf("expected 20 consumed, got %d", body2.Buffered.ItemsConsumed)
	}
	if body2.Buffered.Status != "completed" {
		t.Errorf("expected status=completed, got %s", body2.Buffered.Status)
	}
}

func TestServer_StopWithoutActivePatternIsNoop(t *testing.T) {
	s := newTestServer(t)
	req := httptest.NewRequest(http.MethodPost, "/api/stop", nil)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
}

func TestServer_StartReplacesRunningPattern(t *testing.T) {
	s := newTestServer(t)

	// Start a long-running pattern.
	body := strings.NewReader(`{"pattern":"basic","item_count":100000}`)
	req := httptest.NewRequest(http.MethodPost, "/api/start", body)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("start 1 status = %d, body = %s", rr.Code, rr.Body.String())
	}

	// Quickly replace it with another pattern.
	body2 := strings.NewReader(`{"pattern":"buffered","item_count":10,"buffer_size":5}`)
	req2 := httptest.NewRequest(http.MethodPost, "/api/start", body2)
	rr2 := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusOK {
		t.Fatalf("start 2 status = %d, body = %s", rr2.Code, rr2.Body.String())
	}

	// Cleanup.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = s.Shutdown(ctx)
}

func TestServer_ShutdownIsIdempotent(t *testing.T) {
	s := newTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	if err := s.Shutdown(ctx); err != nil {
		t.Fatalf("first shutdown: %v", err)
	}
	// Second call should not panic.
	if err := s.Shutdown(ctx); err != nil {
		t.Fatalf("second shutdown: %v", err)
	}
}

// TestServer_SecurityHeaders verifies that the security headers middleware
// sets X-Content-Type-Options, X-Frame-Options, and Referrer-Policy on
// every response.
func TestServer_SecurityHeaders(t *testing.T) {
	s := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/api/health", nil)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)

	if got := rr.Header().Get("X-Content-Type-Options"); got != "nosniff" {
		t.Errorf("X-Content-Type-Options = %q, want nosniff", got)
	}
	if got := rr.Header().Get("X-Frame-Options"); got != "DENY" {
		t.Errorf("X-Frame-Options = %q, want DENY", got)
	}
	if got := rr.Header().Get("Referrer-Policy"); got != "strict-origin-when-cross-origin" {
		t.Errorf("Referrer-Policy = %q, want strict-origin-when-cross-origin", got)
	}
}

// TestServer_StartReturnsJSONError is a regression test verifying that bad
// /api/start requests return a JSON error payload (so the frontend can show
// the message) rather than a plain text response.
func TestServer_StartReturnsJSONError(t *testing.T) {
	s := newTestServer(t)
	req := httptest.NewRequest(http.MethodPost, "/api/start",
		strings.NewReader(`{"pattern":"bogus"}`))
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
	if got := rr.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", got)
	}
	var body map[string]string
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["error"] == "" {
		t.Fatalf("expected non-empty error field, got %v", body)
	}
}

// TestServer_CompletedStatusPersists is a regression test for the bug where
// the track goroutine would overwrite a "completed" status with "running"
// after the pattern finished naturally. We let a small pattern complete and
// then sample the metrics 400ms later; the status must still be "completed".
func TestServer_CompletedStatusPersists(t *testing.T) {
	s := newTestServer(t)

	req := httptest.NewRequest(http.MethodPost, "/api/start",
		strings.NewReader(`{"pattern":"basic","item_count":5}`))
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("start status = %d", rr.Code)
	}

	// Wait for the pattern to finish and the defer to record "completed".
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		m := s.collector.GetMetrics("basic")
		if m != nil && m.Status == "completed" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Sleep past several tick periods to give a leaked track goroutine a
	// chance to overwrite the status.
	time.Sleep(400 * time.Millisecond)

	m := s.collector.GetMetrics("basic")
	if m == nil {
		t.Fatal("metrics for 'basic' missing")
	}
	if m.Status != "completed" {
		t.Fatalf("status = %q, want completed (track goroutine overwrote it)", m.Status)
	}
}

// TestServer_WebSocketReceivesMetrics verifies that an open WebSocket
// connection actually receives metric snapshots while a pattern runs.
func TestServer_WebSocketReceivesMetrics(t *testing.T) {
	s := newTestServer(t)
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	wsURL := strings.Replace(srv.URL, "http://", "ws://", 1) + "/api/ws"
	c := newWSClient(t, wsURL)
	defer c.Close()

	// After handshake, the server should send at least one initial snapshot
	// for any metric already in the collector. It may be empty. Now start a
	// pattern so metrics flow.
	body := strings.NewReader(`{"pattern":"basic","item_count":50}`)
	req := httptest.NewRequest(http.MethodPost, "/api/start", body)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("start status = %d", rr.Code)
	}
	defer func() {
		req2 := httptest.NewRequest(http.MethodPost, "/api/stop", nil)
		rr2 := httptest.NewRecorder()
		s.Handler().ServeHTTP(rr2, req2)
	}()

	// Read frames for a couple of seconds. The metric tick is 100ms.
	received := 0
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_ = c.conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		op, payload, err := c.readFrame()
		if err != nil {
			continue
		}
		if op == opText && len(payload) > 0 {
			received++
			// We only need to confirm we got a JSON-shaped payload.
			if !bytes.HasPrefix(payload, []byte("{")) {
				t.Fatalf("payload not JSON: %s", string(payload))
			}
		}
	}
	if received == 0 {
		t.Fatal("did not receive any WebSocket frames within 2s")
	}
	fmt.Printf("received %d WS frames\n", received)
}
