# Production Audit — Issues Found

**Date:** 2026-06-08  
**Repository:** https://github.com/sanskarpan/multithreaded-producer-consumer

---

## CRITICAL Issues

### 1. app.js orphan code blocks cause SyntaxError (Issue #87)
- **File:** `web/static/app.js`
- **Root Cause:** Duplicate WebSocket handlers and start() error handler at class body level
- **Impact:** Dashboard UI completely broken on page load
- **Fix:** Rewrote app.js, removed orphan code blocks, used safe DOM API
- **Commit:** `5b01c24`

### 2. app.js undefined constants (Issue #87)
- **File:** `web/static/app.js`
- **Root Cause:** INITIAL_RECONNECT_DELAY_MS, MAX_RECONNECT_DELAY_MS, CHART_REDRAW_HZ never defined
- **Impact:** WebSocket reconnection and chart redraw broken
- **Fix:** Defined all constants with safe defaults
- **Commit:** `5b01c24`

### 3. app.js variable name mismatch (Issue #87)
- **File:** `web/static/app.js`
- **Root Cause:** `this.reconnectAttempt` vs `this.reconnectAttempts` inconsistency
- **Impact:** Reconnect backoff logic broken
- **Fix:** Unified to `this.reconnectAttempts`
- **Commit:** `5b01c24`

---

## HIGH Issues

### 4. BatchConsumer no auto-flush (Issue #88)
- **File:** `consumer/consumer.go`
- **Root Cause:** BaseConsumer.Consume returns on channel close without flushing partial batches
- **Impact:** Silent data loss for items in partial batches
- **Fix:** Added BatchConsumer.Consume override with auto-flush
- **Commit:** `d12a08f`

### 5. Goroutine leak on pattern replacement (Issue #89)
- **File:** `web/server/server.go`
- **Root Cause:** Pattern goroutine not tracked via WaitGroup
- **Impact:** Goroutine leaks on rapid pattern replacement
- **Fix:** Added sync.WaitGroup tracking and stopPattern waits for exit
- **Commit:** `49847ad`

### 6. RateLimited Stats returns 0 during execution (Issue #90)
- **File:** `patterns/rate_limited.go`
- **Root Cause:** Stats() reads from stored atomic only populated after Run() completes
- **Impact:** Dashboard shows 0% progress during execution
- **Fix:** Stats() reads live consumed count from consumers during execution
- **Commit:** `99ab2a5`

---

## MEDIUM Issues

### 7. Unbounded metric memory growth (Issue #91)
- **File:** `metrics/metrics.go`
- **Root Cause:** No metric eviction mechanism
- **Impact:** Unbounded memory growth in long-running servers
- **Fix:** Added EvictCompleted(maxAge) method, called after each pattern completion
- **Commit:** `0c0939e`

### 8. Missing security headers (Issue #92)
- **File:** `web/server/server.go`
- **Root Cause:** No CSP/CORS/X-Content-Type-Options headers
- **Impact:** Potential clickjacking and MIME sniffing attacks
- **Fix:** Added securityHeaders middleware
- **Commit:** `49847ad`

### 9. No SIGINT handling in demo (Issue #93)
- **File:** `cmd/demo/main.go`
- **Root Cause:** No signal handler registered
- **Impact:** Poor UX, no clean shutdown
- **Fix:** Added SIGINT/SIGTERM handler
- **Commit:** `adf2951`

---

## LOW Issues (Deferred)

| # | Issue | Reason Deferred |
|---|-------|-----------------|
| 11 | No HTTPS support | Requires TLS cert management, out of scope for MVP |
| 12 | No request correlation IDs | Nice-to-have, not blocking production use |
| 13 | No Cache-Control headers | Static assets served by Go FileServer, low risk |
| 14 | No Origin validation on WebSocket | Acceptable for single-origin dashboard |
