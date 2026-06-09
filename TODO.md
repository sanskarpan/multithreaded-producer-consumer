# TODO — Production Readiness Backlog

**Created:** 2026-06-09
**Source:** PRODUCTION_GAP_ANALYSIS.md
**Total Items:** 42
**Estimated Effort:** ~37 hours

---

## P0 — Deploy Blockers (14 items)

Cannot deploy to production without these.

- [ ] **#94** Add MIT LICENSE file
  - README claims MIT but no LICENSE file exists
  - Legal risk — anyone using this code has no license grant
  - Effort: 5 min

- [ ] **#95** Add Dockerfile (multi-stage, distroless)
  - No container support — can't deploy to Kubernetes/ECS/Cloud Run/Fly.io
  - Multi-stage: `golang:1.26-alpine` builder → `gcr.io/distroless/static` runtime
  - CGO_ENABLED=0, non-root user (USER 1000), `-ldflags="-s -w"`
  - Build args: VERSION, GIT_COMMIT, BUILD_DATE
  - Effort: 30 min

- [ ] **#96** Add .dockerignore
  - Docker builds copy `.git/`, tests, docs into build context
  - Exclude: `.git/`, `*_test.go`, `*.md`, `bin/`, `tmp/`, `REVIEW_PROMPT.md`
  - Effort: 5 min

- [ ] **#97** Add docker-compose.yml
  - No reproducible local dev environment
  - Services: app (build from Dockerfile), ports 8080:8080
  - Volume mount for static files (hot reload)
  - Environment variables: PORT, LOG_LEVEL, STATIC_DIR
  - Health check: `curl -f http://localhost:8080/api/health`
  - Effort: 20 min

- [ ] **#98** Add Makefile
  - No standardized build/test/lint commands
  - Targets: `help`, `build`, `run`, `test`, `test-race`, `test-cover`, `lint`, `vet`, `bench`, `audit`, `docker-build`, `docker-up`, `docker-down`, `clean`
  - Variables: APP_NAME, GO_VERSION, GIT_TAG, GIT_SHA, BUILD_DIR
  - `.PHONY` for all targets, dependency chains
  - Effort: 30 min

- [ ] **#99** Add CI/CD pipeline (.github/workflows/ci.yml)
  - No automated quality gates
  - Jobs: lint → test → build → scan
  - Matrix: Go 1.25.x, 1.26.x on ubuntu-latest
  - Upload coverage to Codecov
  - Trivy image scan on Docker build
  - Effort: 1-2 hours

- [ ] **#100** Add .golangci.yml lint configuration
  - Only `go vet` running — missing errcheck, gosec, gocritic, etc.
  - Enable: errcheck, govet, staticcheck, unused, gosimple, ineffassign, gosec, gocritic, bodyclose, whitespace, misspell
  - Timeout: 5m
  - Exclude errcheck from test files
  - Effort: 15 min

- [ ] **#101** Add Kubernetes-ready health endpoints (/healthz, /readyz)
  - Only `/api/health` exists — no liveness/readiness split
  - `/healthz` (liveness): returns 200 if process is running
  - `/readyz` (readiness): checks internal state (can patterns start?)
  - Separate from API health endpoint
  - Effort: 30 min

- [ ] **#102** Add configuration management (environment variables)
  - Everything is hardcoded: port 8080, metric tick 100ms, item limits
  - 12-factor app compliance
  - Env vars: `PORT` (default 8080), `LOG_LEVEL` (default info), `STATIC_DIR` (default web/static), `METRIC_TICK_MS` (default 100), `MAX_ITEMS` (default 10000000)
  - Validate at startup, fail fast on invalid config
  - Effort: 1-2 hours

- [ ] **#103** Add build version embedding via ldflags
  - No way to know which version is running during an incident
  - `go build -ldflags="-s -w -X main.Version=$(git describe) -X main.GitCommit=$(git rev-parse --short HEAD) -X main.BuildDate=$(date -u +%Y-%m-%dT%H:%M:%SZ)"`
  - Expose in `/api/health` response: `{"status":"ok","version":"v1.2.3","commit":"abc1234"}`
  - Log version at startup
  - Effort: 30 min

- [ ] **#104** Add request ID propagation middleware
  - Cannot correlate logs across a single request
  - Generate UUID v4, store in context, set `X-Request-Id` response header
  - Include in all structured log lines
  - Accept incoming `X-Request-Id` header (for distributed tracing)
  - Effort: 1 hour

- [ ] **#105** Add pprof profiling endpoints on private port
  - Cannot debug production performance issues
  - Import `net/http/pprof` on `127.0.0.1:6060` (never public)
  - Endpoints: `/debug/pprof/`, `/debug/pprof/heap`, `/debug/pprof/goroutine`, `/debug/pprof/profile` (CPU), `/debug/pprof/trace`
  - Add `/debug/trace` handler for bounded-duration trace capture (10s default)
  - Effort: 20 min

- [ ] **#106** Add govulncheck to CI and Makefile
  - No vulnerability scanning for stdlib or dependencies
  - `go install golang.org/x/vuln/cmd/govulncheck@latest`
  - `make audit` target: `govulncheck ./...`
  - CI job: run on every PR, fail on HIGH/CRITICAL
  - Effort: 10 min

- [ ] **#107** Add .editorconfig
  - Inconsistent formatting across editors/IDEs
  - Go conventions: indent_style = tab, indent_size = 4, charset = utf-8, end_of_line = lf, trim_trailing_whitespace = true, insert_final_newline = true
  - Effort: 5 min

---

## P1 — Production Hardening (13 items)

Works unreliably under load without these.

- [ ] **#108** Add Prometheus metrics endpoint (/metrics)
  - Internal metrics.Collector exists but isn't Prometheus-compatible
  - `/metrics` endpoint using `prometheus/client_golang`
  - Export: `http_requests_total`, `http_request_duration_seconds`, `goroutine_count`, `pattern_status`, `items_produced_total`, `items_consumed_total`
  - Effort: 2-3 hours

- [ ] **#109** Add OpenTelemetry distributed tracing
  - No request tracing across service boundaries
  - Initialize OTel SDK with OTLP exporter (gRPC)
  - Tracing middleware: create root span, propagate context
  - At minimum: trace external HTTP calls
  - Resource attributes: service.name, service.version, deployment.environment
  - Effort: 2-3 hours

- [ ] **#110** Add CORS middleware
  - Frontend may be served from different origin during development
  - Configurable: allowed origins, methods, headers
  - Default: same-origin for production, `*` for development
  - Set `Access-Control-Allow-Origin`, `Access-Control-Allow-Methods`, `Access-Control-Allow-Headers`
  - Effort: 30 min

- [ ] **#111** Add rate limiting middleware
  - API endpoints have no protection against abuse
  - Token bucket or sliding window rate limiter
  - Configurable per-endpoint limits (or global default)
  - Return `429 Too Many Requests` with `Retry-After` header
  - Effort: 1-2 hours

- [ ] **#112** Add API request validation
  - Only basic checks in handleStart — no struct tag validation
  - Use `go-playground/validator` or manual validation for all inputs
  - Return structured validation errors: `{"error":"validation failed","details":[{"field":"item_count","message":"must be positive"}]}`
  - Effort: 1-2 hours

- [ ] **#113** Add graceful WebSocket connection draining
  - WebSocket connections may be killed abruptly during shutdown
  - Track active WebSocket connections in Server
  - On shutdown: send close frames to all active connections, wait for clients to close, then shutdown HTTP server
  - Effort: 1 hour

- [ ] **#114** Add test coverage reporting to CI
  - No visibility into which code paths are tested
  - `go test -coverprofile=coverage.out -covermode=atomic ./...`
  - Upload to Codecov or coveralls
  - Set minimum threshold: 70% overall, 80% for core packages
  - `make test-cover` target: generate HTML report
  - Effort: 30 min

- [ ] **#115** Add integration tests
  - Only unit tests exist — no end-to-end HTTP + WebSocket tests
  - Integration tests using `httptest.NewServer` that test full request lifecycle
  - Test: start pattern → verify metrics → stop pattern → verify cleanup
  - Test: WebSocket connection receives metric updates
  - Build tag: `//go:build integration`
  - Effort: 2-3 hours

- [ ] **#116** Add fuzz testing
  - Edge cases not systematically explored
  - Go native fuzzing (`testing.F`) for:
    - WebSocket accept-key computation
    - JSON request body decoding
    - Pattern config validation
    - Rate limiter edge cases
  - Effort: 1-2 hours

- [ ] **#117** Add structured error types (AppError)
  - Basic string errors — no error codes, no distinction between user/internal errors
  - Define `AppError` type: Code (int), Message (string), InternalDetail (string), Cause (error)
  - Map codes to HTTP status codes
  - Error middleware: log internals, return safe message to client
  - Effort: 1-2 hours

- [ ] **#118** Add HTTP server timeouts
  - Default timeouts — slowloris vulnerability
  - `ReadTimeout: 15s`, `WriteTimeout: 15s`, `IdleTimeout: 60s`
  - `ReadHeaderTimeout: 5s`
  - Effort: 10 min

- [ ] **#119** Add Cache-Control headers
  - Static assets re-fetched on every load
  - `Cache-Control: public, max-age=3600` for static assets (CSS, JS, images)
  - `Cache-Control: no-cache` for API responses
  - Add to securityHeaders middleware
  - Effort: 15 min

- [ ] **#120** Add Origin validation on WebSocket upgrades
  - WebSocket accepts connections from any origin
  - Validate `Origin` header against allowed origins list
  - Reject connections from unknown origins
  - Effort: 30 min

---

## P2 — Polish & Ecosystem (9 items)

Looks unprofessional without these.

- [ ] **#121** Add OpenAPI/Swagger documentation
  - API consumers must read source code to understand endpoints
  - Hand-maintain `openapi.yaml` or generate from code annotations
  - Serve at `/api/docs` usingSwagger UI or Redoc
  - Document: all endpoints, request/response schemas, error codes
  - Effort: 2-3 hours

- [ ] **#122** Add CHANGELOG.md
  - Users can't see what changed between versions
  - Follow Keep a Changelog format (keepachangelog.com)
  - Document: Added, Changed, Deprecated, Removed, Fixed, Security
  - Link to GitHub releases
  - Effort: 30 min

- [ ] **#123** Add CONTRIBUTING.md
  - Contributors don't know how to set up dev environment
  - Document: prerequisites (Go 1.26+), setup, test commands, lint commands, PR process, code style
  - Effort: 30 min

- [ ] **#124** Add .pre-commit-config.yaml
  - Developers can commit unformatted code
  - Hooks: go-fmt, go-imports, golangci-lint, trailing-whitespace, end-of-file-fixer
  - Effort: 15 min

- [ ] **#125** Add GoReleaser configuration
  - No automated release binary builds
  - `.goreleaser.yml`: cross-compile (linux/darwin × amd64/arm64), archive with README, checksums
  - GitHub Actions release workflow on tag push
  - Effort: 1 hour

- [ ] **#126** Add benchmark baseline tracking
  - Benchmarks exist but no regression detection
  - `benchstat` in CI: compare against committed baseline
  - Fail on significant regressions (>10% slower)
  - Commit `benchstat-baseline.txt` after each release
  - Effort: 30 min

- [ ] **#127** Add load testing scripts
  - No way to verify expected throughput
  - k6 or vegeta scripts for each pattern
  - Document: expected ops/sec, latency targets, resource usage
  - `make load-test` target
  - Effort: 2-3 hours

- [ ] **#128** Add operational runbook
  - On-call engineers don't know how to respond to alerts
  - Runbook with: common alerts, investigation steps, remediation, escalation
  - Cover: high latency, memory growth, goroutine leak, pattern failure
  - Effort: 1-2 hours

- [ ] **#129** Add Architecture Decision Records
  - Design decisions not documented
  - `docs/adr/` directory with ADR template
  - Record key decisions: hand-rolled WebSocket, no deps, slog over zap, etc.
  - Effort: 1-2 hours

---

## P3 — Code Quality (6 items)

Code smell without these.

- [ ] **#130** Convert remaining tests to table-driven format
  - Some tests exist but not consistently table-driven
  - Audit all test files, convert to `tests := []struct{name string; ...}{}` pattern
  - Effort: 1 hour

- [ ] **#131** Add shared test utilities
  - No shared test helpers — each test file reinvents setup
  - Create `internal/testutil/` with: test server constructor, assertion helpers, test data generators
  - Effort: 30 min

- [ ] **#132** Enforce gofumpt formatting
  - Not enforced in CI or pre-commit
  - Add `gofumpt -l .` check to CI and pre-commit hooks
  - Effort: 10 min

- [ ] **#133** Audit error wrapping consistency
  - Inconsistent `fmt.Errorf` without `%w`
  - Audit all `fmt.Errorf` calls, add `%w` for error wrapping
  - Ensure all returned errors are wrapped with context
  - Effort: 30 min

- [ ] **#134** Add godoc comments to all exported symbols
  - Missing on some exported functions/types
  - Audit all exported types, functions, methods — add doc comments
  - Follow Go conventions: start with the name of the thing being documented
  - Effort: 1 hour

- [ ] **#135** Add CONTRIBUTING.md development workflow
  - Document: branch naming, commit message format, PR template, review process
  - Reference conventional commits or similar standard
  - Effort: 30 min

---

## Summary

| Priority | Items | Estimated Effort | Status |
|----------|-------|-----------------|--------|
| P0 | 14 | ~8 hours | 0/14 complete |
| P1 | 13 | ~15 hours | 0/13 complete |
| P2 | 9 | ~10 hours | 0/9 complete |
| P3 | 6 | ~4 hours | 0/6 complete |
| **Total** | **42** | **~37 hours** | **0/42 complete** |

---

## Implementation Order

### Week 1: Foundation (P0)
1. #94 LICENSE (5 min)
2. #107 .editorconfig (5 min)
3. #96 .dockerignore (5 min)
4. #106 govulncheck (10 min)
5. #100 golangci-lint (15 min)
6. #98 Makefile (30 min)
7. #95 Dockerfile (30 min)
8. #97 docker-compose.yml (20 min)
9. #101 health endpoints (30 min)
10. #103 build versioning (30 min)
11. #102 config management (1-2 hours)
12. #99 CI/CD pipeline (1-2 hours)

### Week 2: Hardening (P1)
13. #118 HTTP timeouts (10 min)
14. #119 Cache-Control (15 min)
15. #120 Origin validation (30 min)
16. #104 Request IDs (1 hour)
17. #110 CORS (30 min)
18. #117 Error types (1-2 hours)
19. #114 Coverage reporting (30 min)
20. #105 pprof (20 min)
21. #108 Prometheus metrics (2-3 hours)
22. #111 Rate limiting (1-2 hours)

### Week 3: Polish (P2)
23. #109 OpenTelemetry (2-3 hours)
24. #115 Integration tests (2-3 hours)
25. #116 Fuzz testing (1-2 hours)
26. #121 OpenAPI docs (2-3 hours)
27. #122 CHANGELOG (30 min)
28. #123 CONTRIBUTING (30 min)
29. #124 Pre-commit hooks (15 min)
30. #125 GoReleaser (1 hour)
31. #127 Load testing (2-3 hours)

### Week 4: Quality (P3)
32. #130 Table-driven tests (1 hour)
33. #131 Test utilities (30 min)
34. #132 gofumpt (10 min)
35. #133 Error wrapping (30 min)
36. #134 Godoc comments (1 hour)
37. #135 Contributing workflow (30 min)
