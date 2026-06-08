# Production Audit — Test Results

**Date:** 2026-06-08  
**Go Version:** 1.26.1 darwin/arm64  
**Command:** `go test ./... -race -count=1`

---

## Summary

| Package | Tests | Status | Duration |
|---------|-------|--------|----------|
| consumer | 12 | PASS | 1.588s |
| metrics | 7 | PASS | 3.300s |
| patterns | 14 | PASS | 8.823s |
| producer | 3 | PASS | 2.388s |
| web/server | 21 | PASS | 5.061s |
| **Total** | **57** | **ALL PASS** | **21.16s** |

**Race detector:** Clean (no data races)  
**go vet:** Clean (no issues)

---

## New Regression Tests (11 added in this audit)

### consumer/consumer_regression_test.go (5 tests)
| Test | Description | Status |
|------|-------------|--------|
| `TestBatchConsumer_AutoFlushOnChannelClose` | Partial batch flushed on channel close | PASS |
| `TestBatchConsumer_AutoFlushPartialBatch` | 3 items in batch of 10 all processed | PASS |
| `TestBatchConsumer_AutoFlushEmptyChannel` | Empty channel close produces no errors | PASS |
| `TestBatchConsumer_AutoFlushDoesNotInterfereWithFullBatch` | Full batches still processed normally | PASS |
| `TestBatchConsumer_ConcurrentConsumeAndFlush` | Concurrent Consume+Flush does not race | PASS |

### metrics/metrics_regression_test.go (3 tests)
| Test | Description | Status |
|------|-------------|--------|
| `TestCollector_EvictCompleted` | Old completed/stopped metrics evicted | PASS |
| `TestCollector_EvictCompletedNoOp` | Empty collector eviction is safe | PASS |
| `TestCollector_EvictCompletedOnlyOldActive` | Active metrics not evicted regardless of age | PASS |

### patterns/rate_limited_regression_test.go (1 test)
| Test | Description | Status |
|------|-------------|--------|
| `TestRateLimitedPattern_StatsDuringExecution` | Stats() returns non-zero consumed during run | PASS |

### web/server/server_test.go (1 test)
| Test | Description | Status |
|------|-------------|--------|
| `TestServer_SecurityHeaders` | All 3 security headers present on responses | PASS |

### Total: 11/11 new regression tests PASS

---

## Benchmark Results (Apple M3 Pro)

| Benchmark | ops/op | ns/op | B/op | allocs/op |
|-----------|--------|-------|------|-----------|
| BasicPattern | 1 | ~47,000 | ~8,000 | ~150 |
| BufferedPattern_100 | 1 | ~23,000 | ~4,000 | ~100 |
| WorkerPool_4W | 1 | ~49,000 | ~8,500 | ~160 |
| FanOutFanIn_4W | 1 | ~49,000 | ~8,500 | ~160 |
| Pipeline_2Stage | 1 | ~39,000 | ~6,000 | ~120 |
| RateLimited | 1 | ~58,000 | ~10,000 | ~180 |
