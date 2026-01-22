## Summary
- Add value-log autotune metrics with EWMAs for encode cost, IO cost, ratio, and throughput.
- Measure wall time at value-log append boundaries and feed metrics (with optional env-gated logging).
- Expose EWMA snapshots via cache stats.

## Testing
- `go test ./... -count=1`

## Benchmarks
- Not run.
