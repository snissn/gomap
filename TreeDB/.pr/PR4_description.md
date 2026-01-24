## Summary
- Add `valuelog.AutotuneOptions` + wiring through public/cached options.
- Trainer now evaluates dict candidates and K using throughput scoring when IO cost is known, with gain/dwell gating for switches.
- Integrate autotune candidates, safety margins, and size gating in the cached value-log path.

## Testing
- `go test ./... -count=1`

## Benchmarks
- Not run.
