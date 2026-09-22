## Summary
- Add virtual-time helpers and deterministic autotune tests in valuelog/caching.
- Add fuzz target for frame decode safety.
- Add race validation for core autotune packages.

## Testing
- `go test ./internal/valuelog -count=1`
- `go test ./... -count=1`
- `go test ./caching ./internal/valuelog ./internal/compression -race`

## Benchmarks
- Not run.
