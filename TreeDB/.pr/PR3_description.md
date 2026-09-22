## Summary
- Add throughput-based keep decision for value-log compression with safety margin.
- Writer consumes IO/encode EWMAs via SetKeepPolicy and falls back to size-based when unknown.
- Add unit tests for the keep inequality and margin sensitivity.

## Testing
- `go test ./internal/valuelog -count=1`
- `go test ./... -count=1`

## Benchmarks
- Not run.
