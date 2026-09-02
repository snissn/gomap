## Summary
- Add code-map appendix + worklog section to autotuner runbook.
- Bench metrics report `kept_frac` only to avoid ambiguous “compressed” labeling.

## Testing
- `go test ./... -count=1`

## Benchmarks
- Not run (no behavior change).
