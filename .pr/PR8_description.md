# PR8: regression RC + benchmark harness

## Summary
- add unified_bench dataset value patterns to support compressible workloads
- expose optional TreeDB flags (journal lanes, value-log options, index flags) for regression benchmarking

## Tests
- `go test ./cmd/unified_bench -count=1`

## Notes
- this PR is a regression/RC umbrella; follow-up commits will document bisect results and fixes
