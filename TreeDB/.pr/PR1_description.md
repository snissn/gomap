## Summary
- Extend value-log FrameStats with Attempted/Kept semantics and sampled EncodeNs.
- Writer now reports Attempted on compression fallback; raw paths remain Kept=false.
- Update caching stats + benches to use Kept naming.

## Testing
- `go test ./... -count=1`

## Benchmarks
- Not run.
