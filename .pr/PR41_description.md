Background / harness in PR97:
- https://github.com/snissn/gomap/pull/97

Builds on PR98 (Branch 3: value-log writer direct-to-appendBuf EncodeAll):
- https://github.com/snissn/gomap/pull/98

## Goal (Branch 4: mode3 journal / commitlog)
Reduce extra user-space copies in the mode3 commitlog write path without changing on-disk format or durability ordering.

## Change summary
- `TreeDB/internal/commitlog/writer.go`: when commitlog segment compression is not used for the segment, `AppendBatch` now:
  - computes the payload CRC incrementally from record parts
  - writes the segment header + payload parts directly to the buffered writer
  - avoids building a contiguous `payload` scratch buffer (removes an extra copy)

## Validation
- `go test ./TreeDB/internal/commitlog -count=1`
- `go test ./TreeDB/internal/valuelog -run TestDictAppendReadRoundTrip -count=1`
- `go test ./... -count=1`

## Notes
- This is expected to matter most when commitlog carries inline values (not RID-only entries).

## Benchmark (CPU / copy-focused)
This uses `io.Discard` to isolate user-space copy/CRC costs (not filesystem performance).

- Command:
  - `go test ./TreeDB/internal/commitlog -run '^$' -bench BenchmarkCommitLogAppendBatch_UncompressedInline_Discard -benchmem -count=5`
- Before/after (benchstat):
  - geomean: -1.67% sec/op (small/noisy overall)
  - best case in this run: `records=3/val=16384`: -4.68% sec/op (+4.92% B/s), p=0.008
