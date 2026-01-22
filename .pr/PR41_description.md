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
