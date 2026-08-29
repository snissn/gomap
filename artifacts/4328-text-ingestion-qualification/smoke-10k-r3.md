# #4328 corrected-workflow 10k smoke

- Commit: `f3df32c34ef9817b0bc17f537941f34625738493`
- Date: 2026-08-25
- Command: `GOROOT=/Users/michaelseiler/.gvm/pkgsets/go1.25.5/global/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.0.darwin-arm64 PATH="$GOROOT/bin:$PATH" GOTOOLCHAIN=local GOCACHE=/tmp/gomap-4328-gocache /usr/bin/time -l go run ./cmd/treedb_text_ingest_qual -produce-smoke artifacts/4328-text-ingestion-qualification/smoke-10k-r3 -scale 10000`
- Command result: exit 0; 157.32 s wall; `/usr/bin/time -l` maximum RSS 4,180,574,208 bytes.

Raw rows are retained in `smoke-10k-r3/*.raw.json`; their temporary DB directories were deleted after the rows were written. All four modes checkpointed, closed, reopened, and passed a score-only `refund` probe with `documents_fetched: 0` and `fail_closed: 0`.

`source_chunk.raw.json` is the actual public `Collection.IngestChunkedDocument` lifecycle with vectors disabled: 10,000 returned parents, 30,000 returned child IDs, and 40,000 live text-v2 rows. Its physical storage was 5,621,678,453 bytes after checkpoint and close. The index-page bucket was 5,621,678,080 bytes; the zero value-log/WAL buckets and 373-byte other bucket are stated rather than inferred.

This is a smoke artifact, **not** an accepted retained qualification report: the strict full-matrix validator intentionally rejects it because it has no 100k/1M repetitions or manifest/report summaries. With 48 GiB free at post-cleanup, the measured source-chunk 10k physical footprint projects to at least 52.4 GiB at 100k before filesystem overhead, so the required 100k source-chunk DB cannot be safely created on this worker. No 100k/1M row is claimed.
