# TreeDB pure-text ingestion qualification (#4328)

This command is the fail-closed artifact gate for the text-only qualification.
It accepts no vector/embedding work: `manifest.vectors` must be `false`.

## Artifact contract

Write immutable `manifest.json` before a run. It binds the deterministic
fixture and IDs by SHA-256, analyzer and field weights, exact command/commit,
host/cache/durability state, and the timed boundary. `dirty` must be `false`.

Write `report.json` using schema `treedb_text_ingest_qualification/v1` and set
`manifest_sha256` to the SHA-256 of the exact manifest bytes. Rows must cover
`indexed_insert`, `post_load_backfill`, `source_chunk`, and `maintenance` at
only 10k, 100k, or 1M. Retained 100k/1M mode/scale rows require serialized raw
repetitions 1, 2, and 3 and per-row median/p95 summaries.

Every row records resource rates, stage seconds (`analyzer`, `posting_builder`,
`root_mutation`, `value_log`, `checkpoint`, `reopen`), primary/text-root/
value-log/WAL/total durable bytes, text-v2 component bytes, maintenance debt,
and a checkpoint/reopen score-only probe. The validator rejects count drift,
zero-document-probe violations, failed reopen, dirty provenance, vector
contamination, incomplete accounting, and ambiguous retained rows.

`total_bytes` is durable bytes including the explicitly named components;
`wal_bytes` remains separately visible so reports can state whether a comparison
excludes it. `bytes_per_op` and `allocs_per_op` are allocation metrics, not a
memory-footprint label; `peak_rss_bytes` is the process memory metric.

## Validate

```sh
export GOROOT=/Users/michaelseiler/.gvm/pkgsets/go1.25.5/global/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.0.darwin-arm64
export PATH="$GOROOT/bin:$PATH" GOTOOLCHAIN=local GOCACHE=/tmp/gomap-4328-gocache

go run ./cmd/treedb_text_ingest_qual \
  -manifest /path/to/manifest.json -report /path/to/report.json
```

The collection runner that creates these rows remains the next M0/M1 step; do
not treat an unvalidated or partial 1M run as qualification evidence.
