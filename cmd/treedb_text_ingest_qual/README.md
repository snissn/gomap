# TreeDB pure-text ingestion qualification (#4328)

This command is the fail-closed artifact gate for the text-only qualification.
It accepts no vector/embedding work: `manifest.vectors` must be `false`.

## Artifact contract

Write immutable `manifest.json` before a run. It binds the deterministic
fixture and IDs by SHA-256, analyzer and field weights, exact command/commit,
host/cache/durability state, and the timed boundary. `dirty` must be `false`.

Write `report.json` using schema `treedb_text_ingest_qualification/v2` and set
`manifest_sha256` to the SHA-256 of the exact manifest bytes. Rows must cover the complete mode × scale matrix: `indexed_insert`,
`post_load_backfill`, `source_chunk`, and `maintenance` at each of 10k, 100k,
and 1M. Each 10k group has exactly smoke repetition 1; each retained 100k/1M
group has exactly repetitions 1, 2, and 3. Summaries occur once per
mode/scale, never in raw rows, and are recomputed from those raw repetitions.

Every row separately accounts for source documents, generated chunks, and
indexed live rows (maintenance may intentionally have fewer live rows). Stages
and resource metrics are either `observed` or `unavailable` with a reason;
zero is not used as a made-up measurement. The manifest binds clean VCS and
vectors-disabled/zero-vector-index state as observed product identity. Every
row records stage timing, primary/text-root/value-log/WAL bytes, text-v2
component bytes, maintenance debt, and an actual checkpoint/close/reopen
score-only probe. The validator rejects count drift, zero-document-probe
violations, failed reopen, dirty/vector product identity, incomplete accounting,
and ambiguous retained rows.

`total_bytes` is exactly the disjoint `primary + text_root + value_log`
components; it explicitly excludes WAL, which remains separately visible.
Primary must exclude both text-root and value-log bytes, so value-log bytes are
never double counted. `bytes_per_op` and `allocs_per_op` are allocation metrics, not a
memory-footprint label; `peak_rss_bytes` is the process memory metric.

## Validate

```sh
export GOROOT=/Users/michaelseiler/.gvm/pkgsets/go1.25.5/global/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.0.darwin-arm64
export PATH="$GOROOT/bin:$PATH" GOTOOLCHAIN=local GOCACHE=/tmp/gomap-4328-gocache

go run ./cmd/treedb_text_ingest_qual \
  -manifest /path/to/manifest.json -report /path/to/report.json
```

## Real 10k smoke producer

The command can create one real raw row for every mode using the public
collection, text-v2, deterministic chunker, checkpoint, close/reopen, and
score-only APIs. It writes DBs and raw rows under the supplied directory; the
raw smoke files are deliberately not retained qualification artifacts because
they do not claim physical disjoint storage accounting or the complete retained
matrix.

```sh
go run ./cmd/treedb_text_ingest_qual \
  -produce-smoke /tmp/gomap-4328-smoke -scale 10000
```

It records unavailable stage/resource metrics with explicit reasons rather than
inventing zeroes. Do not treat an unvalidated or partial 1M run as qualification
evidence.
