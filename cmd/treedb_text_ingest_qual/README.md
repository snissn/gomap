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

Every row separately accounts for source documents, generated chunks, returned
indexed parent rows, total live indexed rows, and whether source parents are
text-indexed (maintenance may intentionally have fewer live rows). For
`source_chunk`, live rows must equal returned parents plus returned children.
`source_chunk` uses the public text-only `IngestChunkedDocuments` parent-and-child
lifecycle with no vector index or embedder; rows record the actual maximum batch
size and batch count. Its timed wall boundary includes deterministic chunk
planning and normal durable parent/child text writes. One bounded public batch
is the atomicity unit. Stages
and resource metrics are either `observed` or `unavailable` with a reason;
zero is not used as a made-up measurement. The manifest binds clean VCS and
vectors-disabled/zero-vector-index state as observed product identity. Every row records stage timing, physical storage, logical text-v2 component
bytes, maintenance debt, and an actual checkpoint/close/reopen score-only probe.
The validator rejects count drift, zero-document-probe violations, failed reopen,
dirty/vector product identity, incomplete accounting, and ambiguous retained rows.

Physical storage is observed only after checkpoint and close by walking the DB
directory. `physical_index_page_bytes`, `physical_value_log_bytes`,
`physical_wal_bytes`, and `physical_other_bytes` are disjoint and sum exactly to
`physical_total_bytes`; `physical_total_wal_excluded_bytes` is also reported.
Unknown regular files are retained in `other_paths`, never silently omitted.
`logical_primary_payload_bytes` (known input documents) and the text-v2
components are logical measures and explicitly overlap physical storage, so they
are non-additive. CPU and max RSS use `getrusage` on Darwin/Linux; allocations
are cumulative `runtime.MemStats` malloc counts, not B/op.

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
score-only APIs. It writes DBs and raw rows under the supplied directory. The
raw smoke files are deliberately not retained qualification artifacts because
they lack the complete retained 100k/1M repetition matrix and manifest-bound
summaries.

```sh
go run ./cmd/treedb_text_ingest_qual \
  -produce-smoke /tmp/gomap-4328-smoke -scale 10000
```

It records unavailable stage/resource metrics with explicit reasons rather than
inventing zeroes. Do not treat an unvalidated or partial 1M run as qualification
evidence.
