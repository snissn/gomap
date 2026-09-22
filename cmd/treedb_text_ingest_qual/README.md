# TreeDB pure-text ingestion qualification (#4328)

This command is the fail-closed artifact gate for the text-only qualification.
It accepts no vector/embedding work: `manifest.observed.vectors_enabled` must be
`false` and `manifest.observed.vector_indexes` must be zero.

## Artifact contract

Freeze the deterministic fixture and IDs, analyzer and field weights, exact
command and measured commit/root tree, complete `TreeDB` and qualification
harness subtree OIDs, expected implementation path/blob, host/cache/durability
state, and timed boundary before a run. After assembling the exact report, set
its payload digest in the manifest and then treat both files as immutable.
`manifest.observed.vcs_clean` must be `true`. Validation
also rejects staged, unstaged, or untracked changes anywhere under either
measured runtime subtree. It resolves every measured OID from the manifest
commit and requires candidate `HEAD` to have byte-identical
`TreeDB` and `cmd/treedb_text_ingest_qual` subtrees. The implementation blob is
an additional narrow check, not the candidate-equivalence boundary. The
supplied manifest bytes must also hash to the exact manifest blob committed at
candidate `HEAD`; artifact-only descendants remain valid only while that blob
is unchanged.

Use schema `treedb_text_ingest_qualification/v7` for both `manifest.json` and
`report.json`. The manifest authenticates the canonical report payload (with
`manifest_sha256` blank) through `report_payload_sha256`, binds every current
raw measurement file through `raw_rows_sha256`, and is itself anchored to the
candidate `HEAD` blob. The report separately binds the SHA-256 of the exact
manifest bytes. Rows must cover the complete mode
× scale matrix: `indexed_insert`, `post_load_backfill`, `source_chunk`, and
`maintenance` at each of 10k, 100k,
and 1M. Each 10k group has exactly smoke repetition 1; each retained 100k/1M
group has exactly repetitions 1, 2, and 3. Summaries occur once per
mode/scale, never in raw rows, and are recomputed from those raw repetitions.

Every row separately accounts for source documents, generated chunks, returned
indexed parent rows, total live indexed rows, and whether source parents are
text-indexed (maintenance may intentionally have fewer live rows). Maintenance
rows record deleted-document `tombstone_debt` separately and must account for
exactly the half of source documents deleted by that workload. `stale_debt` is
an explicit metric state; the producer reports it unavailable with a reason
until TreeDB exposes a stale-debt counter. For `source_chunk`, live rows must
equal returned parents plus returned children. `source_chunk` uses the public
text-only `IngestChunkedDocuments` parent-and-child lifecycle with no vector
index or embedder; rows record the actual maximum batch size and batch count.
Its timed wall boundary includes deterministic chunk planning and normal
durable parent/child text writes. One bounded public batch is the atomicity
unit. Stages and resource metrics are either `observed` or `unavailable` with
a reason; zero is not used as a made-up measurement. The manifest binds clean
VCS and
vectors-disabled/zero-vector-index state as observed product identity. Every
row records stage timing, physical storage, logical text-v2 component bytes,
tombstone debt, and pre-close plus reopened score-only probe evidence.
`reopen_validation` measures reopen, collection/stat recovery, and the score-only
probe; the subsequent close is outside that stage. Reopen success requires
parity for live rows, generation, text storage statistics, and deterministic
probe results. The validator also pins the expected result count and digest for
every deterministic mode/scale fixture, so a consistently wrong result cannot
pass through reopen parity alone. It reads every current raw row, verifies its
manifest-anchored digest, and requires its decoded value to equal the report
row. It rejects count drift, missing or substituted raw evidence,
zero-document-probe violations, failed or non-parity reopen, dirty/vector
product identity, incomplete accounting, and ambiguous retained rows.

Physical storage is observed only after checkpoint and close by walking the DB
directory. `physical_index_page_bytes`, `physical_value_log_bytes`,
`physical_wal_bytes`, and `physical_other_bytes` are disjoint and sum exactly to
`physical_total_bytes`; `physical_total_wal_excluded_bytes` is also reported.
Unknown regular files are retained in `other_paths`, never silently omitted.
`logical_primary_payload_bytes` (known input documents) and the text-v2
components are logical measures and explicitly overlap physical storage, so they
are non-additive. CPU and max RSS use `getrusage` on Darwin/Linux; allocations
are cumulative `runtime.MemStats` malloc counts, not B/op. The smoke parent
starts one fresh child process per mode, and each row records that child's
`peak_rss_scope: fresh_process_per_mode` and PID; the validator rejects invalid
or unscoped RSS measurements.

## Validate

Use Go 1.26:

```sh
export GOTOOLCHAIN=local GOCACHE=/tmp/gomap-4328-gocache

go run ./cmd/treedb_text_ingest_qual \
  -manifest /path/to/manifest.json -report /path/to/report.json
```

## Real 10k smoke producer

The command can create one real raw row for every mode using the public
collection, text-v2, deterministic chunker, checkpoint, close/reopen, and
score-only APIs. It writes raw rows under the supplied directory and creates
each child process's temporary DB under the system temporary directory. The
raw smoke files are deliberately not retained qualification artifacts because
they lack the complete retained 100k/1M repetition matrix and manifest-bound
summaries.

```sh
go run ./cmd/treedb_text_ingest_qual \
  -produce-smoke /tmp/gomap-4328-smoke -scale 10000 -repetition 1
```

It records the supplied `-repetition` in every raw row, runs each mode in a
fresh child process, and removes each child's temporary DB after its raw row is
written. It records unavailable stage/resource metrics with explicit reasons
rather than inventing zeroes. Do not treat an unvalidated or partial 1M run as
qualification evidence.
