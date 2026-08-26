# #4328 pure-text qualification evidence

Status: the strict full 10k/100k/1M matrix is retained in `manifest.json` and
`report.json`; the validator accepts it. The pre-fix public
`IngestChunkedDocument` baseline remains under `smoke-10k-r3/`.

The current public `IngestChunkedDocuments` 10k fixture is
`smoke-10k-r1/`: 10,000 parents, 30,000 children, 40,000 live rows, 40 durable
batches of at most 256 sources, 41 text generations, 0.524 s source/chunk wall,
84,164,608-byte peak RSS, 3,565,035 cumulative allocations, and 39,846,262
WAL-excluded physical bytes. Against the frozen baseline (20,001 generations,
155.567 s, 4,180,574,208-byte RSS, 82,293,880 cumulative allocations, and
5,621,678,453-byte WAL-excluded physical), it meets the generation, sub-1-GiB
physical, and >=2x throughput gates without an RSS, allocation, or storage
regression. The manifest hash-binds these typed baseline values and thresholds;
the validator rejects a report row that violates any one of them.

`smoke-100k-r{1,2,3}/` and `smoke-1m-r{1,2,3}/` retain all four raw modes for
the required repetitions. Each mode/repetition was measured in a fresh child
process; rows bind the actual repetition, deterministic per-scale fixture and
ID hashes, and process-scoped peak RSS, while maintenance rows bind observed
deletion tombstone debt. Source fixture construction is outside the
timed/allocation boundary and no duplicate encoded document corpus is retained
through chunk ingestion. Every source row records actual batch size/count and
checkpoint/close/reopen-validation score-only zero-fetch evidence. The v7
validator pins each deterministic mode/scale probe result count and digest and
verifies every current raw row against manifest-anchored digests and its report
row. Generated DB directories were deleted after copying only raw rows; no
transient DBs are retained.

## Measured revision

The measurements were produced at the clean, immutable commit
[`dc24070e430a7d7f0a33cb78cbd1aea6bd348d90`](https://github.com/snissn/gomap/commit/dc24070e430a7d7f0a33cb78cbd1aea6bd348d90),
root tree `2f3d71a82d1ae6a47f9f136a18235ec0a839599e`, TreeDB subtree
`e0334394834f5c97a5a805121c8b5e480e366f64`, and qualification-harness
subtree `2aadaa0f01543fbe4bae7bd56b2316737f2e7b88`. Its
`TreeDB/collections/document_chunking.go` blob is
`c088814d49fc0087a0145fe4363fe49ee77b234a`. `SHA256SUMS` binds the final
retained manifest, report, and every raw row.

JSON shape validation alone does not prove those Git relationships. The CLI
must run inside the candidate repository checkout with the measured objects
available locally. It resolves the measured commit to the manifest root tree,
TreeDB subtree, qualification-harness subtree, and implementation blob. It
independently requires the candidate `HEAD` TreeDB and qualification-harness
subtrees to equal the measured subtrees and the supplied manifest bytes to
equal the manifest blob committed at candidate `HEAD`. Artifact-only
descendants may differ only while that manifest blob is unchanged. Any runtime
or harness drift, manifest substitution, missing Git object, or unavailable Git
object database fails closed.

From the candidate repository root, validate the retained artifact with:

```sh
go run ./cmd/treedb_text_ingest_qual \
  -manifest artifacts/4328-text-ingestion-qualification/manifest.json \
  -report artifacts/4328-text-ingestion-qualification/report.json
```
