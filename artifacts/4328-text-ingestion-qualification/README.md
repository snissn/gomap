# #4328 pure-text qualification evidence

Status: the strict full 10k/100k/1M matrix is retained in `manifest.json` and
`report.json`; the validator accepts it. The pre-fix public
`IngestChunkedDocument` baseline remains under `smoke-10k-r3/`.

The current public `IngestChunkedDocuments` 10k fixture is
`smoke-10k-r1/`: 10,000 parents, 30,000 children, 40,000 live rows, 40 durable
batches of at most 256 sources, 41 text generations, 0.491 s source/chunk wall,
84,279,296-byte peak RSS, 3,565,309 cumulative allocations, and 39,846,262
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
checkpoint/close/reopen-validation score-only zero-fetch evidence. The v5
validator pins each deterministic mode/scale probe result count and digest.
Generated DB directories were deleted after copying only raw rows; no
transient DBs are retained.

## Measured revision

The measurements were produced at the clean, immutable commit
[`10c0b4c55348775103a9727c104dea1103e0f944`](https://github.com/snissn/gomap/commit/10c0b4c55348775103a9727c104dea1103e0f944),
root tree `a96172f6a7b385cfcce46be935208c7ae2b8bf4a`, TreeDB subtree
`4928b649dae066cb24d648b43930e0a1c5fb6334`, and qualification-harness
subtree `4168d88634950c078af501898a15c0b0a0d51fdf`. Its
`TreeDB/collections/document_chunking.go` blob is
`44295fa09e0e78eccb76a8ba327aa87831978a6d`. `SHA256SUMS` binds the final
retained manifest, report, and every raw row.

JSON shape validation alone does not prove those Git relationships. The CLI
must run inside the candidate repository checkout with the measured objects
available locally. It resolves the measured commit to the manifest root tree,
TreeDB subtree, qualification-harness subtree, and implementation blob. It
independently requires the candidate `HEAD` TreeDB and qualification-harness
subtrees to equal the measured subtrees. Artifact-only descendants may differ;
any runtime or harness drift, missing Git object, or unavailable Git object
database fails closed.

From the candidate repository root, validate the retained artifact with:

```sh
go run ./cmd/treedb_text_ingest_qual \
  -manifest artifacts/4328-text-ingestion-qualification/manifest.json \
  -report artifacts/4328-text-ingestion-qualification/report.json
```
