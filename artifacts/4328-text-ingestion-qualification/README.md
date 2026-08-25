# #4328 pure-text qualification evidence

Status: the strict full 10k/100k/1M matrix is retained in `manifest.json` and
`report.json`; the validator accepts it. The pre-fix public
`IngestChunkedDocument` baseline remains under `smoke-10k-r3/`.

The post-fix public `IngestChunkedDocuments` 10k fixture is
`smoke-10k-r5/`: 10,000 parents, 30,000 children, 40,000 live rows, 40 durable
batches of at most 256 sources, 41 text generations, 0.471 s source/chunk wall,
82,411,520-byte peak RSS, 3,617,300 cumulative allocations, and 40,370,550
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
through chunk ingestion. Every source row records
actual batch size/count and checkpoint/close/reopen score-only zero-fetch
evidence. Generated DB directories were deleted after copying only raw rows; no
transient DBs are retained.

## Measured revision

The measurements were produced at the clean, immutable commit
[`0679eb28135a731d9699311b36364d1c9b27db71`](https://github.com/snissn/gomap/commit/0679eb28135a731d9699311b36364d1c9b27db71),
tree `876ea6c3f9e213cf178448cd242212e208d5c7e2`. Its production
`TreeDB/collections/document_chunking.go` blob is
`76697734b60a5086d7d186088c0321a510b4909c`, identical to the reviewed
candidate. Later commits only strengthened the evidence validator and added
derived fixture identities to the retained JSON; they did not alter measured
values or relabel the generating commit. `SHA256SUMS` binds the final retained
manifest, report, and raw rows.

JSON shape validation alone does not prove those Git relationships. The CLI
must run inside the candidate repository checkout with the measured objects
available locally. It resolves the measured commit to the manifest tree, the
tree/path to the manifest blob, and independently requires the current
`HEAD:TreeDB/collections/document_chunking.go` blob to equal the measured blob.
Missing Git objects, an unavailable Git object database, or any mismatch fails
closed.

From the candidate repository root, validate the retained artifact with:

```sh
go run ./cmd/treedb_text_ingest_qual \
  -manifest artifacts/4328-text-ingestion-qualification/manifest.json \
  -report artifacts/4328-text-ingestion-qualification/report.json
```
