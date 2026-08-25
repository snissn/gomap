# #4328 pure-text qualification evidence

Status: the strict full 10k/100k/1M matrix is retained in `manifest.json` and
`report.json`; the validator accepts it. The pre-fix public
`IngestChunkedDocument` baseline remains under `smoke-10k-r3/`.

The post-fix public `IngestChunkedDocuments` 10k fixture is
`smoke-10k-r4/`: 10,000 parents, 30,000 children, 40,000 live rows, 40 durable
batches of at most 256 sources, 41 text generations, 0.433 s source/chunk wall,
111,738,880-byte peak RSS, and 40,108,405 WAL-excluded physical bytes. Against
the frozen baseline (20,001 generations, 155.567 s, 4,180,574,208-byte RSS,
5,621,678,453-byte physical), it meets the generation, sub-1-GiB physical, and
>=2x throughput gates without a correctness/RSS/allocation/storage regression.

`smoke-100k-r{1,2,3}/` and `smoke-1m-r{1,2,3}/` retain all four raw modes for
the required repetitions. Every source row records actual batch size/count and
checkpoint/close/reopen score-only zero-fetch evidence. Generated DB directories
were deleted after copying only raw rows; no transient DBs are retained.

Validate each retained artifact with:

```sh
go run ./cmd/treedb_text_ingest_qual -manifest manifest.json -report report.json
```
