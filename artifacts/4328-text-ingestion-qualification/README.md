# #4328 pure-text qualification evidence

Status: the pre-fix `IngestChunkedDocument` baseline remains under
`smoke-10k-r3/`. The post-fix public `IngestChunkedDocuments` 10k smoke is
retained under `smoke-10k-r4/`; it used 40 durable batches of at most 256
sources and measured 41 text generations, 0.433 s source/chunk wall time,
111,738,880-byte peak RSS, and 40,108,405 WAL-excluded physical bytes for
10,000 parents, 30,000 children, and 40,000 live rows. No full 100k/1M
qualification matrix is accepted or claimed here.

Retain compact `manifest.json`, `report.json`, validation output, and a decision
report for a completed full 10k/100k/1M matrix. The four raw 10k smoke rows are
also retained because they evidence the actual public lifecycle. `smoke-10k-r4`
records the actual source chunk batch size/count in its source row. Keep generated
databases, profiles, and partial/failed large-run payloads outside the
repository, while recording their paths and failure status in the compact
report.

Validate each retained artifact with:

```sh
go run ./cmd/treedb_text_ingest_qual -manifest manifest.json -report report.json
```
