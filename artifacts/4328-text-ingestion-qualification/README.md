# #4328 pure-text qualification evidence

Status: corrected-workflow 10k smoke is retained under `smoke-10k-r3/`; no
full 100k/1M qualification matrix is accepted or claimed here.

Retain compact `manifest.json`, `report.json`, validation output, and a decision
report for a completed full 10k/100k/1M matrix. The four raw 10k smoke rows are
also retained because they evidence the actual public lifecycle. Keep generated
databases, profiles, and partial/failed large-run payloads outside the
repository, while recording their paths and failure status in the compact
report.

Validate each retained artifact with:

```sh
go run ./cmd/treedb_text_ingest_qual -manifest manifest.json -report report.json
```
