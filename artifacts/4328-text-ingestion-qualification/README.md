# #4328 pure-text qualification evidence

Status: M0 validator only; no scale row is accepted or claimed here yet.

Retain only compact `manifest.json`, `report.json`, validation output, and the
decision report for completed 10k/100k/1M runs. Keep generated databases,
profiles, and partial/failed large-run payloads outside the repository, while
recording their paths and failure status in the compact report.

Validate each retained artifact with:

```sh
go run ./cmd/treedb_text_ingest_qual -manifest manifest.json -report report.json
```
