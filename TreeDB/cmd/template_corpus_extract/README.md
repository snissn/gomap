# template_corpus_extract

Extracts stable binary corpora for template-compression experiments from a TreeDB directory.

## Output files

- `pointer_values.bin`: pointer-backed value payload corpus
- `outer_leaf_pages.bin`: outer-leaf page payload corpus
- `manifest.json`: extraction metadata and counts

Record format for both corpus files:

- `u32le length` + `raw payload bytes`

## Example

```bash
go run ./TreeDB/cmd/template_corpus_extract \
  -app-dir /path/to/application.db \
  -out-dir /tmp/template_corpus_fast \
  -pointer-limit 200000 \
  -outer-leaf-limit 200000 \
  -pointer-stride 1 \
  -outer-leaf-stride 1 \
  -overwrite
```

## Notes

- `-app-dir` should be a TreeDB root directory (contains `maindb/`, optional `dictdb/`, `templatedb/`).
- `pointer_values.bin` may be empty if the input database has no pointer-backed values.
- `outer_leaf_pages.bin` is deduplicated by value-log `(fileID, offset)` to avoid duplicate leafref payload entries.
