# Ordinary dense vector search v5

Ordinary v5 uses the maintained public document service search and document
materialization path. The response contains exactly the existing document ID,
document, and dense response sections. Metadata is:

```
route_tag: byte
candidates, exact_fallbacks, full_document_scan_fallbacks: uvarint
column_graph_prepared_search, search_pack_search: uvarint
column_graph_delta_scored, documents_scanned: uvarint
result_count: uvarint
scores: result_count little-endian float64 values
```

The route tags are explicitly reserved as follows:

| Tag | Route proof |
| --- | --- |
| 4 | ANN with exactly one of prepared=1 or search_pack=1 |
| 5 | Intentional exact search; prepared, search_pack, and delta are zero |
| 6 | ANN ColumnGraph compatibility traversal; prepared and search_pack are zero |

Tag 6 proves `ColumnGraphFallbackSearch=1` in the current client response. The
service exposes the same counter as `column_graph_fallback_search`. Compatibility
means graph traversal through sources available without mmap; it does not mean
exact search or a full document scan. Every ANN response requires exactly one of
prepared, search pack, or compatibility. Counters with multiple route flags,
non-unit route values, or ANN document scan work fail closed. All routes require
zero exact-fallback and full-document-scan-fallback counters. Intentional exact
search reports actual documents scanned and may not scan fewer documents than
its candidate count.

Tags 4 and 5 retain their existing frame layout and meaning. Tag 6 adds no
positional counter and does not reuse retired runtime tag 1. Existing pre-alpha
v5 clients reject unknown tag 6; they cannot silently relabel compatibility work.
Typed versions 2, 3, and 4 keep their independent admission and proof contracts.

`TestDenseVectorSearchOrdinaryNativeDefaultParity` covers the actual native owner,
service, and wire path, including compatibility traversal on Windows. Linux
controls separately cover a real collection's public bound searcher forced into
non-mmap compatibility using the existing reader seam, service counter mapping,
and hostile synthetic wire frames. Those separate controls are not Linux
end-to-end emulation of Windows.
