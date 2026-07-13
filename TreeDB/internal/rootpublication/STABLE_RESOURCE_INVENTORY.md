# Stable resource producer inventory

This reviewed inventory is the #3677 registration contract. A row is complete
only when its producer, identity/frontier, registrar, recovery validator, and
deleting owner are named. The implementation remains dormant until #3679 and
#3718 consume `StableResourceToken` at their respective activation boundaries.

| Root/catalog/frame field | Producer | Kind | Stable identity and frontier/digest | Namespace operation | Registrar | Recovery validator | Deleting owner |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `ValuePtr.FileID` | `caching/value_log_appender.go:CaptureValueLogStableSnapshot` via `db/value_log_appender.go:CaptureValueLogStableResourceToken` | value_log | duplicated writer descriptor with exact platform file identity + file ID generation; exact appended byte frontier | none for existing-file appends; newly created relaxed-rotation segments carry a captured parent-directory obligation and fail closed when unsupported | `db.ValueLogStableResourceProvider` | `db.ValidateValueLogStableResource` | `db.ValueLogStableDeletionOwner`, consulted by `db/vlog_gc.go` / rewrite |
| raw leaf root generation | `db/leaf_page_log.go:AppendLeafPage` | outer_leaf | generation ID + sealed digest | lane create/rotate | leaf builder | `db/leaf_generation_manifest.go` | `db/leaf_generation_gc.go` |
| packed leaf generation | `db/leaf_generation_pack.go` | outer_leaf | generation ID + sealed digest | pack/create/rename | pack writer | `db/leaf_generation_manifest.go` | `db/leaf_generation_gc.go` |
| leaf manifest | leaf generation pack writer | outer_leaf | manifest generation + digest | rename into leaf namespace | pack writer | manifest loader | leaf generation GC |
| collection dictionary | `collections/template_v1.go` | dictionary | catalog generation + digest | catalog create/rename | collection root builder | `collections/template_v1.go` | `db/vacuum_collection_roots.go` |
| collection catalog/template | `collections` catalog builders | template | catalog generation + digest | catalog create/rename | collection root builder | catalog decode | collection vacuum |
| column part | `collections` column writer | column | asset ref + content digest | asset create/rename | collection root builder | column asset loader | column asset lifecycle |
| typed-column parts/value/code | `internal/typedcolumn/part.go`, collections adapters | typed_column | part/section ID + digest | part create/rename | collection root builder | `internal/typedcolumn/part_image_decode.go` | `collections/column_store_compaction.go` |
| HNSW/vector packs | `collections/column_vector_graph_manifest.go` | vector | pack ID + digest | pack create/rename | collection root builder | `collections/column_vector_graph_manifest.go` | `collections/column_store_compaction.go` |
| text dictionaries/postings/positions | `collections/text_v2_storage.go` | text | catalog/asset ID + digest | text asset create/rename | collection root builder | text catalog validation | text maintenance |
| command-WAL segment/external RID | `db/command_wal_raw.go` | command_wal | segment generation/RID + durable LSN | WAL segment create/rotate | command-WAL builder | `db/command_wal_v2_recovery.go` | `db/command_wal_raw.go` |
| query-ready assets | `internal/typedcolumn/query_ready_delta.go` | query_ready | asset ID + digest | query-ready create/rename | collection root builder | `internal/typedcolumn/query_ready_delta.go` | `collections/column_store_compaction.go` |

All paths in this table are diagnostic only; token identity and pinned callbacks
are the durable authority. Adding an authoritative field requires adding its
row and a registration test before activation.
