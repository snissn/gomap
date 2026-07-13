# Stable resource producer inventory

This reviewed inventory is the #3677 registration contract. A row is complete
only when its producer, identity/frontier, registrar, recovery validator, and
deleting owner are named. The implementation remains dormant until #3679 and
#3718 consume `StableResourceToken` at their respective activation boundaries.

| Root/catalog/frame field | Producer | Kind | Stable identity and frontier/digest | Namespace operation | Registrar | Recovery validator | Deleting owner |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `ValuePtr.FileID` | `db/value_log.go` append and rotate | value_log | file ID + generation; appended byte frontier | value-log lane create/rotate | value-log producer | value-log open/replay | `ValueLogGC` / rewrite |
| leaf root generation | `db/leaf_page_log.go`, pack rewrite | outer_leaf | generation ID + sealed digest | pack/create/rename | leaf builder | leaf generation open | leaf generation GC |
| leaf manifest | leaf generation pack writer | outer_leaf | manifest generation + digest | rename into leaf namespace | pack writer | manifest loader | leaf generation GC |
| collection catalog/template | `collections` catalog builders | template | catalog generation + digest | catalog create/rename | collection root builder | catalog decode | collection vacuum |
| column part | `collections` column writer | column | asset ref + content digest | asset create/rename | collection root builder | column asset loader | column asset lifecycle |
| typed-column parts/value/code | `internal/typedcolumn`, collections adapters | typed_column | part/section ID + digest | part create/rename | collection root builder | typed-column image validation | column asset lifecycle |
| HNSW/vector packs | `collections` vector graph builders | vector | pack ID + digest | pack create/rename | collection root builder | vector pack validation | vector maintenance |
| text dictionaries/postings/positions | `collections/text_v2_storage.go` | text | catalog/asset ID + digest | text asset create/rename | collection root builder | text catalog validation | text maintenance |
| command-WAL segment/external RID | `db/command_wal_raw.go` | command_wal | segment generation/RID + durable LSN | WAL segment create/rotate | command-WAL builder | command-WAL replay | WAL retention |
| query-ready assets | #3694 query-ready producer | query_ready | asset ID + digest | query-ready create/rename | collection root builder | query-ready validation | query-ready lifecycle |

All paths in this table are diagnostic only; token identity and pinned callbacks
are the durable authority. Adding an authoritative field requires adding its
row and a registration test before activation.
