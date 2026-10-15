# Shared Set and published current-root disposition

Starting HEAD a8c52ed66fc587d4823f020cf15ea1c3ee8c1ebc. This is a source audit, not exact-head hosted acceptance. `all-set-uses.txt` is the complete production ValueLogSet search captured for this audit; `callsite-search.txt` retains the broader search. Final source identity is in freeze.json.

The retained unit remains the SAME existing valuelog.Set. A published state owns one reference. Every pointer-bearing current-state consumer captures and acquires that exact reference while rootReuseMu and db.mu exclude replacement, then releases it after its actual final use. Snapshot-backed consumers retain their SAME captured state and index generation. No second publisher, new quota, or replacement token is introduced. Topology copies carry the SAME primaryRoot current-root edge; they do not acquire or drop it.

## Every production state.Store

| Store / containing path | Exclusion and original owner disposition |
|---|---|
| root_publication_activation.go / queued prepared publication | Existing finalizeQueued publication rootReuse gate plus db.mu; new actual logical root owns its original current-root edge. Old publication Set goes to original postwork. |
| db.go / legacy finalizeCommit | Existing rootReuse gate from finalize through publishSnapshotView; legacy DATA path with no PRIMARY runtime. Original postwork releases old publication Set. PRIMARY uses the queued prepared path. |
| db.go / RefreshValueLogSet | Existing rootReuse then db.mu; takes fresh manager Set; copies SAME primaryRoot and releases old Set. |
| db.go / publishValueLogSetNoRefresh | Existing rootReuse then db.mu; SAME primaryRoot preserved; original old Set release. |
| vacuum_online.go / actual index swap publication | ADDED rootReuse then db.mu around state/index publication and publishSnapshotView. Actual replacement PRIMARY root comes from original replacement runtime. Old Set and old index generation retain original disposal paths. |
| compact_storage_audit.go / publishCompactStorageValueLogSet | ADDED rootReuse then db.mu, consumes original retained manager Set. Copies SAME primaryRoot. Exact old publication Set release. |
| leaf_generation_gc.go / publishLeafGenerationState | ADDED rootReuse then db.mu. SAME primaryRoot. Both same-Set Acquire/Release and refreshed-Set replacement preserve original reference balance. |
| open_readonly.go / two initial stores | Private construction before the DB escapes. Each initial Set obtained by CurrentSetNoRefresh, owned by original DB state. No concurrent snapshot capture. |

There are nine actual state.Store sites (the final row contains two). Open's later private initial replacement uses publishSnapshotView before Open returns; its old initial Set is released before escape. Read-only publishers, queued and legacy finalizers, refresh, audit, leaf topology, and vacuum were inspected together. rootReuse is acquired before db.mu everywhere added. Leaf topology callers release db.mu before entering this helper; no recursive rootReuse acquisition was added. Queued/legacy postwork enters leaf reconciliation after its publication gate is released.

## Pointer-bearing current-state consumers

| Consumer family | Exact lifetime and same-capture rule |
|---|---|
| vlog_rewrite.go / optimistic user application | MOVED exact Set acquire inside existing rootReuse + db.mu capture, before either unlock. Holds original index registry entry and writeMu read exclusion; joined original Set Release after actual tree/zipper use. |
| vlog_rewrite.go / serialized user application | Same move under existing capture gate; writeMu exclusive and original registry entry remain; joined exact release. |
| vlog_rewrite.go / system and collection applications | ADDED rootReuse + db.mu capture and exact Set acquire; writeMu excludes index replacement. Retain through descriptor/tree/zipper work and final publication; join exact release. |
| db.go / CompactIndex | ADDED exact capture pin under rootReuse + db.mu before collect/build. Existing writeMu and teardown exclusion protect selected pager. Join original release after final use. |
| ordered_root_publish.go / publishOrderedRootIterator | ADDED exact Set pin under rootReuse + db.mu; callers already hold writer/index lifetime. Reader and base tree use selected Set through whole merge/build, joined release. |
| compact_storage_audit.go / prepareCompactStorageAuditTopology | ADDED exact current Set pin before unlock; independent managerSet already owns its own reference. Comparisons and possible topology publish finish before joined current Set release. |
| vlog_rewrite.go / online dictionary and training | All three online callers now call prepareCurrentRewriteLeafDict: actual Snapshot, its SAME state + idx.pager, joined actual Close. No fallback to shallow State on failed capture. Train/scan reads selected Set, never manager's later current Set. |
| compact_storage.go / carried plan | Actual Snapshot supplies SAME live state/Set for sizing and plan construction; actual Close returned even on quick path. Invalid carry closes snapshot before fallback. Nil capture returns ErrClosed. |
| compact_storage.go / carry.advance | Actual Snapshot validated against immutable/scalar published basis before use. SAME live state supplies generation view and all Set file sizing. Actual Close joined, nil capture ErrClosed, mismatch invalidates carry. Returned pack's publishedState is only identity/scalar/immutable basis. |

## Already-owned, private, and scalar-only callsites

| Production paths | Proven containing lifetime / boundary |
|---|---|
| db.go ordinary/bounded capture; native_mvcc_snapshot_v6.go | Existing rootReuse capture fence remains through SAME Set acquisition. Empty native Set always pins nonnull Set. Ordinary known-empty private one-shot uses nil reader and may keep only its legacy shallow DBState wrapper alias; no mutable Set backing use. |
| db.go Snapshot.Close, native_mvcc_snapshot_close_v6.go | Release original snapshot Set only after actual final consumer; native continuation and sticky failure remain original owner. |
| bounded_read_owner.go; stable_resource.go | Called with live containing Snapshot/resource capture; SAME snapshot.state Set. No shallow current-state fallback. |
| api.go Stats, fragmentation.go | Acquire actual Snapshot before reading state/Set, retain until final iteration/metrics use. |
| leaf_generation_plan.go plan builder, record-index and source scan helpers | Called under actual plan Snapshot/sourceScan.snap. Source file reads and record lengths use its SAME Set. Both carried plan readers repaired above. |
| maintenance_reachability.go; leaf_generation_maintenance_limits.go | Actual caller Snapshot retained through all pointer/record-length reads. |
| compact_storage_audit.go full audit / protected basis | in.snap owns the exact Set. Audit key ValueLogSetIdentity comparisons are pointer identity only; they never dereference retired Set backing. |
| compact_storage.go indexVacuumLeafGuard | Set map use occurs while guard's explicit Snapshot pin is still live. Early release is after final Set use; later guard actions use separate retained leaf-generation pins. |
| compact_storage.go relocation, audit and ignored-file input | in.snap owns exact Set throughout actual final read. |
| vlog_rewrite_chunk_plan.go, leaf_generation_pack_rewrite.go | Actual scan Snapshot/sourceScan.snap covers all Set pointer/file/record reads. leafGenerationPackCopyBasis uses scalar roots/sequence plus immutable leaf topology only. |
| value_reader.go / ValueReaderForState | Explicit borrowed boundary documented. Internal dictionary inputs now actual owned Snapshot. internal/dictdb resource capture uses SAME live snapshot.State; ownership is either local snapshot or transferred to retained builder/index token. Root audited caching and cmd callers under live Snapshot. |
| vacuum_offline.go and offline vlog_rewrite.go | Exclusive offline DB owns state; exact original Set acquired before physical scan/close and released through original receipt. No concurrent exposed capture exists. Dictionary input uses this SAME offline pager and owned state. |
| durable_root_runtime.go candidate creation/rebinding/closure scan | Fresh CurrentSetNoRefresh-derived original reference belongs to private candidate Snapshot. Rebinding transfers that candidate's original ref and releases original old ref. No exposed state.Store. |
| prepared_root_publication_profile.go | Returns owned original fresh Set to original installation/candidate owner; bounded admission remains unchanged. |
| wal_recovery.go temporary scan DBState | Private constructed state obtains fresh CurrentSetNoRefresh reference; original release after scan. |
| db.go Open initial state / Close terminal state | Initial state never escapes before ownership installed. Terminal state Set released by original teardown after containing ownership checks; no new completion inference. |
| command_wal_raw.go db.State | Reads scalar AppliedCommandLSN only. |
| leaf_generation_pack_rewrite.go db.State | Reads scalar roots/sequence and immutable LeafGenerations only; no mutable Set dereference. |
| vlog_gc.go, incremental GC, leaf-page-log, refresh helpers | Call existing owning refresh/publication operations; comments or method names in search, no extra unowned Set consumer. |
| DBState construction and current-root copies | Initial two read-only states, private WAL/candidate/rebound scan states, original commit/vacuum states, refresh/no-refresh copies, audit and leaf copies all audited. New topology copies preserve SAME primaryRoot; new logical publications keep original allocation path. |

## Actual controls

TestCurrentStateValueLogConsumersRetainExactSet reaches ten actual consumer seams, selects a current Set distinct from a still-held older scan, and interleaves real RefreshValueLogSet after capture. Its selected Set has exactly two refs at capture, one after refresh, nonempty real outer-leaf backing and successful physical leaf reads; actual consumer return is last release and scrubs it. Older scan remains independently readable.

TestTopologyPublishersWaitForSnapshotSetCapture pauses actual admission capture before exact Set acquisition, runs each actual audit / same-Set leaf / fresh-Set leaf publisher, proves exclusion, resumes capture and reads actual persistent pointer values across publisher completion. Audit uses a distinct new registered generation so it must really replace state. Last captured consumer drains retired Set backing.

TestTopologyPublicationPreservesPrimaryRootLifetime performs all three real topology publications, proves SAME original current-root edge and admitted original metadata owner, advances both slots plus parents with six actual writes, reads retained iterator, verifies Snapshot.Close invalidation while ownership/charge stay live, then actual iterator.Close, root retirement and final DB/governor metadata drain.
