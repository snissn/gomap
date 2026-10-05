# 10M AFTER-only publication audit

Measured runtime: `87eb344543709e7752c5f5c222f8a7d42f330dac`. Source-equivalent current revision supplied by coordinator: `4163c789551ef142f9ad27b73f639bcc02ca0f66`. Object diff over `TreeDB` and `cmd/unified_bench` finds only `cmd/unified_bench/README.md`; inspected production TreeDB objects are identical. All source references below are to the measured SHA, not the potentially stale primary checkout. Scope: read-only source/artifact inspection; no benchmark, test, production change, GitHub mutation, or new production graph node.

**Finding:** repeated whole-tree candidate resource projection on ordinary overwrite/delete is the strongest remaining algorithmic seam. The counters prove 160.282 seconds inside successful guarded publication; they do not measure how much belongs to the scanner. The scanner is a code-backed hypothesis, not a function-level attribution or a proven regression.

## Retained measurements

Input: `final-remaining30/1-treedb-durable-holdout-s173-10m/stdout.json`, row 0, phase `quicksilver_concurrent`, exact before/after counter deltas. `run.json` reports exit 0 and validated=true; stdout reports profiled=false.

| Measurement | Observed |
|---|---:|
| Load | 137.327978 s |
| Concurrent reader interval / composition | 8.001105 s / 166.218172 s |
| Concurrent reads | 3,835,904; 479,421.8 reads/s over reader interval |
| Concurrent p50 / p99 | 7.972 / 15.991 us |
| Four concurrent checkpoints | 116759.559, 21416.847, 11198.682, 11117.350 ms |
| Final checkpoint | 10852.599 ms |
| `treedb.flush_apply.publish_final_install.ns_total` | 0 -> 160281838047 (16 calls) |
| `treedb.flush_apply.apply_ns_total` | 0 -> 864709430 |
| `treedb.flush_apply.publish_prepare.ns_total` | 0 -> 9054105 |
| `treedb.flush_apply.root_reduce.ns_total` | 0 -> 4652990 |
| `treedb.flush_apply.leaf_log_output.append_wait_ns_total` | 0 -> 723458659 |
| `treedb.cache.checkpoint.active_background_flush_wait_ns_total` | 0 -> 143627350117 |
| `treedb.public.checkpoint.ns_total` | 0 -> 160492430690 |
| `treedb.cache.flush_apply.backend_batch_write_ns_total` | 0 -> 161429236129 |
| `treedb.durable_root.manifest_build.nanos` | 0 -> 2828542 |
| `treedb.vlog.mmap_read.fallback_readat` | 19714736 -> 70074422 |
| Background vacuum total-time delta | 4.885285 s |

These timers overlap and must not be summed. `publish_final_install` is an alias for successful guarded-publication duration (`TreeDB/db/flush_apply_stats.go:319-328`). Optimistic ordinary batch timing begins after commitMu acquisition at `batch.go:539`, includes command-WAL append and `finalizeCommitLockedWithOptions`, and ends at :615; the serialized sibling measures :800-830. Thus the counter can include builder acquisition, durablePublishMu waiting, dependency capture, candidate building, activation/admission, and applicable durability waits. It is not the time of a small final installation function. Initial/final reopen and the four concurrent barriers are distinct costs. The 8-second rate is not end-to-end concurrent completion throughput.

## Exact path and complexity

1. Both ordinary batch callers build the same apply-local reference delta (`batch.go:453`, :763); the build-group sibling does likewise (`root_publication_build_group.go:259`). Ordered-root callers use `buildValueLogRefDeltaWithOptions` (`ordered_root_publish.go:1537`, :1632). All production callers were located with exact-SHA git grep.
2. `OldEntriesRemoved` explicitly counts overwrites as well as point/range deletes (`TreeDB/zipper/read_only_prepare.go:89-97`). `vlog_gc_incremental.go:757` sets `requiresCandidateProjection` when this count is nonzero with outer leaves. Ordinary delta builder captures producer segment identities at :724-731, so O3 already fixes additive/current-segment ownership; it does not eliminate this deliberate destructive fallback.
3. `planOuterLeafBaseDependencyReuseV1` immediately declines at `durable_root_runtime.go:687-689` for that flag. Its only production caller is the common closure builder at :869. If reuse declines, closure capture routes at :995 to `captureDurableValueLogResourcesWithLimitsV1`; logical-count projection also always declines for outer-leaf mode at :286-294. Candidate scanner is then selected at :647-648.
4. The scanner (`durable_root_runtime.go:477-561`) walks primary outer-leaf references (:521), projects primary-root values (:528), discovers roots from system descriptors (:531), walks outer leaves for discovered roots (:539), projects those values (:542), and runs a third maintenance projection to retain final exact counts (:545). The user root is included in both primary and discovered root sets (`maintenance_roots.go:113`), so a destructive fallback repeats user-tree value projection three times. Registration/rebinding boundaries exist to make newly exposed resources readable; they cannot simply be removed without preserving that invariant.
5. Each maintenance projection creates a new page memo (`maintenance_reachability.go:197-198`); memo reuse covers roots inside one call (:353-359), not these three calls. It reads outer-leaf pages (:341-348), checks integrity (:325-337), and examines every leaf value's pointer metadata (:213-219), without reading every pointed-to value payload. Complexity is whole candidate index/leaf content per fallback, rather than changed keys only.
6. Active queued publication captures this closure before activation (`root_publication_activation.go:765-773`); finalizer chooses the runtime branch at `db.go:3441-3447`. The direct durable-root branch also uses the common capture builder (:3462). The scanner wrappers are also used by rebuilt-index resource capture (`durable_root_runtime.go:1282`), so optimizing the shared scanner needs replacement-index and recovery coverage too.
7. The ordered multi-root path has an existing exception retaining predecessor raw-leaf dependencies while applying exact logical removals (`ordered_root_publish.go:1546-1561`). Ordinary DB-root publication intentionally lacks that exception for leaf-generation GC. Existing tests require destructive exact projection and verify the tracker, retained snapshots, checkpoint/reopen (`root_publication_activation_test.go:1055-1095`). Do not clear the ordinary flag merely because the ordered path does.

## What remains unproven

No CPU/alloc/block/function profile was captured in this cell, and the artifact has no observed candidate-scan count or fresh-capture timing. Therefore 16 successful publishes do not prove 16 destructive scans. The large ReadAt delta is consistent with repeated leaf projections but also contains other reads/maintenance; it is not scanner-exclusive. Guarded timing also contains admission/lock waits. Fine candidate timing already exists in `CommandWALPublishTiming` and in `root_publication_activation.go:867-925`, but this artifact does not expose it for these physical flushes. Manifest build (2.829 ms), root reduction (4.653 ms), prepare (9.054 ms), and append wait (723.459 ms) are individually too small to explain the 160-second guarded boundary. This does not exclude uninstrumented stable I/O or queue waiting.

## Minimal follow-up options, ranked

1. First measure candidate fresh capture / full-scan calls, visited pages, and guarded admission/lock waits using existing timing/counters or one focused profile. Reuse the existing `testScanCandidateExternalReferencesHook` for correctness checks. A same-cell bounded repeat must distinguish whole-tree projection from waiting before any production optimization is claimed.
2. If confirmed, reduce repeated projection within `scanCandidateExternalReferencesWithCountsAndLimitsV1`: carry the already computed primary counts, scan only newly discovered roots, and reuse the final result when no new roots appear. Preserve producer registration, set rebinding, descriptor discovery, cross-root deduplication/count semantics, recovery, integrity verification, and final exact tracker evidence. This keeps exact reachability and avoids inventing a global cache or changing the on-disk contract. Expected benefit is fewer whole-tree passes; no speedup is measured yet.
3. Only if one exact pass remains dominant, extend apply-fed evidence to exact raw outer-leaf membership/count deltas so ordinary destructive publication can project the changed frontier. Reuse existing reference-delta/tracker structures where appropriate, keeping physical leaf membership separate from logical ValuePtr counts. This is a broader correctness change requiring overwrite/delete/range, rotation, snapshot, recovery, and GC proof; predecessor supersets alone can pin stale leaf segments indefinitely. Do not add this as a graph node without separate scope authorization.

Increasing cache/mmap budgets may reduce I/O inside the scan but does not remove repeated O(database size) work. O2 value-frame grouping and O1 owned Snapshot.Get are different seams from the resource scanner's leaf/pointer metadata walks; no claim is made that their measured gains disappear.

## Capacity and storage interpretation

This is an AFTER-only 10M holdout seed-173 capacity result: ordinary commits, four reader workers, config updates=40,000 and updated_keys=40,000; mutation fields report updates/deletes/inserts/overwrite_targets=10,000 each and overwrite_sets=40,000. Generic seeded values are 95% small, 4% medium, 1% large; observed key bytes 453,780,812 and value bytes 2,578,106,427. Full verification reports 10,000,000 final values and 20,110,000 final misses after durable checkpoint/reopen. Holdout miss target is 70%, working set 20%. This supports completion/correctness on this cell, not a BEFORE/AFTER regression claim, a Pareto win, or a waived promotion gate.

Observed file inventory total: initial 13,454,141,090 bytes (12.53 GiB), final 12,655,826,985 bytes (11.79 GiB). Initial/final aggregate index files: 1,209,270,272 / 320,077,824 bytes; value_vlog files: 1,489,703,362 / 1,498,525,114 bytes; leaf_vlog files including manifests: 10,755,166,524 / 10,837,169,485 bytes. `maindb/wal` is only 12 bytes at both snapshots; subtracting that directory yields 13,454,141,078 / 12,655,826,973 bytes. These are file-inventory sizes, not exact live-data bytes or actual allocated disk blocks. Leaf-log footprint dominates, with historical COW data/retained generations potentially present; no measured retention attribution or compaction benefit is established. Index shrink and vacuum counters prevent interpreting total shrink as O2 compression benefit.

## SHA256 retained inputs

Hashes cover the exact complete input files and Git-object content used for the central audit; line references use the measured revision.

- `tmp/quicksilver-authoritative-20261003/final-remaining30/1-treedb-durable-holdout-s173-10m/stdout.json`: `d16cdb999cc353675c940bd29628f828be3b1168c5bd26aaf6a381bc04166a89`
- `tmp/quicksilver-authoritative-20261003/final-remaining30/1-treedb-durable-holdout-s173-10m/run.json`: `d1bf075e0fd34c2ed5d9c626d0dca3b59615f0442ff0ca535e678ad964adf546`
- `tmp/quicksilver-authoritative-20261003/final-remaining30/1-treedb-durable-holdout-s173-10m/stderr.log`: `f69bdcd400b2da9aec5766e542ba32ab8041ceb88910250127957af02d9bed22`
- `87eb344543709e7752c5f5c222f8a7d42f330dac:TreeDB/db/batch.go`: `a296efba982140b2b6a46db638ecd55e64e970cfba0cc76ca910cfda2679041a`
- `87eb344543709e7752c5f5c222f8a7d42f330dac:TreeDB/db/db.go`: `b4edf9c3cdbea44b84e4b0571242b0fb1f2563ad50b24f41703dcadf70033b3f`
- `87eb344543709e7752c5f5c222f8a7d42f330dac:TreeDB/db/flush_apply_stats.go`: `98e8e2a5a910e4730a791a32dbf6a3d201d1e1ac9fb7aacaa0bd34d834bb9dc9`
- `87eb344543709e7752c5f5c222f8a7d42f330dac:TreeDB/db/durable_root_runtime.go`: `9a9c5df95027bf0a4ecb46d47ab2d862e64384d83db4fea709d4650ffaa7db1d`
- `87eb344543709e7752c5f5c222f8a7d42f330dac:TreeDB/db/root_publication_activation.go`: `29dd55332c6dba74ff7f89837b382034ad2040d62749fcc1116f040660f16132`
- `87eb344543709e7752c5f5c222f8a7d42f330dac:TreeDB/db/vlog_gc_incremental.go`: `42fff00679e31e8a9021eb4d232087965712eec219b230283b3433c1c813acca`
- `87eb344543709e7752c5f5c222f8a7d42f330dac:TreeDB/db/maintenance_reachability.go`: `b16ad78689082140793052a0826dfcdaa8bbaaa25cb78326a62b596bf9f87c84`
- `87eb344543709e7752c5f5c222f8a7d42f330dac:TreeDB/db/maintenance_roots.go`: `5baba2e5accfde6dd9695cadc98f3efede282753d56c22040e02bc74cd6f4532`
- `87eb344543709e7752c5f5c222f8a7d42f330dac:TreeDB/db/ordered_root_publish.go`: `d98eb43203903cb6c72ac9f15c32bf7c3bd0fac153b115db24ddf6c5b0f4cef8`
- `87eb344543709e7752c5f5c222f8a7d42f330dac:TreeDB/db/root_publication_build_group.go`: `1df5987797ee7e4939991b1485d0570e7e77c3f09d7bfb7ced2894ba6b2ade38`
- `87eb344543709e7752c5f5c222f8a7d42f330dac:TreeDB/db/root_publication_activation_test.go`: `b35e5854f0f592eb8a66225b56e6c3061cf6b66b18a42aec4fb4ebe952b11137`
- `87eb344543709e7752c5f5c222f8a7d42f330dac:TreeDB/zipper/read_only_prepare.go`: `e3fa4254b0ea525e0567aa13f6a5a5349ecdfd905f1dd4a5dffe972f4cd63502`
