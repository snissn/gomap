# Isolated captured-root GE evidence prototype

**Disposition: NO-GO for production activation.** Both-size public allocation RED→GREEN and focused risk checks pass, but the authorized comparison is incomplete: eight small-fixture invocations passed and the first multilevel baseline invocation failed during public Close. Driver stopped; no candidate repair or additional collection followed.

This is one nonmergeable #4876 reassessment prototype. Neither production M5 activation nor native/PRIMARY whole-product acceptance follows. Existing graph, merged #4878 prerequisite and final gates remain for root disposition.

## Source and mechanism

Six production files differ from the coherent imported baseline: caching/cow_read.go, caching/cow_read_workspace.go, db/bounded_read_owner.go, tree/tree.go, tree/primary_directory.go and new tree/owned_successor.go. Two public harness files and one new tree test also differ. changed-source.json gives exact before/after SHA256; frozen-source-census.json and baseline-census.json bind every source input. No other census member changed or disappeared.

The path is public Store.GetAt → captured COW Snapshot.SeekGE → cowSeekGEOneAdmitted → retained basis Snapshot.SeekGEEntryWithFixedScratch → the exact captured Tree.SeekGEWithFixedScratch. It merges that one backend candidate with established immutable memtable.SuccessorTable sources using existing seekPointSuccessorTable/choosePointSuccessor priorities. Unknown source types and a winning physical tombstone retain the original merge fallback. Logical MVCC tombstones retain ordinary public interpretation.

Compact ancestor refs/indexes are members of cowPointScratch, whose full unsafe.Sizeof is externally admitted before allocation; no pool, new ordering authority or current-root acquisition is added. The original tree-height limit fails closed. Ancestors reload immutable parents at leaf boundaries. Exact lookup and PRIMARY operand selection share the same fixed-buffer external-leaf loader, preserving checksum/backing identity checks. Borrowed keys are copied before pointer materialization, and returned values are owned copies. Existing snapshot beginRead/endRead and cache read mutex remain.

Coupling: this path uses ordinary PRIMARY/base digest/component validation plus established immutable COW SuccessorTable.SeekGE. The recent-main Git objects already contain memtable.SuccessorTable, COWRoot.SeekGE and point_successor.go. It does not call native MVCC ordering, cursor, continuation or selection-v6 APIs. It still requires the coherent captured-basis/root publication/lifetime seams present in the frozen baseline; this diagnostic cannot establish their merge disposition or remove graph prerequisites.

## Public activation and focused checks

The 16x public read-only allocation guard is <48KiB, deliberately generous to reject the 57KiB complete iterator. Both baseline and candidate use the same harness. Small is the original four physical records/two versions/two logical keys. To prove actual historical leaf advance, large uses 8194 logical keys at 10/20/30, 24582 physical rows; all setup chunks/checkpoints and root/layout proof occur outside timing. Its actual captured PRIMARY base is depth3 with 2728 leaves, and internal separator bulk-00000004 starts at timestamp10. Public GetAt15 must advance from the prior leaf and returns timestamp10 with exact value. The first two-version large fixture lacked this separator, so that failed proof is retained; only the test fixture changed.

Baseline allocation RED: small58500B/op/15alloc; multilevel68104B/op/25alloc. Candidate GREEN: see public-final-green.raw (small about11.8KB/read, multilevel about21.3KB/read). No artificial production activation counter was added. Public value/physical layout, actual COW selector, ACK route and lease endpoints are asserted.

New tree tests verify equal-key PRIMARY put precedence, inline absence, bad base/component digests, exclusive end, absence, and refusal of nil/foreign external-leaf buffers. Existing focused ownership, old-cut, pressure/refusal retry, tombstone/empty-key/value and queued-close admission tests pass. Focused race passes. The first new tree fixture wrongly allocated base page0/revision0; its construction failure is retained and corrected to valid page/ref/revision inputs without a production candidate change.

## Partial paired public effect

| Fixture | Before→after ns/op | B/op | alloc/op | reader p50 ns | writer p50 ns |
|---|---:|---:|---:|---:|---:|
| small inline | 18186.5 → 15505 | 67329 → 20274 | 92 → 82 | 11845.5 → 10000 | 4505 → 4020 |
| small forced_pointer | 152239 → 269774 | 76557 → 29365 | 186 → 174 | 15990 → 13355 | 12350 → 12305 |

Two balanced pairs per completed fixture, 64x each, CPU0-3/GOMAXPROCS4, Go1.26.3. Eight successful calls have actual matching ACK/layout/capture/publication/Close checks. These tiny samples show lower read allocations and reader p50, but forced-pointer total elapsed regresses substantially and is a concern, not a pass. Tail-sensitive writer I/O remains; no attribution campaign was run. All samples and host process snapshots are retained.

## Hard stop

multilevel-inline-pair1-before.raw fails at N=1 calibration after target value/layout/selector checks: `treedb: final command WAL checkpoint during close: root publication captured basis mismatch`. Public Close wrapper: TreeDB/public.go:1857-1860. Underlying guard: TreeDB/db/root_publication_build_group.go:173-176 rejects basis DB/index/user-root mismatch in beginRootPublicationBuildGroup. No field-specific mismatch was measured, so stale basis vs fixture/close misuse is unresolved. The failed process produced no accepted 64x multilevel row. No multilevel candidate invocation ran.

Root should disposition this existing-baseline failure before further production reassessment; the prototype does not rebase, bypass Close, suppress errors or weaken the guard. No remaining timing budget was consumed.

## Exact retention

Worktree HEAD a99d3c8474b8898b98624c47758653e7c8cb9b7f; index SHA256 9ee0093cadbcefa37ede5b1a6cb3d3e801afb91697f2a8872fb396063fa0f2b7.
Before binary SHA256 d97d5b69496b25d18382743b69a547daa5758598021491fb4bce056990e20b36; after d93e99bc782436c0a0b5fb8daa2e3dfa8a191cbcbcf2ca54fe16051fbe1867d0.
All source copies, binaries, raw failures/successes, qualification and measurement drivers/receipts remain in this task-owned packet. No commits, pushes, GH writes, cleanup, source mutation outside the isolated prototype, or child agents. Go is quiet after collection. Disposable-only release list is empty: retain unique source/binaries until root publishes and verifies recovery; no deletion authorization is inferred.
