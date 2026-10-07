CLEAN scoped source integration review at `ebe414b6d6a22a7f616c108157a01f8ed109e7cc`. No material correctness defect found in this bounded review. Fresh exact-source validation and A/C/D capture remain required; no performance, capacity, actual landing or CI acceptance is inferred.

New main `3cfe2ad896cad1818f72d3d14c74e548fecc1657` adds/changes 56 canonical production inputs compared with `f2c93cfcdf5f7ef54f6d7bc4ff9e6946fb21a72c`. Reviewed P1 `8506a498346d35c635bbd0f59d99a135d6c6785d` and consolidated head have the identical 1,122-input runtime `cb5b0d7b3633666ae16752d8c353eeeb23542c71e5e25773e33359d3ffa79aa9`. Previous 1,098-input runtime `9bae22…` is historical. Canonical scope includes Go, assembly/native source and go.mod/go.sum, exactly as source helper lines30–38.

The consolidated head has parents 8506 and reviewed C90fa. All seven A/C harness blobs equal C90fa, hash `706b71f11508bf9c0075b0dd9c5463cd741a49cba83996feaf753330ace77822`. The four original A semantic blobs, D capture/validator and selected lifecycle fixture/tests are unchanged. This supports workload-semantic applicability only: actual final certificates and captures need newly landed source bindings and observed environment receipts.

Explicit COW admission: cow_btree is opt-in; ordinary public blank MemtableMode still defaults to adaptive. COW limits/reverse/conditional unsupported cases apply only to explicit COW. Existing profiles and A/C/D fixture/options sources do not request COW.

R1 opener reachability: A/C use backend root APIs with cached leaf-log wiring via OpenBackendWithCachedLeafLog, so shared lower-layer writer/manager/tree/command append paths are reachable, while public read-owner wrapper and public maintenance wrappers are not those retained workload entry points. D uses direct OpenBackend durable profile with side-store authority; no cached COW owner is constructed.

Shared successful append observer: Writer now calls successful produced-frame observer seam. Nil observer returns without callback; ordinary batch returns nil observer and preserves pooling/flush conditions. Added branches and layout are real new runtime inputs and can affect measured cost; no performance equivalence inferred.

Shared physical rewrite revision: Pointer match/swap now preserves revision from current winning entry, preventing maintenance from resetting logical revision. Reachable D native rewrite; current exact focused revision tests needed.

Accepted root publication error cleanup: Accepted-error finalize releases producer-owned vlogRefDelta before observation clears it; nil-basis ordinary group registration retains existing growable path. Bounded snapshot basis and fixed registry admission are separate new APIs selected by COW.

Command WAL ACK/recovery: Collection production source, R1 process-cut test source and non-raw recovery files remain byte-identical to previous R1. Shared raw append driver changed to add optional pre-append canonical finalizer; old prepared API passes nil and normal dependency RID-cache release remains. COW finalizer is not used by these native collection fixture operations. Byte identity of collection callers alone does not waive updated shared runtime validation.

Shared default public APIs: Public reads capture existing lower-layer owners through TryRLock so a waiting Close denies new read capture without retaining wrapper lock through callbacks. Public ValueLogGC now also performs cached post-maintenance reconciliation in non-COW mode. Existing public online GC/read-close tests are available; retained D direct-backend entry point bypasses this wrapper. This is a behavioral delta to record, not a discovered integration defect.

Default bounded-read guards: Ordinary tree GetEntry passes bounded=false; fixed key scratch/owned iterator/read-admission APIs are separate COW paths. Snapshot callback lease Close is nil-safe in non-COW path.

Physical identity/lifetime auto-merge: Manager additions are frozen Set metadata envelopes and exact registered-file metadata lookup; stable-resource addition returns immutable registered identity only with live owner. R1 Manager Close/worker admission/wait/retry/delete functions are byte-identical to 3182. Existing leaf manifest bounded-FD cleanup and identity selector source are unchanged; no stale numeric identity reopen introduced.

Merged specifications: COW recovery/lifecycle sections explicitly preserve sealed recovery, RID/revision/namespace identity authority, durable dependency custody and persistent vlog constraints. R1 guide/spec retain corrected maximum 18 GC-child descriptors and finite cost qualification pending.

Minimal focused existing validation for the root executor (reviewer ran none):

- `./TreeDB/collections -run ^Test(R1TypedRow|R1Mutation|R1Lifecycle)`: Retains public full-row current/held read parity, indexed mutation matrix and 18 selected process cuts, D mixed lifecycle/rewrite/vacuum/reopen tests. Existing current-source execution should supply normal/race receipts, not duplicated reviewer runs.
- `./TreeDB/db -run ^(TestRewriteSwapPreservesWinningCurrentRevision|TestSelectedValueLogRewriteRevisionLifetime|TestRootPublicationBuildGroupCommandWALAcceptedWaitFailureDoesNotPoisonOpenHandle)$`: Minimal additive shared new-main regression coverage. The selected rewrite test covers 3 WAL modes, current winning revisions, old held physical pointers and reopen; it does not explicitly run GC.
- `./TreeDB/internal/valuelog -run ^(TestCOWProducedFrameObserverMatchesEncodedFrames|TestCOWProducedFrameObserverRejectsBeforeObservation|TestManagerCloseJoinsPinnedZombieRetry5066|TestManagerConcurrentRetryAdmissionAndClose5066)$`: Checks new successful-frame observer timing/refusal and preserved R1 worker-close/admission semantics. Root already planned retry tests; append only missing observer tests.
- `./TreeDB/db -run ^Test(LeafManifestRevisionGC|LeafGenerationGCReclaimsReleasedManifestRevisions5066|R1NativeAppenderCreationMetadata|R1Rebuilt)`: Reuse root-planned R1 focused regression packet: bounded descriptors, late held protection, partial/error cleanup, malformed/rebound inventory, both recovery slots and metadata lifetimes.

The public read-close and online GC tests exercise changed public wrapper behavior but do not replace D native profile evidence. No duplicate broad suite or new test framework is requested.

Physical merge proof: all seven selected manager/retry/delete functions retain identical serialized source bytes from `3182e15dfe1aa11120db9d309ad0d590283398d6`; individual hashes are in JSON. All 274 canonical collections production inputs are identical. Shared lower-layer paths above still changed, so source identity of callers is not performance or binary identity.

Exact changed paths and before/new-main/prior-R1/integrated/consolidated Git blobs (full values and per-path diff hashes in JSON):

- modified `TreeDB/cached_backend_maintenance.go`: old `5518f80cdcdf012db4603569a415c3e7178be341` → new main `6b099fc24b1e6bcf7ae4f0cd521039cdd915eb81`; final `6b099fc24b1e6bcf7ae4f0cd521039cdd915eb81`.
- modified `TreeDB/caching/conditional_txn.go`: old `59aa62d0c71a133b17a6fa811b037d68bc944d23` → new main `6c9e3bd7d4731be6a59ab9dfaa11e35b9ed4e594`; final `6c9e3bd7d4731be6a59ab9dfaa11e35b9ed4e594`.
- added `TreeDB/caching/cow_batch.go`: old `absent` → new main `56847c1f313ecd25384fd77a37230f8b03a9ef6e`; final `56847c1f313ecd25384fd77a37230f8b03a9ef6e`.
- added `TreeDB/caching/cow_batch_storage.go`: old `absent` → new main `533c212677fdf39311bc5ee5c793ef040f4bc564`; final `533c212677fdf39311bc5ee5c793ef040f4bc564`.
- added `TreeDB/caching/cow_checkpoint.go`: old `absent` → new main `09ec3014bddd790bb955e4bab3f2bf784dc7f014`; final `09ec3014bddd790bb955e4bab3f2bf784dc7f014`.
- added `TreeDB/caching/cow_cut.go`: old `absent` → new main `1568701b933c818192b09b7e353318a35f9b9129`; final `1568701b933c818192b09b7e353318a35f9b9129`.
- added `TreeDB/caching/cow_flush.go`: old `absent` → new main `b43a467197fc492c0e34b859bf8176fe40cea036`; final `b43a467197fc492c0e34b859bf8176fe40cea036`.
- added `TreeDB/caching/cow_generation_preflight.go`: old `absent` → new main `f67fd65a41e64d2a6b1f4b66a39cfa486a2bd971`; final `f67fd65a41e64d2a6b1f4b66a39cfa486a2bd971`.
- added `TreeDB/caching/cow_maintenance.go`: old `absent` → new main `1cd5fe9099ff64f2200583c9fdf1ff8affdba82e`; final `1cd5fe9099ff64f2200583c9fdf1ff8affdba82e`.
- added `TreeDB/caching/cow_metadata.go`: old `absent` → new main `767284221a1866523a5ea62af5c9f5f72d5df9ff`; final `767284221a1866523a5ea62af5c9f5f72d5df9ff`.
- added `TreeDB/caching/cow_open.go`: old `absent` → new main `738668580b950604ff5b4997dd5eec45687c9e34`; final `738668580b950604ff5b4997dd5eec45687c9e34`.
- added `TreeDB/caching/cow_producer.go`: old `absent` → new main `1f6c23104e9f07676c3972a5d79e551f3e2bb226`; final `1f6c23104e9f07676c3972a5d79e551f3e2bb226`.
- added `TreeDB/caching/cow_read.go`: old `absent` → new main `d9be3aed14928245616e94a38fcb69cf33326872`; final `d9be3aed14928245616e94a38fcb69cf33326872`.
- added `TreeDB/caching/cow_read_workspace.go`: old `absent` → new main `8b657b3d7db5ce1fabfaad50c72db5fdcc832ea3`; final `8b657b3d7db5ce1fabfaad50c72db5fdcc832ea3`.
- added `TreeDB/caching/cow_resources.go`: old `absent` → new main `3de15631832f6b302cd484c17d689961dd16d610`; final `3de15631832f6b302cd484c17d689961dd16d610`.
- added `TreeDB/caching/cow_rollover.go`: old `absent` → new main `1971efb5b351b5c1af44801322be73d5f0504b5d`; final `1971efb5b351b5c1af44801322be73d5f0504b5d`.
- added `TreeDB/caching/cow_stats.go`: old `absent` → new main `40ff8fbf0b95a63934eec422b2e0fad7876ed527`; final `40ff8fbf0b95a63934eec422b2e0fad7876ed527`.
- added `TreeDB/caching/cow_table.go`: old `absent` → new main `0e5c4c82c7b5ebd4d2fdcb8fbccfaaf7f452bbd1`; final `0e5c4c82c7b5ebd4d2fdcb8fbccfaaf7f452bbd1`.
- modified `TreeDB/caching/db.go`: old `0c051875a6899caec5d2e059a05a6034da2e6835` → new main `26285636844230d33d1b56da08c8bb8c43fe95c9`; final `26285636844230d33d1b56da08c8bb8c43fe95c9`.
- modified `TreeDB/caching/point_successor.go`: old `8838a4700e4ebf9965485d739be75225b01ae6f1` → new main `b1bf8f83c9073d53b620131908520b3f165306c1`; final `b1bf8f83c9073d53b620131908520b3f165306c1`.
- modified `TreeDB/caching/snapshot.go`: old `85c59dfc8847f28388058b86f3834dfa0f3e16b9` → new main `08649740d8fdf79fe97b8f302f31e48c59061f9b`; final `08649740d8fdf79fe97b8f302f31e48c59061f9b`.
- modified `TreeDB/caching/snapshot_iterator_lifetime.go`: old `f2eb5c2b1a920c08038169ec04ccae23dacf9568` → new main `2f6845be2d51cdfa05e40ec5e54d6cec4cef47f0`; final `2f6845be2d51cdfa05e40ec5e54d6cec4cef47f0`.
- modified `TreeDB/caching/snapshot_read_lifetime.go`: old `0b9a1e5f1c9ebdf827a6adadb9d09236f62adf46` → new main `8724eb59dca4c0253d1257bab323bfff8aa7093a`; final `8724eb59dca4c0253d1257bab323bfff8aa7093a`.
- modified `TreeDB/command_wal_public_cached.go`: old `f80c92a2312ac4af5ac7827a1e83d6764a9b8486` → new main `e989f3fe292918fe9431c24a0851b676f3513414`; final `e989f3fe292918fe9431c24a0851b676f3513414`.
- modified `TreeDB/compact_storage.go`: old `8d12f470d695a86eb99372921643d3673a3f6126` → new main `ab9aa54ee3a894cfbc07a7ce00a3fe635c3522db`; final `ab9aa54ee3a894cfbc07a7ce00a3fe635c3522db`.
- added `TreeDB/db/bounded_read_owner.go`: old `absent` → new main `e6f67a19d852561668ee4df8bc62cdeedeb72a55`; final `e6f67a19d852561668ee4df8bc62cdeedeb72a55`.
- modified `TreeDB/db/command_wal_raw.go`: old `83ffc2d21603516f05470048ef08123f024a637c` → new main `81a7216cd33291f82c6b4dec3bb7b4ec06eb9ed6`; final `81a7216cd33291f82c6b4dec3bb7b4ec06eb9ed6`.
- added `TreeDB/db/cow_options.go`: old `absent` → new main `b15665219887c367087c4bff62a19701c1cf0243`; final `b15665219887c367087c4bff62a19701c1cf0243`.
- modified `TreeDB/db/db.go`: old `07c79f324245d3e89b742a85ecb81bf1940c9de2` → new main `71c8fabaab6253675f48bf6d59f870fc104c9be8`; final `71c8fabaab6253675f48bf6d59f870fc104c9be8`.
- modified `TreeDB/db/root_publication_build_group.go`: old `7c124705a8499269817d01395808625a72998957` → new main `0e26a8af745f6aaf5ad8c126e6a9099dd2589468`; final `0e26a8af745f6aaf5ad8c126e6a9099dd2589468`.
- modified `TreeDB/db/vlog_rewrite.go`: old `43860615915a14752fb6aea084d4fb4e5b8a9b8c` → new main `d4901447167143ae310046dc2da046221740a3f1`; final `d4901447167143ae310046dc2da046221740a3f1`.
- added `TreeDB/internal/dictdb/read_definition.go`: old `absent` → new main `439852a9b0fd57600bd46d1dfe3a97cc21cae3d8`; final `439852a9b0fd57600bd46d1dfe3a97cc21cae3d8`.
- modified `TreeDB/internal/durabilitycut/durabilitycut.go`: old `3ee06040808884c59d34bf983615aba1f5c1fe0a` → new main `764a1fd220f757f8edee3cab63e4fdc6acd9f65d`; final `764a1fd220f757f8edee3cab63e4fdc6acd9f65d`.
- modified `TreeDB/internal/merging/merging.go`: old `f7ce8793b2d95316245737e81798a7c2db427749` → new main `5500afb0baec30b68098171facde727e3815aada`; final `5500afb0baec30b68098171facde727e3815aada`.
- added `TreeDB/internal/valuelog/cow_inspection_heap.go`: old `absent` → new main `8968fef8ee6eac902752710755068f45366a9399`; final `8968fef8ee6eac902752710755068f45366a9399`.
- added `TreeDB/internal/valuelog/cow_inspection_stack.go`: old `absent` → new main `9a48ca86ef631d9c85587dd6761bdb551c1e3799`; final `9a48ca86ef631d9c85587dd6761bdb551c1e3799`.
- added `TreeDB/internal/valuelog/cow_reader.go`: old `absent` → new main `14496f75288211f91b66fdad23c766d2672514ec`; final `14496f75288211f91b66fdad23c766d2672514ec`.
- modified `TreeDB/internal/valuelog/leaf_page_payload.go`: old `c52e2ba6c86ea8aaf8ed3cad7da531acd3662c28` → new main `bbfec790d7d1bc262b9e51ca73fb0cbc235fb68b`; final `bbfec790d7d1bc262b9e51ca73fb0cbc235fb68b`.
- modified `TreeDB/internal/valuelog/manager.go`: old `1a613731464df0c8a8ef3d0036fb71232ca141ac` → new main `881b1d4bfb041c13c9210be43a2df7b9c2e24082`; final `976ddd6ed35599342c134eb78ed8433c91e994f9`.
- added `TreeDB/internal/valuelog/produced_frame.go`: old `absent` → new main `d1a5d36d0e88954a83c212c4e676dfc3b3b4cb03`; final `d1a5d36d0e88954a83c212c4e676dfc3b3b4cb03`.
- modified `TreeDB/internal/valuelog/stable_resource.go`: old `d9dd7c01fd6965d3e47cf33e04a56da9d2e20534` → new main `27af14233e201939895b1cc216f6f7419349ad6a`; final `ffffe0e0e16c285def4b82b5d5287186c6bb0df6`.
- modified `TreeDB/internal/valuelog/writer.go`: old `e983927e76d92bccda174088e4b7020683f6103d` → new main `86fa63914fa0573e85848b6c0c30742c0b3cd357`; final `86fa63914fa0573e85848b6c0c30742c0b3cd357`.
- modified `TreeDB/internal/valuelog/writer_writev.go`: old `e5be5faf7cc2a24200a8e56a314e9b68654c9773` → new main `520220827ed68037b039035aaa38f8d476857706`; final `520220827ed68037b039035aaa38f8d476857706`.
- modified `TreeDB/leaf_generation_pack.go`: old `19a4676ffd9c72fc39a9fc1b7c070fb2fd5435e3` → new main `94648d23b607db08c14f38ede4f290eaebf46c30`; final `94648d23b607db08c14f38ede4f290eaebf46c30`.
- modified `TreeDB/leaf_generation_pack_from_plan.go`: old `3dcf23202167faf5fc94ad7232532c8f43ac6287` → new main `626ff072f196b28fb21e7222f2b056fdfab0a21a`; final `626ff072f196b28fb21e7222f2b056fdfab0a21a`.
- modified `TreeDB/leaf_generation_pack_run_once.go`: old `55edd1dde4c3e3979c6f14008ad122b707ddc021` → new main `50946db30d54d8607acdb9047925e5197ced362c`; final `50946db30d54d8607acdb9047925e5197ced362c`.
- modified `TreeDB/lifecycle/registry.go`: old `6b49d839ba0768b803462b3275463970109a295d` → new main `e68ef29f9ea8486ed5f4cbb1122906e40ca48b2d`; final `e68ef29f9ea8486ed5f4cbb1122906e40ca48b2d`.
- modified `TreeDB/node/internal.go`: old `b30b01260970d1f7d312b8d0fcb81d5c2073d851` → new main `78f62980635cd31b6b416e5e520e3789e36886e8`; final `78f62980635cd31b6b416e5e520e3789e36886e8`.
- modified `TreeDB/node/leaf.go`: old `8e1ab3330290db5a06f7f97dbdf78695ded1669b` → new main `22058a6261ac91d2f290f9eac1fd59509d3abe2e`; final `22058a6261ac91d2f290f9eac1fd59509d3abe2e`.
- modified `TreeDB/node/node.go`: old `0452cee5443174ef8eb00232bec8aed1df4f36cc` → new main `0c018f9f653bff0fa9f65c92cf10076f91f16062`; final `0c018f9f653bff0fa9f65c92cf10076f91f16062`.
- modified `TreeDB/public.go`: old `c39b0cf403139984fce20cfb40c555e63517ad68` → new main `682959be85edfb499b0b1c372a08cc0c91118644`; final `682959be85edfb499b0b1c372a08cc0c91118644`.
- modified `TreeDB/tree/iterator.go`: old `dd33637ab1c88b749e600b4878c7e39bdac67d00` → new main `f5380d1ef83636eeca3170c1220801bb888e1e31`; final `f5380d1ef83636eeca3170c1220801bb888e1e31`.
- added `TreeDB/tree/owned_iterator.go`: old `absent` → new main `a3e61a05747aafc49b3a815258ce813ede5948fa`; final `a3e61a05747aafc49b3a815258ce813ede5948fa`.
- modified `TreeDB/tree/tree.go`: old `e27e060315bdb96fe3002062eaa7ee63a153a22d` → new main `1a201ac02b3e94b7af96f53eb79c2b3a8201032b`; final `1a201ac02b3e94b7af96f53eb79c2b3a8201032b`.
- modified `TreeDB/vlog_gc.go`: old `22abc52f31829fb389a2382d43c385fb326fbf39` → new main `549282932b7c44f663439c304e759730a0cd834d`; final `549282932b7c44f663439c304e759730a0cd834d`.
- modified `TreeDB/vlog_rewrite.go`: old `63b2173a2c2feaefb03fbef364f0733d71c4e74b` → new main `8380beb1de364ff016704bbbbf732ddd144480d1`; final `8380beb1de364ff016704bbbbf732ddd144480d1`.

Selected current source evidence:

- [TreeDB/public.go:592](https://github.com/snissn/gomap/blob/ebe414b6d6a22a7f616c108157a01f8ed109e7cc/TreeDB/public.go#L592): `func (db *DB) captureReadOwners() (*caching.DB, *db.DB, error) {`
- [TreeDB/public.go:780](https://github.com/snissn/gomap/blob/ebe414b6d6a22a7f616c108157a01f8ed109e7cc/TreeDB/public.go#L780): `if opts.MemtableMode == "cow_btree" {`
- [TreeDB/public.go:1113](https://github.com/snissn/gomap/blob/ebe414b6d6a22a7f616c108157a01f8ed109e7cc/TreeDB/public.go#L1113): `opts.MemtableMode = "adaptive"`
- [TreeDB/open_backend.go:13](https://github.com/snissn/gomap/blob/ebe414b6d6a22a7f616c108157a01f8ed109e7cc/TreeDB/open_backend.go#L13): `func OpenBackend(opts Options) (*db.DB, func() error, error) {`
- [TreeDB/open_backend.go:48](https://github.com/snissn/gomap/blob/ebe414b6d6a22a7f616c108157a01f8ed109e7cc/TreeDB/open_backend.go#L48): `backend, err := db.Open(opts)`
- [TreeDB/open_backend.go:76](https://github.com/snissn/gomap/blob/ebe414b6d6a22a7f616c108157a01f8ed109e7cc/TreeDB/open_backend.go#L76): `func OpenBackendWithCachedLeafLog(opts Options) (*db.DB, func() error, error) {`
- [cmd/collection_workload_bench/r1.go:689](https://github.com/snissn/gomap/blob/ebe414b6d6a22a7f616c108157a01f8ed109e7cc/cmd/collection_workload_bench/r1.go#L689): `opts := treedb.OptionsFor(profile, dir)`
- [cmd/collection_workload_bench/r1.go:690](https://github.com/snissn/gomap/blob/ebe414b6d6a22a7f616c108157a01f8ed109e7cc/cmd/collection_workload_bench/r1.go#L690): `t.db, t.cleanup, err = treedb.OpenBackendWithCachedLeafLog(opts)`
- [TreeDB/collections/r1_lifecycle_5060_profile_test.go:44](https://github.com/snissn/gomap/blob/ebe414b6d6a22a7f616c108157a01f8ed109e7cc/TreeDB/collections/r1_lifecycle_5060_profile_test.go#L44): `want := map[string]bool{"outer": true, "packed": true, "prefix": true, "columnar": true, "internal_base": false, "command_wal": true, "disable_background_prune": true, "verified_reads": true, "current_writable_mmap": false}`
- [TreeDB/collections/r1_lifecycle_5060_profile_test.go:56](https://github.com/snissn/gomap/blob/ebe414b6d6a22a7f616c108157a01f8ed109e7cc/TreeDB/collections/r1_lifecycle_5060_profile_test.go#L56): `opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir)`
- [TreeDB/collections/r1_lifecycle_5060_profile_test.go:58](https://github.com/snissn/gomap/blob/ebe414b6d6a22a7f616c108157a01f8ed109e7cc/TreeDB/collections/r1_lifecycle_5060_profile_test.go#L58): `db, closeOwner, err := treedb.OpenBackend(opts)`
- [TreeDB/caching/db.go:3018](https://github.com/snissn/gomap/blob/ebe414b6d6a22a7f616c108157a01f8ed109e7cc/TreeDB/caching/db.go#L3018): `func (db *DB) appendValueLogForRecordsObserved(l *lane, records []valuelog.Record, durability journalDurability, observer valuelog.ProducedFrameObserver) ([]page.ValuePtr, error) {`
- [TreeDB/caching/db.go:12500](https://github.com/snissn/gomap/blob/ebe414b6d6a22a7f616c108157a01f8ed109e7cc/TreeDB/caching/db.go#L12500): `cowMode := modeStr == "cow_btree"`
- [TreeDB/caching/db.go:17633](https://github.com/snissn/gomap/blob/ebe414b6d6a22a7f616c108157a01f8ed109e7cc/TreeDB/caching/db.go#L17633): `func (db *DB) appendValueLogInternalObserved(l *lane, dictID uint64, dict []byte, records []valuelog.Record, durability journalDurability, capture *stableOuterLeafCapture, observer valuelog.ProducedFrameObserver) ([]page.ValuePtr, *rootpublication.StableResourceSet, error) {`
- [TreeDB/caching/cow_producer.go:93](https://github.com/snissn/gomap/blob/ebe414b6d6a22a7f616c108157a01f8ed109e7cc/TreeDB/caching/cow_producer.go#L93): `func (b *Batch) cowProducedFrameObserver() valuelog.ProducedFrameObserver {`
- [TreeDB/caching/cow_producer.go:113](https://github.com/snissn/gomap/blob/ebe414b6d6a22a7f616c108157a01f8ed109e7cc/TreeDB/caching/cow_producer.go#L113): `func installCOWProducerObserver(w valueWriter, observer valuelog.ProducedFrameObserver) (valuelog.ProducedFrameObserver, error) {`
- [TreeDB/caching/cow_producer.go:124](https://github.com/snissn/gomap/blob/ebe414b6d6a22a7f616c108157a01f8ed109e7cc/TreeDB/caching/cow_producer.go#L124): `func restoreCOWProducerObserver(w valueWriter, observer, old valuelog.ProducedFrameObserver) {`
- [TreeDB/cached_backend_maintenance.go:5](https://github.com/snissn/gomap/blob/ebe414b6d6a22a7f616c108157a01f8ed109e7cc/TreeDB/cached_backend_maintenance.go#L5): `func (db *DB) runCachedBackendMaintenance(fn func() error) error {`
- [TreeDB/cached_backend_maintenance.go:6](https://github.com/snissn/gomap/blob/ebe414b6d6a22a7f616c108157a01f8ed109e7cc/TreeDB/cached_backend_maintenance.go#L6): `if db != nil && db.cached != nil && db.cached.COWMode() {`
- [TreeDB/cached_backend_maintenance.go:9](https://github.com/snissn/gomap/blob/ebe414b6d6a22a7f616c108157a01f8ed109e7cc/TreeDB/cached_backend_maintenance.go#L9): `return db.reconcileCachedBackendMaintenance(fn())`
- [TreeDB/vlog_gc.go:113](https://github.com/snissn/gomap/blob/ebe414b6d6a22a7f616c108157a01f8ed109e7cc/TreeDB/vlog_gc.go#L113): `err = db.runCachedBackendMaintenance(func() error {`
- [TreeDB/db/vlog_rewrite.go:3018](https://github.com/snissn/gomap/blob/ebe414b6d6a22a7f616c108157a01f8ed109e7cc/TreeDB/db/vlog_rewrite.go#L3018): `_, ptr, flags, revision := iterator.UnsafeEntryWithRevision(it)`
- [TreeDB/db/vlog_rewrite.go:3022](https://github.com/snissn/gomap/blob/ebe414b6d6a22a7f616c108157a01f8ed109e7cc/TreeDB/db/vlog_rewrite.go#L3022): `b.AppendPointerViewNoTouchTrustedSortedWithRevision(swap.key, swap.newPtr, revision)`
- [TreeDB/db/root_publication_build_group.go:64](https://github.com/snissn/gomap/blob/ebe414b6d6a22a7f616c108157a01f8ed109e7cc/TreeDB/db/root_publication_build_group.go#L64): `func (db *DB) BeginRootPublicationBuildGroup() (*RootPublicationBuildGroup, error) {`
- [TreeDB/db/root_publication_build_group.go:75](https://github.com/snissn/gomap/blob/ebe414b6d6a22a7f616c108157a01f8ed109e7cc/TreeDB/db/root_publication_build_group.go#L75): `func (db *DB) BeginRootPublicationBuildGroupFromSnapshot(basis *Snapshot) (*RootPublicationBuildGroup, error) {`
- [TreeDB/db/root_publication_build_group.go:423](https://github.com/snissn/gomap/blob/ebe414b6d6a22a7f616c108157a01f8ed109e7cc/TreeDB/db/root_publication_build_group.go#L423): `releaseValueLogRefDelta(group.vlogRefDelta)`
- [TreeDB/db/command_wal_raw.go:1529](https://github.com/snissn/gomap/blob/ebe414b6d6a22a7f616c108157a01f8ed109e7cc/TreeDB/db/command_wal_raw.go#L1529): `func (db *DB) appendRawKVCommandWALOrderedEntryScanWithHintPrepared(prepare func() error, scanEntries func(func(batchpkg.Entry) error) error, opHint int, mode RawKVCommandWALAppendMode, measured bool) (uint64, CommandWALRequestTiming, error) {`
- [TreeDB/db/command_wal_raw.go:1530](https://github.com/snissn/gomap/blob/ebe414b6d6a22a7f616c108157a01f8ed109e7cc/TreeDB/db/command_wal_raw.go#L1530): `return db.appendRawKVCommandWALOrderedEntryScanWithHintPreparedFinalized(prepare, nil, scanEntries, opHint, mode, measured)`
- [TreeDB/db/command_wal_raw.go:1832](https://github.com/snissn/gomap/blob/ebe414b6d6a22a7f616c108157a01f8ed109e7cc/TreeDB/db/command_wal_raw.go#L1832): `if intent.rawKVFinalize == nil {`
- [TreeDB/internal/valuelog/produced_frame.go:29](https://github.com/snissn/gomap/blob/ebe414b6d6a22a7f616c108157a01f8ed109e7cc/TreeDB/internal/valuelog/produced_frame.go#L29): `func (w *Writer) observeProducedFrame(header FrameHeader, first page.ValuePtr, count int) {`
- [TreeDB/internal/valuelog/produced_frame.go:30](https://github.com/snissn/gomap/blob/ebe414b6d6a22a7f616c108157a01f8ed109e7cc/TreeDB/internal/valuelog/produced_frame.go#L30): `if w.producedFrameObserver != nil {`
- [TreeDB/internal/valuelog/manager.go:1233](https://github.com/snissn/gomap/blob/ebe414b6d6a22a7f616c108157a01f8ed109e7cc/TreeDB/internal/valuelog/manager.go#L1233): `retentionShape      SetRetentionSizes`
- [TreeDB/internal/valuelog/manager.go:1798](https://github.com/snissn/gomap/blob/ebe414b6d6a22a7f616c108157a01f8ed109e7cc/TreeDB/internal/valuelog/manager.go#L1798): `func (m *Manager) Close() error {`
- [TreeDB/internal/valuelog/manager.go:1820](https://github.com/snissn/gomap/blob/ebe414b6d6a22a7f616c108157a01f8ed109e7cc/TreeDB/internal/valuelog/manager.go#L1820): `m.retryWorkers.Wait()`
- [TreeDB/internal/valuelog/stable_resource.go:204](https://github.com/snissn/gomap/blob/ebe414b6d6a22a7f616c108157a01f8ed109e7cc/TreeDB/internal/valuelog/stable_resource.go#L204): `func (f *File) RegisteredStableIdentity() (rootpublication.StableIdentity, bool) {`
- [TreeDB/internal/valuelog/stable_resource.go:893](https://github.com/snissn/gomap/blob/ebe414b6d6a22a7f616c108157a01f8ed109e7cc/TreeDB/internal/valuelog/stable_resource.go#L893): `m.startZombieRetry(file)`
- [TreeDB/internal/valuelog/stable_resource.go:913](https://github.com/snissn/gomap/blob/ebe414b6d6a22a7f616c108157a01f8ed109e7cc/TreeDB/internal/valuelog/stable_resource.go#L913): `m.startZombieRetry(file)`
- [TreeDB/tree/tree.go:571](https://github.com/snissn/gomap/blob/ebe414b6d6a22a7f616c108157a01f8ed109e7cc/TreeDB/tree/tree.go#L571): `func (t *Tree) GetEntry(key []byte) (node.LeafEntry, error) {`
- [TreeDB/tree/tree.go:572](https://github.com/snissn/gomap/blob/ebe414b6d6a22a7f616c108157a01f8ed109e7cc/TreeDB/tree/tree.go#L572): `return t.getEntry(key, nil, nil, nil, false)`
- [TreeDB/tree/tree.go:578](https://github.com/snissn/gomap/blob/ebe414b6d6a22a7f616c108157a01f8ed109e7cc/TreeDB/tree/tree.go#L578): `func (t *Tree) GetEntryWithFixedScratch(key, keyScratch, leafScratch []byte, readLeaf func(page.LeafLogPtr, []byte) ([]byte, error)) (node.LeafEntry, error) {`
- [TreeDB/internal/memtable/cow_budget.go:127](https://github.com/snissn/gomap/blob/ebe414b6d6a22a7f616c108157a01f8ed109e7cc/TreeDB/internal/memtable/cow_budget.go#L127): `func (l *COWExternalLease) Close() {`

Remaining authority stays with root: actual landed identity, current required gates and normal/race/vet receipts, original baseline pins, independent A semantic certificate with actual current source/environment, actual C/D analyzer acceptance and noise guards. No old evidence metadata may be rewritten.

Operations: Git object/path/diff reads, existing private review metadata reads, Python hashing/source comparisons; no Go/build/test/capture/GitHub/polling/delegation. Original evidence and repository files preserved. Only this new review pair retained.
