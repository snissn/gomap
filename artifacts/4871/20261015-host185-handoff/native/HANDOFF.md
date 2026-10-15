# Native actual publication / Finish split handoff

STOPPED for coordinator transfer. WIP / incomplete / Q32 RED / native ACK=0. No further source edits/tests/imports. No Git content mutations, staging, push, cleanup, timing or profiles. No owned Go/test child remains. Root owns publication and lease release.

## Exact state

Worktree: /home/mikers/benchmarks/mvcc-4871-repair-20261013/native-4878-actual-split-20261015
HEAD: b3c7ff702538e552e685ded554a79eb256a3380c; branch codex/4878-actual-split-20261015.
Index: /home/mikers/dev/snissn/gomap/.git/worktrees/native-4878-actual-split-20261015/index; SHA256 de735ef7183bb6359b802410bba623c79001643a5034a596f7a08ac5bb9f85fb.
Setup index differed (cf92...); fresh git status refreshed metadata, staged diff remains empty. No staging occurred. Further snapshot reads disable optional Git locks.

handoff-source-manifest.json binds the 14 owned paths separately from the inherited frozen43 overlay and every current dirty/untracked path. Immutable source copies are under handoff-source/. handoff-git-status.txt preserves exact changed/untracked census. Overlay artifact a5406f13dd05fb305c5a83d88d54686ab2cd461b, manifest 6cbc2613c25ef5b5bb8703a23ef7d3f4b9fe345fa7ec3e23c2ca9621259d9f92. All inherited paths outside owned edits are byte-identical. Do not reset/recreate the tree. Old native tree, ordinary source and foreign fixtures were not written.

## Shared protocol and accepted Cut/receipt contract

Original Coordinator/EnqueueBuilt now accepts supplied work, caller-owned native receipts remain in original dispatch accounting, original transaction consume is available at the native durable fence, and physical Finish retains custody. Frozen43 consumed-lineage versus terminal physical ACK handling is preserved. Arena acquire/detach admits atomically. No separate publisher/guardian/pool.

conditionalCommitMutation.nativeCut carries SAME Cut into original visible installation; physical-only is checked and commit occurs at nonfallible visible install after allocator activation, before waits. Ordinary admission remains original. Real native producer is NOT wired yet.

Accepted: SAME Request; exact selected+anchor; expectedBaseSeq under original conditional authority. Abort once preaccept. Successful durable fence must commitPublishedSeal/commitPrimarySealV5, ConsumeNativePublicationResourcesV6, original transaction consume and ReleaseNativePublicationTurnV6. Original physical resources, charges and activeDispatch remain through actual Finish; CompleteNativePublicationV6 settles the join and pruned ACK waits. Late Finish cannot replay an older frontier. Close/Stop join outside conflicting locks. Preserve original 33 fixture, caps and negative controls. No global constructor override or synchronous DATA fallback. Previous diagnostic Cut-across-WaitThrough deadlock is not an ordinary production deadlock result.

## Runtime results (scoped historical runs; final snapshot not qualified)

Pinned Go B/toolchains/go1.26.3-from-185-20261014/bin/go; GOROOT that directory, GOTOOLCHAIN=local, GOWORK=off, GOPROXY=off. B=/home/mikers/benchmarks/mvcc-4871-repair-20261013.

PASS shared-compile.log/.json:
`go test ./TreeDB/internal/rootpublication ./TreeDB/internal/primaryarena -run '^(TestPrimary|TestNativeMVCC)' -count=1 -timeout=40s`

PASS shared-focused.log/.json:
`go test ./TreeDB/internal/rootpublication ./TreeDB/internal/primaryarena -run '^(TestNativePublication|TestNativeTransaction|TestPrimaryFinish)' -count=1 -timeout=30s -v`
Component controls: callback admission/refusal/retry, Finish failure custody, native receipt release, Stop join, terminal accounting. These are unit controls, not real selected-record DB activation. PASS predates appended builder-handoff test and final consume helper; do not label current snapshot fully GREEN.

Retained producer compile RED, producer-compile.log:
`go test ./TreeDB/db -run '^$' -timeout=20s`
Missing valuelog.LeafAppendWork, leafMergeCursor/leafMergeEvent/mergeReserve, node.Builder.ResetWithOptionsWithWork and further retained prerequisites. The six-file attempted producer preview is parked OUTSIDE BUILD in retained-producer-port-preview/ with manifest. Original native_mvcc_current_delete_v6.go restored from its preimage; five new preview files removed from product. Later mixed rootpublication/db invocation compiled DB with no matching DB tests (native-builder-handoff.log).

Latest fixture RED, native-builder-handoff-green.log/.json (misleading filename, actual exit 1):
`go test ./TreeDB/internal/rootpublication -run '^TestNativeBuilderHandoffPreservesOriginalOnRefusalV6$' -count=1 -timeout=30s -v`
Refusal before candidate effects: `invalid root publication candidate: candidate frontier does not match exact primary authority`. Earlier ResourceSet/private-field typo fixed; exact authority/fixture mismatch unresolved, not proven production bug. Stop requested before another correction.

## Next source decision and missing actual evidence

Do not expand predecessor imports during transfer. Establish finite compatible retained-producer closure before continuing. retained-producer-predecessor-census.json lists 89 comparisons, NOT an approved/minimal import scope; captured during preview and includes owned changes. Historical 64cee8410991f0712896e52162a594683ec35b18 is reference only, and its cursor still uses an unbounded diagnostic finish. Missing materialization/node/value-log/allocator work seams require coherent adaptation to advanced frozen43 custody, not wholesale obsolete shared files.

Repair builder-handoff fixture exact primary authority, then wire real selected+anchor/expectedBase native producer. First required actual control: pause real Finish after real durable handoff; newer foreground durable writer completes while original receipt/resources remain live and native ACK waits. Follow with deletion, anchor/future, held-reader/reopen, wrong selection/stale base refusal, rollback, late physical errors and Close/Stop. None proved here. Whole Q32, supported work/byte/resource, composite authority and final performance gates OPEN.

All preimages, raw failures, negative controls, unique parked producer sources and original fixture/caps retained. No cleanup release.
