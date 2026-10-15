# Native public cost and diagnostic allocation evidence

Production remains RED. One direct PruneVersions latency invocation refused at phase16 after one ordinary preACK CommitAt, ACK0 and cut0. The reported 479,495ns is time to refusal, not latency to ACK; ordinary write was77,971ns. Allocation was NOT_COLLECTED in this cell: zero log fields do not mean0B/0alloc. Checked cursor Close failed with 'freelist: installed append needs retained trie retirement continuation'; DB.Close then failed with unfinished COW publication. Reopen was not reached. Production outer-allocation test compiled but was not executed. No production speedup or Q32/1MiB completion acceptance follows.

A separately named, directly selected UNBOUNDED DIAGNOSTIC allocation invocation completed ACK1 with exactly one ordinary preACK CommitAt and checked Close/reopen. It never fell back from production. This diagnostic recorded90 prepared observations, cut0, two encoded output leaves(flags0xf044), DATA children37→38/shared36, and same installed/published DATA page29. Old-cut/output/future/postACKwrite/format/reopen checks passed. Its5,554,903ns first-call-to-ACK wall and325,413ns write were collected under rate1 allocation profiling and GC-conditioned setup: these are diagnostic overhead observations, not clean latency or comparative performance evidence.

Both fresh calls used Go1.26.3, CPU0-3 and GOMAXPROCS4,90s Go/120s outer limits; no timeout. The fixture had1024 persisted unchanged pointer rows, proven multileaf DATA and Q32/1MiB, one selected version, a retained old cut, and the same fixed-one-write trigger. Existing every-return identity qualification was untouched. Per-call ReadMemStats was absent. Two fresh selected calls ran; no further collection is authorized.

## Settled allocation attribution

Raw runtime.MemProfile snapshots were taken after twoGC before and after the measured public sequence. Raw record buffers were allocated before baseline, and JSON/frame encoding occurred after snapshots. Before/After buckets are each aggregated by exact Stack0 before subtraction, because runtime bucket identity includes allocation size as well as stack. Before5040records/4674stacks; After5876/5487. Raw records, PCs and decoded caller frames are preserved. Parent independently reproduced totals and found no negative deltas.

| Caller category | Bytes | Objects |
|---|---:|---:|
| Diagnostic prune public stack |2,674,568|1,102|
| One ordinary CommitAt public stack |9,344|81|
| Unclassified/window-unproven |6,976|16|
| All deltas |2,690,888|1,199|

Missing public-entry frames are unclassified/window-unproven, not automatically background: runtime stack records have bounded depth. Setup/Stats/profile serialization were outside the measured window. GC/profile runtime or unrelated activity can remain in unproven rows.

Largest allocation families (percent of2,683,912B proven public-entry bytes):
- PruneCursor:737,280B/1object,27.470%, prune_cursor_v6.go:118.
- ChargedProjection:630,784B/1object,23.502%, charged_projection.go:117.
- Materialized frames:540,672B/3objects,20.145%, materialized_write_work.go:126 and495.
Together1,908,736B/71.118%. Cold codec scratch adds139,264B/1object at block_codec.go:74.

Object leaders: prune_cursor_v6.go:97 local OrdinalScanWork15,904B/142objects; rootpublication/resource_allocation.go:40 resource allocation wrappers6,720B/84objects; internal/stringslite.Clone1,304B/70objects. Full top lists and stacks are in top-allocation-sites.json, attribution-v2/allocation-summary.json and raw diagnostic-allocation-records.json. These count leaders differ from byte leaders. Manager.currentSetLocked is4,224B/6objects in this sample, not the dominant byte source.

## Frozen source anchors and interpretation

All paths below are relative to attribution-v2/sources; source-anchor-bindings.json verifies their bytes against the measured effective census.
- TreeDB/mvcc/prune_cursor_v6.go:28-82 defines the retained cursor. Lines47,52,55 embed six64KiB byte buffers (393,216 explicit bytes), plus max-key arrays and the prepared backend object at74. Lines109-118 admit and charge sizeof cursor before construction.
- TreeDB/tree/charged_projection.go:63-89 embeds pages[maxTraversalDepth][page.PageSize] at69, plus delta page, key arrays, base/decoder/comparison state. TreeDB/tree/tree.go:58 sets depth50: the explicit page stack is204,800B, only part of the630,784B allocation. Lines111-117 preserve existing retained admission.
- TreeDB/zipper/materialized_write_work.go:62-84 embeds five4KiB page arrays, node.Builder, LeafAppendWork, merge decoder/comparison. Allocation anchors126 and495 admit frame storage through the existing owner.
- TreeDB/internal/valuelog/leaf_append_work.go:27-41 embeds12,800B payload/body/encoded arrays plus lz4.Compressor and FramePreparer. The compressor is an important embedded-state candidate; no field-size measurement was run.

Highest-leverage source hypothesis: review simultaneously retained fixed working states and their actual phase lifetimes before optimizing small resource wrapper allocations. These bytes are admitted/charged; this is not evidence of hidden ownership. Any future sharing/lazy allocation must preserve original owner, work admission, continuation and input/output overlap contracts. This sample does not prove the fixed workspaces cause latency or GC stalls, that any buffer is removable, or a production improvement.

## Exact scope and retention

Each measured closure retained3997 effective source-input hashes, including dependencies/toolchain, and unchanged before/after source plus HEAD/index metadata. Between production latency and diagnostic attribution, the sole effective source delta was the new leased test file's profile aggregation serializer; production bytes were identical. The test file's final measured SHA is efc065bbbe16258db79947d0cca0b130185c8194740f01f6490de29d48faf0fc.

This packet includes selected frozen anchor sources and the effective-input census, not a complete reconstruction of all dirty compiled production inputs. Both measured binaries remain retained locally and excluded from the Git publishable allowlist until root establishes complete source recovery and authorizes release. No paths are released for deletion. Source/Go exclusivity and the new-file lease are finished and released to native owner; native may now change its sources. No production edits, Git/GH writes, cleanup, extra profile/test invocation or final qualification was performed by this lane.

Exact receipt/argv/environment/buildinfo/hardware/process evidence is under clean-cost-v1 and attribution-v2. Raw source/binary identities and initial/final closure checks are preserved. The top-level manifest is a finite relative-path/bytes/SHA allowlist of frozen publishable evidence only; it excludes native mutable directories, caches and .test binaries. Production cost remains refusal/allocation NOT_COLLECTED; diagnostic results do not replace original pre-sprint/main/M0 or final performance/correctness gates.
