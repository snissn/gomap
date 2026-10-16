# Independent prebuilder-observer review — final frozen packet02

**REJECT** for truthful source pricing, with one concrete blocker (OBS1). No additional observer safety, alias, lock-order or fixture-oracle blocker was found in the sixteen-path delta. This is a source-only observer review, not native publication acceptance or runtime authorization.

Review mode: `INTERNAL_SUBAGENT_REVIEW`; reviewer `/root/ordinary_review`; actual routed model `gpt-6.1-sol`, high. `candidate_edits_by_reviewer:false`. Manual hosted review cannot pin the explicitly required model and was not requested.

## Exact binding

* Accepted product/fixture base manifest: `83b71641be6460d2ece624fec51b8a84ad3ecea158844eb1c61b0828a72f0fcd`.
* Final packet02 manifest: `e79baff2c224c50e25e355ef7832e1772322cb5f4b139da68080738104bc34c8` — all10346 files.
* Archive: `141b79619e5a85278313a522793799bfecd86526dd89fffc1b6204cdf95725d4`.
* Complete delta: `88144f1dc0ff0c2e72e06e1998b7de80a502e774b6fe8ec60c0e4e9c3bfafb7b` — sixteen changed paths, four new files.

I independently verified every archive file and its exact path set against the final manifest, with zero mismatches. Source review used immutable extraction `prebuilder-observer-review-extraction-02`; no live-worktree equality or Git commit identity is implied. The accompanying JSON records every inspected changed-path hash and evidence-file hash.

## OBS1 — duplicate notification bytes in the reported enqueue price

At `TreeDB/internal/rootpublication/primary_empty_candidate_work_v6.go:193`, `g.EnqueueBytes` already includes `2*g.NotifyBytes` for the two permits in `EnqueueNativeBuiltWithWorkV6`: `notify` at `native_builder_work_v6.go:139` and `startNotify` at line152. Line198 adds another `g.NotifyBytes`, and lines199–200 incorrectly describe it as an additional original enqueue notification.

The actual nested `enqueueLockedWithNativeReceiptV6` has no notification: its post-activation tail installs pending/visible state and the timer, then returns (`coordinator.go:408–437`). The wrapper starts the attempt with the already admitted `startNotify`, then commits the already admitted `notify` and wake (`native_builder_work_v6.go:180–182`). There are two notification admissions, not three.

Every ready coordinator observation therefore overstates `EnqueueBytes` by `112 + 3*8 = 136` bytes on the pinned layout, and that row feeds group3 `computedBytes`. This is conservative overcounting, not unsafe underadmission, but the observer/packet claims actual source `Reserve` traffic and must be truthful before collection.

**Correction:** remove the trailing `+ g.NotifyBytes` from line198 and correct the comment. Keep `EnqueueRecords=46` and the fixed F record constant281. The 46 records already include the empty-Set operation's3 records and precisely two notification permits. Bind/review the narrow successor; do not collect from packet02.

## Coverage and source conclusions

I read the complete base83-to02 observer patch and all sixteen changed source paths, then independently compared the five-path01-to02 correction. That correction adds actual publication/receipt identity checks, planned map phase/presence/length, correct RAM-bank image provenance wording and scalar outer-cursor traffic; it does not change producer authority or ownership.

I traced the original final-driver/final-basis/Cut operations and the observer's F rows, canonical allocator bound/R, claim/activation A, installed-frame V, snapshot Z, coordinator acquire/enqueue/release/completion and timer/channel traffic. Apart from OBS1, the reviewed rows mirror their original source expressions; conservative original N/claim bounds remain labelled bounds. F record algebra remains `281+D+2K+23P+8M+3S+N+Z+V+A`, with R already included. A missing applicable row is never a complete total.

The observers reserve before header reads, protected visits and accumulation; their results are fixed by-value scalars. Original private operands and original locks are reused; no observer obtains a lease/owner/root/Set edge, invokes a callback or syscall to price it, exports a pointer/path/FD, or retains a cursor/lock across return. Mutable owner binding/bucket/receipt walks complete under their original lock or report incomplete; immutable frame ordinals restart and price revisited links. Active enrollment/foreign lease capability remains unsupported rather than taking a reversed lock order. Owner accounting envelopes are kept separate from physical charges and concrete policy reservations.

The sole test attachment remains after the actual phase26/candidate8/seal13/two-capture refusal checks. The original33 versions, discard floor32, selected/anchor operands, old reader, caps, ACK0/refusal, newer durable writer, exact stale cancellation and reopen assertions remain intact. No production call site invokes the added ForTest helpers. No production protocol/representation change is accepted by this review.

## Important incomplete evidence — expected, not additional source blockers

* `MaintenanceNativeSnapshotPriceForTestV6` lines284–298 visits all256 acquisition shards at one record per shard when `next` is nonnil. It cannot complete in Q32. Its refusal is honest and effect-free; the caller forces `applicableComplete=false`. One observation may therefore still yield no aggregate F. Do not widen its budget or infer absent Z.
* Private captured FreelistTxn maps, private sink validation map and conditional planner clone remain explicit opaque backing, even empty-but-nonnil. Nonempty resource logical graphs, charge-transfer dedup and non-atomic owner observations also remain named gaps. `retainedUnionComplete=false` is correct; neither an Owner.Bytes envelope nor raw/rounded subframe sizes close the union.
* Equal before/after scalar basis does not prove an atomic whole-graph snapshot. Future Finish/last-reader/last-owner geometry after another ordinary writer is unobserved. The printed conditional original chain and sticky hard-I/O/incomplete Close distinction do not constitute runtime completion evidence.

## Validation and proposed observation boundary

Both compile receipts were inspected: packet01 exit0 in96.542s and packet02 exit0 in93.159s, with zero test RUNs. Each reports actual leader join, empty process group and no remaining direct/adopted children. Final compile before/after manifests independently equal the final10346-file manifest. The pinned Go executable hash is `d68b7abbc40d0844f673f6cf06ae3cded225c50437c6454fa37ef178d079fe65`; compiler hash `3bb421d581f60b9ab48c2c73adf5c018557e89af32ad018d9c4417afa4bf90c1`, independently verified. This is compile evidence only; I ran no Go command or test.

The unexecuted proposed command (`proposed-actual33-command.json`, SHA256 `81e52f221df526a6541cb04f8eae0b9913c1a2ab2d5b2d05bfe38aee6a22644b`) selects only `^TestNativeMVCCRealBackendRouteRefusalPreservesCustodyV6$`, count1, pinned Go1.26.3, GOMAXPROCS2, package deadline3m and outer deadline240s. It keeps the original fixture caps and refusal/new-writer/stale/cancel/reader/reopen boundary. Its intended scope is coherent for ONE previsibility observation after OBS1 correction/rebinding and a separate root grant. This rejected packet authorizes no execution.

No actual33 observation, complete retained total, numeric allowance, publication/Finish/ACK, Q32 qualification, ordinary/native integration, performance, CI or mergeability claim is made. Original Q32/1MiB33/80/4096 remains RED/ACK0. Review/source/runtime lease released; no candidate edits were made.
