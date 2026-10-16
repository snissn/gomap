# Resource Finish control: source diagnosis

The failure is a stale fixture boundary assumption. It does not demonstrate loss of original custody. Exact frozen source is e434542a59d55cf67c90d6a16c5787abeb908efda4c1c705033658b9ba8e0577; inspected file hashes are bound to that manifest in the JSON companion. The partial control packet is unchanged. No source edits, Go execution or retry occurred.

The first3-record call funds descriptor/scrub1 (`primary_transaction_construction_v6.go:461`) and DropToReleaseQueue2 (`primaryarena/arena.go:767`). Drop then calls validWithWork→slotWithWork→PrimaryReadRootOwnerWithWorkV6 (`arena.go:773,150-165`). The next table-header1 reservation (`pager/primary_read_root_v6.go:206`) refuses: Reserve failure does not advance work (`iterator/ordinal.go:80-86`). Consequently there is no ref decrement/enqueue/RootDrops increment, c.root is not cleared, and release is never called. The raw calls0/effects0 matches this source path. finishBusy is cleared; descriptor, root, queue, Set and metadata remain original; the optional retirement-done scalar may advance harmlessly before this refusal. physicalCleanupError stays nil.

Fresh fixture geometry is source-determined: one constructed RAM root at the bucket head, no certificate/image/groups/edges, no enrollment/bindings. The full before-callback cost is descriptor1 + Drop2 + table-header1 + matching-link1 =5. At limit5 the root drops exactly once and c.root clears; callback calls1, but its own Reserve1 refuses, so effects0. This is the originally intended callback-boundary assertion. Preserve3 as an additional early lookup refusal rather than replacing its meaning silently.

| Stage | Records | Original custody/effect |
|---|---:|---|
| Descriptor + Drop + actual table lookup | 1+2+1+1=5 | Drop only after both protected lookup reads; queue retains original last root. |
| Callback dispatch | 1 | Calls increment before its own reservation; effects remain0 on refusal. |
| Queue header including empty check | 10 | 2+releaseCells8 before queue inspection. |
| Last-bank lookup + pager node unlink | 2+2 | Source table header/link plus removal header/link; native accounting returned after alias scrub. |
| No-enrollment accounting completion | 5 | Bind1, remove1, active-enrollment header1, empty cursor-init1, reset1; original queue RemovalWork. |

All byte expressions are in JSON stage_prices; no scalar exemption or reduced production pricing is proposed. A 1MiB fixture byte cap remains unchanged. These record constants apply only to the stated source fixture; additional links/bindings require their real work.

The existing retry-only11 call remains appropriate **after** the5 call: descriptor1+callback1 leaves9, and queue10 refuses before physical mutation. It must keep original constructor, drop count, metadata and bank-free counters, calls2/effects0.

The later32 call has a second latent fixture assumption. With root already dropped, it spends descriptor1+callback1+bank queue(10+lookup2+unlink2)=16. A second queue call spends header10 plus exact accounting5, total31. Bank and pager node are now physically detached/scrubbed, accounting is complete, BanksFreed advances once. The callback loops again and the required empty-queue10 observation refuses. It returns false,nil with effects0, calls3, SAME constructor/Set/queue custody and no terminal. Completing all these operations in one call would need41 records:1+1+14+15+10. That41 is a diagnosis, **not an allowance proposal**. A fresh unchanged32 call spends descriptor1+callback1+empty-queue10=12, releases/checks the SAME empty Set once, increments effects to1/calls4, clears original aliases and returns the original terminal. No root drop/bank free repeats.

The failure branch is distinct: after3 then5, its32 call spends descriptor1+callback1, increments effects once and returns the injected cause **before queue drainage**. finishPrimaryStep records the first physical error, preserves constructor/queue/Set, clears busy. Its zero-work retry and Abort return that same first receipt without replay. This is retained error debt, not successful teardown.

Sibling controls do not require larger limits: the selected binding control PASSED and owns an independent visible root, so its Finish Drop is nonlast and its queue is empty (one10-record check);32 fits. The ordinary refusal sibling uses its original1/100 adapter vectors and does not assert native32 completion. ContinueFinishWorkV6 uses this same resumable queue, then detaches physical aliases before separate original accounting completion; no one-call promise is present. Ordinary ReleaseStep cannot drain the private native queue and the existing counterexample must remain.

Recommend only an explicitly selected test-fixture repair in durable_root_transaction_work_callbacks_v6_test.go: retain3 as early refusal with actual unchanged-root assertions; add5 to preserve the exact original callback refusal/custody assertions; retain11 queue pre-effect refusal; split success into the two source-required32 continuations, asserting SAME constructor/Set, one RootDrops, one BanksFreed, zero early Set effect, final one Set effect and terminal idempotence. Keep the physical failure/zero-work/Abort first-cause assertions. No higher final allowance, nil-work native fallback, new owner or production patch is justified by this failure.

Proposed later verification is exact `^TestPrimaryFinishResourceRefusalAndFailureCustodyV6$`, normal then race, both required children, pinned existing environment and source/process binding. This needs a new root grant; the consumed19-call sequence is not reusable. Q32/1MiB/33/80/4096 remain unchanged RED/ACK0; no genuine native publication/Finish qualification follows.
