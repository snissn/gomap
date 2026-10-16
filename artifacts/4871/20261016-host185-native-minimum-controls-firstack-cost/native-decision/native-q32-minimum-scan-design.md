# Resumable Q32 snapshot minimum: selected design advice

Current source: repair01 manifest `30e0fe7fb53fa4eafef910fa6e50da5d8126cc225341da8de45b6a1d9cd2d206`, full archive `3492a17d4ab432bb8f3089aaba9a8c1ff583a88ca620db1a3747c4d42bc49e5c`. All thirteen inspected files match that manifest. Read-only advice; no source/Go/runtime/Git action.

## Decision

The smallest safety-preserving repair is an inline, saved two-pass acquisition observer inside SAME `nativeMVCCSnapshotMinimumV6`, validated by the existing `snapshotAcquireEpoch`. Admit and load one actual shard at a time; save the index before returning. Keep a single epoch spanning the initial scan, every registry observation and the terminal scan. This removes the **indivisible 256-record minimum unit** without omitting any load or changing Q32. It needs no new global counter, reader registry, owner, lock across yields, or registration protocol. It restores bounded next-shard progress under a stable acquisition epoch. It does **not** prove eventual completion against indefinitely continuing legal acquisition churn.

`db.go:3895` is not an atomic snapshot of 256 counters today: it already loads them sequentially. Safety of the minimum comes from the acquisition protocol plus its epoch checks, not from the loop fitting in one function call. Merely moving its index into state while keeping the epoch sample after that loop would be unsafe.

## Exact current paths

* `native_mvcc_allocator_preparation_v6.go:46–112`: entry header, whole 256-shard scan, then epoch sample; one saved index-generation registry per step; terminal whole scan plus epoch comparison. Both native minimum scan sites are in this method (initial line50 and terminal line85). Its only production consumer is `StepNativeMVCCAllocatorV6` phase4 (line185), which subsequently constructs the original reuse capability.
* `db.go:1796–1798` and `1944–1946`: ordinary and bounded acquisition both increment their selected counter **before** incrementing the epoch, and only then load the view/register a reader. Their exit paths increment the epoch **before** decrementing the counter (`1782–1783`, `1950–1951`).
* `native_mvcc_snapshot_v6.go:139–141`: genuine native Snapshot birth follows the same counter→epoch order; it retains `epochLive` across finite registration. `StepNativeMVCCSnapshotCaptureV6` phase2/3 registers the actual pinned sequence, then `finishNativeMVCCCaptureEpochV6:245–250` publishes completion epoch before counter decrement. Cancellation reaches the SAME finish helper in original close; no early disappearance of an unregistered native reader is allowed. These original producers require no change.
* `lifecycle/registry.go:63–107,179–256`: joining an already-published cohort cannot lower its sequence; first publication and each minimum scan use the original registry mutex. Unregister can only remove a restriction. The proposed outer state still invokes the actual original `MinPinnedSeqWithWorkV6`, under existing short `idxMu`, and saves scalar generation IDs rather than retaining registry pointers.

## Concrete original-state algorithm

Extend the existing inline minimum state with a small phase, `scanIndex`, and completion-valid state. Reuse its `epoch`, `minimum`, generation `index` and `currentSeen`. No allocation or pointer-bearing scan state.

1. After the original entry admission, admit the epoch read/state initialization, sample `E= snapshotAcquireEpoch.Load()` **before the first shard load**, initialize minimum to MaxUint64 and both scan positions to zero.
2. Initial-scan phase: before each actual load, admit one record plus the actual counter load and saved-index/branch traffic. Load exactly `snapshotAcquireRO[scanIndex]`; a nonzero value invalidates the attempt and returns not-ready. Otherwise save `scanIndex+1`. Budget exhaustion returns not-ready with that position intact. Never replay already scanned shards within the same attempt.
3. After all256 loads, admit/compare the epoch against E. On mismatch reset the whole attempt (minimum, generation cursor, currentSeen and scans). On equality enter registry phase. The scalar epoch must be checked on every later entry before using accumulated observations; changes restart, never return a stale partial minimum.
4. Registry phase preserves the existing protected map selection and actual coherent child call. Move to terminal phase only after the original map/current-generation visits finish. **Persist that phase before returning or attempting any terminal load**: the terminal scan must not be preceded by another complete initial scan/map pass on each yield.
5. Terminal-scan phase repeats steps2–3 using the same E and a separate/reset saved index. Any nonzero counter or changed E invalidates the entire minimum. After the final admitted epoch equality, return `(minimum,true,nil)`.
6. If the caller then lacks the following phase4 capability-work reservation, a subsequent call may reuse the completed scalar result only after a newly admitted comparison against E. A completed result must never become an unchecked permanently cached minimum. If E changed, restart. This avoids an avoidable terminal rescan/prefix trap while keeping the evidence valid. Clear/reset the inline observer through existing containing cancellation/reset.

Epoch wrap is the same no-full-uint64-cycle-during-observation assumption already required by the existing equality protocol. Do not weaken it to a small version or claim an ABA proof from a boolean.

## Safety argument across yields

Assume a result passes both scans and the final comparison with E unchanged since sampling E before the first scan. Any acquisition that loads a view during that interval must first publish its begin-epoch increment, contradicting equality. An acquisition that had already published begin before E and remains unregistered cannot complete without publishing its end-epoch increment; absent such a change its counter remains positive throughout the observation, and its actual shard is examined. A begin counter increment delayed before its begin-epoch increment is harmless to the returned minimum because that acquirer has not yet reached view load/registration; seeing that counter simply refuses conservatively. The corresponding exit window is also conservative because completion epoch precedes the decrement.

Therefore every pre-existing potentially old view is either registered and included in the coherent registry minimum, or prevents success by a counter/epoch observation. Held old readers remain in their original registry and must still bound the returned minimum; scanning in pieces does not unregister or substitute them. A start/finish entirely between two calls changes E and forces restart even if all shard samples are zero. A reader completing registration after its registry was visited also changes E and forces restart. No mutex is held between calls.

New acquisitions after the final accepted epoch load are the same original post-observation race as today; this repair does not change the existing capability/publication cut or claim to solve that separate protocol by a cached scalar.

## Work accounting and progress

Both complete scans still execute/admit exactly256 actual counter loads (512 for one successful attempt), plus each original header, epoch comparison, saved-index write and branch traffic. A candidate per-load row is `(1, sizeof(atomic.Int32)+sizeof(saved index)+branch/control bytes)` with actual types and exact local traffic derived in implementation; alternatively conservatively charge the current helper's doubled-counter traffic **plus** explicit saved-index bytes. Do not retain the old whole-loop Reserve, precharge256 and run unmetered, or describe the whole loop as one atomic primitive. Admit phase transitions/epoch reads separately before their effects. Repeated enclosing entry and generation/map/registry prefixes remain charged each call.

With Q32 and a bounded parent prefix leaving at least one child row, each stable-epoch scan call advances >=one index; a successful attempt has finite512 total loads. Per-call unit no longer depends on256. Actual per-call capacity and next-progress expression must be recalculated after added state/branches using source sizeof, and containing `nativeMVCCAllocatorPreparationV6` admission automatically uses its enlarged sizeof. No numeric allowance is selected here.

Continuous acquisitions can change E on every call and prevent completion indefinitely. Resetting is safe; calling that progress success would be false. If the acceptance contract requires success despite that schedule, incremental observation alone is insufficient. It requires a separately proved original retained lower-bound witness or an explicit acquisition/publication handshake that prevents old unregistered views, with its real lifetime and admission. A new shared total counter could avoid the256 scan but changes every ordinary acquisition hot path and contention semantics; it is a larger alternative, not the recommended narrow repair. Returning ready with minimum0 is specifically unsafe as a shortcut: freelist `ReuseCapability.permits` treats minPinned0 as **no constraint** (`generation_v1_txn.go:156–157`). Keep original not-ready on uncertainty.

## Siblings and limits

`MinPinnedSnapshotCommitSeq` ordinary synchronous behavior remains unchanged. `snapshot_publication_work_v6.go:38,59` also has native calls to the same synchronous acquisition sum for **leaf-pin pruning**, distinct from the two minimum scans. The selected finaldriver has no leaf generations, so those branches are absent. A saved old zero result must not be substituted at either mutating pruning site: it lacks the minimum's E-bracketed evidence and live pruning exclusion. Those branches remain separately unclosed for general native Q32 work; this advice does not silently price or qualify them. The ordinary-only call at79 is unchanged.

Other known native minimum indivisible geometries remain: whole `idxAll` walk (`1+len` records) and dirty registry fallback (`17+len(seqs)` records). The selected one-index/fast-only shape fits; large historical fallback vectors or many generations can still refuse. Fixing this early scan blocker is not a general Q32 qualification or sustained concurrent-writer/readers proof.

## Minimal implementation/control packet

Production: native minimum struct/step only; factor an inline scalar scan helper in the same DB file if useful. No change to acquisition producers, global counters, ordinary minima, registry registration, snapshot locks or external limits. Update DB minimum test-only price helper and G/source docs for a per-progress-unit scan plus actual enlarged containing bytes; retain whole-success traffic separately. Adjust current minimum tests from whole-unit-too-large assertions to truthful incremental Q32 assertions.

Focused controls (normal/race, proposed only):

1. Q32 completes both256 scans across fresh work vectors; every actual load is admitted, exact next index persists, first/last shard detected, and total load count is512 for the no-restart success. Byte/record remainder refusal cannot read/advance the next shard.
2. Initial scan: publish acquisition on an already-scanned shard, including start+finish entirely between calls; unchanged-looking counters plus changed epoch must restart. Also exercise incremented counter before begin epoch and completion epoch before decrement.
3. Terminal scan: acquisition starts after its own shard was scanned or registers an older real sequence after that registry visit; final epoch check rejects, subsequent quiet attempt returns the held old-reader minimum.
4. Hold genuine old reader throughout, add/cancel actual partial native registration, then drain it via original cancellation; minimum never exceeds the held reader and native count remains live until registration/cancel actually finishes. No fake successful native publication needed.
5. Finish minimum but refuse caller phase4 tail; next helper invocation under unchanged E can revalidate/reuse, changed E must discard it. Cancel/reset at each scan boundary leaves no resource ownership change.
6. Controlled sustained churn never produces a ready unsafe value; after stopping churn, the SAME observer converges under Q32. This proves conditional convergence, not wait freedom.
7. Existing ordinary minimum/retired-generation tests remain, plus dirty-fallback/large-map refusal retained explicitly.

No implementation or runtime is requested by this report; root selection is still required.
