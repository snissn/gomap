# Concurrent native service construction refusal

Source-only inquiry at `0a8323f7ca6f6128d53dfca43a408c7e443d39ab`, including main `f429275bbf0c0bda2cedfc9d5862200cee067ff5`. Staged public harness SHA256 `478e57f2e84a09caafaff4c2149f40aacd76715adfc2196a268acf4f005963df`. No Go, test, build, profiler, GitHub operation, or canonical edit was performed. Existing four staged paths remained unchanged during this inquiry.

**Decision:** repair the scheduling gap between producer maintenance and the already serialized one-shot native mutation. This route has stale advisory preparation across its collection mutation queue; it does not have four speculative native transactions waiting at that queue. Preserve the exact construction refusal. The logs establish real product failure, not a safe concurrency capability or acceptance result.

## Evidence and route

Both fixed native diagnostic cells completed only 20 of 32 public requests with four writers. Cell02 includes a next-state construction refusal `total=33597504 typed=29702016 keys=1024 catalog=81920 domains=13`, against both limits `33554432`: an excess of 43,072 bytes. Later next-state totals include 33,931,680 and 34,324,896. Other refusals expose only `immutable memtable capacity exhausted`. Source cannot locate those plain refusals to a particular allocator without additional evidence; do not silently attribute every refusal to the descriptive check.

1. `TreeDB/documentservice/shared_cow_write_bench_test.go:123–125` deliberately constructs `New(manager)`, as the original read fixture does. `service.go:94–104` supplies no deferred-build worker handle in this constructor. Substituting the command-owner constructor would change the tested public integration. Native collection producer maintenance nevertheless runs below it.
2. `service.go:416–425` takes `Service.writeMu.RLock` for deferred requests, allowing the four callers to reach the collection concurrently. `service.go:566–579` selects `InsertBatchWithStatsValidatedFloat32Projection` for ordinary ColumnGraph. The request's initial document/embedding preparation occurs before native transaction creation. No capacity retry exists on this path; the duplicate-ID fallback is a separate semantic case.
3. `collections/native_existing.go:23–29` sets the coarse `hasNativeVectorIndexes` flag for **any** vector declaration, including this ColumnGraph declaration. `native_hnsw.go:1203–1212` consequently calls `lockVectorIndexMutation`; `vector_index.go:2115–2119` acquires the DB/collection-shared `nativeHNSWMutationMu`. This is a real shared gate, not one independent lock per newly opened handle.
4. Crucially, `native_existing.go:85–90` orders `PrepareNativeProducer()` **before** that gate, and calls `insertPrepared` only after acquiring it. `native_json_insert.go:26–35` then opens and closes the transaction. `native_json_txn.go:99–105` selects `BeginPreparedNativeConditionalTxn`, and `caching/cow_native_txn.go:28–42` admits/captures the opening without another pressure check. Thus waiters have stale pressure observations, but have not yet pinned native transaction openings or built native row/index deltas.
5. `caching/cow_native_txn.go:55–62` uses a 75% current-debt watermark. `:103–108` returns immediately below it without taking `nativePressureMu`. Above it, `:117–148` serializes one checkpoint plus actual semantic cleanup, then **releases the mutex before the producer enters its mutation gate**. It makes no reservation and promises no future headroom. Several callers can pass the same low observation, then perform successive gated mutations without another check. A different caller may also run producer-triggered checkpoint concurrently with the active gated mutation.
6. The final publication is not incorrectly applying a stale complete engine. `caching/cow_batch.go:93–130` validates original dependencies and passes the latest semantic state and paired physical basis to reprepare. `collections/native_json_txn.go:311–350` composes the immutable logical delta onto that latest collection, then calls `nativeTypedEnsureConstructible` **before canonical assignment**. Its refusal is the intended integrity boundary.

`native_typed_engine.go:21–108` sums every pending typed delta's original `buildCapacity`, applicable retained/basis costs and the shared operation peak. `:350–381` adds handoff/build, descriptor-key and catalog projection requirements. `:392–421` uses that same inventory for observation and next-state validation. `native_engine_build.go:85–126` actually reserves the corresponding builder cohort on the maintenance allocator. Therefore the 33,597,504 value is a bound for constructing the complete next physical cohort, **not a measurement of four simultaneously retained transaction heaps**. Disjoint document keys avoid logical write conflicts, but share this finite physical-construction capacity.

The single-writer lifecycle does not cover this gap: the next request's pressure observation follows the previous request's commit and Close. Four callers can enqueue after earlier observations and consume more than the remaining 25% headroom. Neither the mutation mutex nor the maintenance mutex currently spans the complete observation-to-mutation interval.

## Ownership assessment

No missing transaction Close was found on the served insert path. `finishNativeJSONInsert` defers Close (`native_json_insert.go:35`); commit also closes bindings/staging (`native_json_txn.go:437–456`). `closeLocked` detaches aliases, releases typed input, private custody, core opening and receipts (`:684–726`). `caching/cow_native_txn.go:196–231` queues private publication cleanup before closing the original opening; `cow_native_semantic.go:164–197` detaches and queues successor/custody before releasing its attach-time opening. The pressure observer itself releases its borrowed cut before checkpoint (`cow_native_txn.go:84–87`). These source paths support a scheduling diagnosis; they do not prove that every asynchronous refund completed in the failed runs. Retain Close-zero and failure-cleanup assertions.

## Compare the three repair shapes

The initial DB-turn suggestion is withdrawn as the preferred correction after auditing cross-collection scheduling. The least-global choice is **C: fresh pressure revalidation under the existing collection gate, with drain outside that gate, before any original transaction or callback starts**.

**Current owner scope.** `nativePressureMu` has one production declaration (`caching/cow_cut.go:201`) and exactly one lock/unlock pair (`cow_native_txn.go:117–118`). It is DB-wide. Only the above-watermark branch takes it, covering reobservation, optional remembered-state return, Checkpoint, semantic cleanup join and final observation. The mutex ends before capture; below-watermark observations bypass it. Existing advisory callers are caching `BeginNativeConditionalTxn` (:20) plus 13 collection sites: create (`native_json_public.go:94`), insert (`native_existing.go:85`), update/delete/single mutation (`native_json_crud.go:36,122,294`), typed grouping (`native_typed_upsert_group.go:73`), HNSW create/drop/build/save (`native_hnsw.go:57,72,749,1485`), HNSW schema (`native_hnsw_schema.go:16`), text DDL (`native_text_ddl.go:25`) and text rewrite (`native_text_rewrite.go:98`). No caller currently receives a retained producer turn.

| Shape | Scheduling cost / proof | Disposition |
|---|---|---|
| A: take existing collection mutation gate before pressure/Checkpoint | Keeps unrelated collections independent. No direct gate reacquisition was found in native builder, handoff or native state/custody destruction. But synchronous error notification can reenter public vector APIs and reacquire that same gate; see concrete edge below. | Do not adopt without repairing/proving callback reentry too. |
| B: retain DB pressure mutex through one-shot commit/Close | Same-collection native planning is already at most 1, but K unrelated participating collections become at most 1 staged preparation/commit. Holding it through durable completion also prevents overlap feeding the existing append/group-commit owner. Wrapping grouped APIs can inhibit aggregation; callback-bearing updates risk nested-write deadlocks. | Reject blanket all-producer policy. Even ordinary-ColumnGraph-insert-only participation introduces unmeasured cross-collection serialization. |
| C: fresh non-draining observation under collection gate; unlock for drain; reacquire/revalidate before capture | Uses the existing per-collection exclusion only for observation and the already serialized mutation. Checkpoint and cleanup retain existing off-gate order. Unrelated collections retain preparation/group-commit concurrency. | Select this bounded correction. |

### Why A cannot be declared safe from the direct build call graph

`caching/cow_checkpoint.go:43–85,147–161` enters flush/write/writer owners, captures/rotates the frontier and builds immutable native state. `collections/native_engine_build.go`, `native_engine_handoff.go`, `native_engine_state.go:99–174` and `native_mutation_custody.go` do not directly acquire `nativeHNSWMutationMu`; immutable construction itself does not require it. The obstruction is a callback, not a demonstrated direct gate dependency.

The concrete possible cycle is:

`collection gate G held by producer -> PrepareNativeProducer -> Checkpoint / joinSemanticCleanupFrontier -> cleanup worker Finish/error -> configured synchronous NotifyError -> public native vector read/load/status takes G`.

`caching/cow_semantic_cleanup.go:110–125` explicitly runs the user notification synchronously on the cleanup worker, before marking the record completed (:155–161); the join waits for that completion (:170–188). `collections/native_hnsw.go:1534–1536` takes the collection gate for public native HNSW status; public native load takes it at :844–846. The public `BuildVectorIndex` entry also takes it before native metadata validation (`vector_index.go:998–1020`), including an attempted build on the current ColumnGraph collection. Therefore even a callback operation destined to return unsupported can wait on G first. This is a possible legal callback schedule, not a claim that the diagnostic configured this callback. Public read reentry is explicitly anticipated at `TreeDB/public.go:590–595`. The owned cleanup guard prevents recursive Close/high-pressure preparation self-join, not every native vector read's gate acquisition. Merely moving the existing call under G would introduce this deadlock possibility. No original blanket off-consumer-lock comment was used as a substitute for this source edge.

### Concrete C algorithm and affected seam

Keep ordinary `PrepareNativeProducer` and exact final admission. Add a narrow **non-draining** revalidation operation over the existing `cowNativePressureState` inventory, forwarded at the existing `TreeDB/db/native_semantic_state.go:240,305` bridge. It must return a scalar observation identity, release its borrowed cut before return, and must not acquire `nativePressureMu`, run checkpoint, join cleanup or invoke an application body. Reuse `nativeProducerPressureState` and the watermark formula; do not add a second estimator or reservation owner.

At `collections/native_existing.go:85–90` for the already gated ordinary native insert:

1. Perform preparation outside G as today, retaining its **post-preparation scalar observation identity** (logical sequence, paired physical-basis token and semantic construction inventory; no retained snapshot).
2. Acquire G. Before `insertPrepared`, observe current pressure freshly. If it is below the watermark, continue with exactly one original capture/stage/commit/Close. If it is pressured but is the exact unchanged identity just prepared/drained, continue to the existing finite admission as well: old readers may legitimately prevent reclamation.
3. If it is pressured and changed since preparation, release G, invoke the existing at-most-one drain on that fresh state, get its post-preparation identity, then reacquire G and repeat **only this pre-opening revalidation**. No transaction, document callback or canonical operation has begun; none is replayed.

The unchanged-after-one-drain exit is mandatory. `nativePressureRemembered` already suppresses no-progress repeat checkpoints; blindly looping on `underPressure` would otherwise spin forever around a held-reader state. Reuse that existing no-progress policy, with value-only observation output, rather than invent another persistent ledger. Do not introduce a new retry quota or governor: iteration is restricted to changing pre-opening observations; unchanged no-progress observations exit to original exact admission/refusal. No body-work credit is renewed. Other collections can still advance shared debt between validation and final publication, so original exact refusal remains necessary; this correction promises no universal progress under arbitrary concurrent owners.

Use one collections helper consistently at all **four existing one-shot same-gate CRUD sites**: insert (`native_existing.go:85–90`), batch update (`native_json_crud.go:36–41`), batch delete (`:122–127`), and conditional single delete (`:294–299`). Single update already delegates to batch update (`:333–340`), so it needs no second observation owner. The helper returns the held collection unlock function and existing `vectorMutationHeld` flag, after pre-opening validation; callers keep their current defers, original transaction/body and maintenance behavior. If the existing coarse gate is not needed, preserve ordinary advisory preparation and avoid inventing a new gate for plain/no-vector collections. This does not serialize different collections or grouped typed requests. Grouped typed admission has a separate queue/capture lifetime (`native_typed_upsert_group.go:73–82`) and remains outside this correction. Existing application-body retry behavior elsewhere is not changed; this helper itself never retries a body or callback.

### Small concrete bridge API

Keep the original `PrepareNativeProducer() error` for existing callers, forwarding internally to the result-bearing implementation and discarding its scalar result. Add these two fixed bridge methods and forwarding methods, with no callback argument:

```go
ObserveNativeProducer() (NativeProducerObservation, error)
PrepareNativeProducerObservation() (NativeProducerObservation, error)
```

A comparable value-only token can reuse the existing state fields without exposing a capture or receipt:

```go
type NativeProducerObservation struct {
    LogicalSequence uint64
    Basis StateToken
    Semantic NativeSemanticProducerPressure
    ReservedBytes uint64
    Sources, Generations int
    UnderPressure bool
}
```

`StateToken` and `NativeSemanticProducerPressure` are existing scalar identity/inventory types. `ReservedBytes`, raw source/generation counts and `UnderPressure` use the existing raw watermark inputs; exclude historical `PeakBytes`. No token field grants publication authority or reserves capacity. Full value equality identifies unchanged relevant pressure; no pointer, map, registry, retained engine, release method or persistent new policy state is needed.

`ObserveNativeProducer` calls the existing retained-cut observation and computes the same watermark. It **must never take `nativePressureMu`**, checkpoint, join cleanup, or wait for an application writer. It retains the ordinary brief cut/budget/basis read synchronization and manifest-read work; “non-draining” does not mean a lock-free/I/O-free observation. Keep observation errors as errors, without capacity retry.

`PrepareNativeProducerObservation` is the original preparation algorithm with value-result exits: return its observed value on the low-pressure exit, its rechecked value on the no-pressure/remembered-state exits, and its final post-checkpoint/post-cleanup value after a drain. It retains the original `nativePressureMu` scope and cleanup self-join guard. Returning the final observation is essential: the helper compares against **the exact state already prepared**, rather than an unrelated later observation or just the predrain sequence. No pressure mutex is held across the caller's gate, opening or commit.

Collections helper pseudocode:

```text
prepared = PrepareNativeProducerObservation()       // outside G
for {
    acquire existing G
    fresh = ObserveNativeProducer()                 // no pressure-mutex wait
    if observation error: release G; return error
    if !fresh.UnderPressure || fresh == prepared:
        return held G                               // begin original body ONCE
    release G
    prepared = PrepareNativeProducerObservation()   // off-G, existing one drain
}
```

Preserve the existing no-gate fast path, empty-input handling and pre-opening error cleanup. Any fresh under-threshold observation can proceed immediately even if its identity changed. An unchanged pressured post-preparation observation proceeds once to exact admission/refusal, including legitimate held-reader pressure. A changed pressured observation releases the gate before all maintenance. This is the smallest proposed correction; no implementation or runtime proof has occurred in this inquiry.

Selected lock order: **G -> fresh borrowed scalar observation -> release observation**; if drain is required, **release G -> existing pressure mutex -> Checkpoint flush/write/writer owners -> semantic cleanup join -> release pressure mutex -> reacquire G -> fresh observation**; only then the unchanged original capture and mutation order. There is no G-to-checkpoint/cleanup or G-to-pressure-mutex wait. For this four-writer same-collection fixture, planning/capture concurrency is 1 before and after; cross-collection preparation remains independent. Additional scans/revalidation and possible maintenance waits have real cost but no timing estimate is justified from source.

## One next discriminating public test

Use one correctness test with the exact existing 4-worker, 32-disjoint-batch ×128, 4096-document public service load, same `New(manager)`, durable profile, schema and default limits. Keep each request exactly once and preserve every response/ID/count, Optimize, reopen and Close assertion. A test-only scheduling hook immediately after off-gate preparation/before G should hold contenders while another original request commits; release them into the actual public insert route. Scalar observation/gate-entry logical-sequence events must show that an intervening pressured state forces off-gate drain and revalidation before opening the waiting request. Do not add retained cuts merely to observe order.

Required result: all 32 requests accepted without caller/body retries at unchanged limits; exact documents durable after reopen; zero retained budget owners after final Close. Include an unchanged-after-one-drain/held-reader control to require termination and original finite refusal, with failed-private-work cleanup intact. If C still refuses, retain that result and identify the exact refusing allocator/cost at this public boundary; do not lower the watermark, enlarge the cap or rerun favorable samples. Hybrid's separately proven cold-reopen text-candidate refusal remains outside this correction and blocks its own product gate.
