# Original native growth budget-refusal repair

Base: root recovery 4aa7ebb23fc925a6dcc8d8abee3d2c3bfd6efe35. This packet repairs previsibility construction/refusal propagation only. The native final driver, activation, enqueue, publication and physical Finish were not executed. Q32 remains RED, ACK0; all original product limits are unchanged.

The baseline failure is retained by root at native-build/current-previsibility-4aa7e. Owner.addWithWork already returned `(false,nil)` when its complete original atomic resize/possible rollback did not fit. AddWithOrdinalWork/AddPendingWithOrdinalWork converted this into the hard unsupported error ErrWorkRequired. Consequently the actual33 route stopped at phase15/16 and the partial-token12 boundary errored before allocation. This was a propagation defect, not evidence that the actual current geometry required a larger quantum.

## Connected repair and custody

The two original adapters now return `(ready,error)`, including genuine ordinary nil-work callers which still invoke the original Add/AddPending. All26 direct production callers are classified in native-build/growth-yield-repair/growth-callers.json. Typed continuations return their existing unready result before allocation or phase advancement. Unsupported lease/shape and actual capacity/closed/stale errors retain their original errors.

Three existing error-only helpers cannot directly return ready: newResourceAllocationWithWorkV6, newResourcePinnedBackingWithWorkV6, Arena.AdmitRootMetadataWithWorkV6. They transport only effect-free budget refusal as distinct ErrWorkBudget. Their original typed parents recognize that exact marker and yield. No caller globally converts ErrWorkRequired, arbitrary errors, or cleanup failures into a yield. Arena root-metadata header/validity budget refusals use the same marker. The existing kind-view Set wrapper passes the marker through to its typed clone parent.

Nested parent dispositions:

- StableIndexTokenConstructionWorkV6.Step handles each of its three original allocation calls separately. A later refusal retains the SAME token and already-created backing; its original work-bearing capture disposer owns partial cleanup. It does not reset the phase or release the token on a yield.
- EmptyDependencyManifestWithWorkV6 returns unready before manifest allocation.
- CloneAdmittedStableResourceSetWithOrdinalWorkV6 keeps both original guarded sibling-edge rollback paths: empty-directory refs and immutable kind-backing refs. Each rollback was already admitted before retaining, cannot be last under source.mu, and still precedes the typed unready return. No clone escapes on refusal.
- StepNativeMVCCCandidateV6 consumes only the root-metadata budget marker before allocating/installing primaryStateRootV5.
- Aggregate pager bank/node construction returns unready under the SAME arena→table lock scopes before the concrete constructor, serial/table count or metadata accounting changes. A fresh vector can resume the same original operand.
- Materialized zipper frames, allocator reservation scratch, bank claims, prepared transaction/promotion, graph directory scratch and seal/candidate metadata now preserve typed unready/error results. No new control, owner, registry, physical-resource edge or private cleanup authority is introduced.

No production struct field, allocation size, retained backing or containing-frame layout changed in this repair. Existing unsafe.Sizeof admissions and actual owner accounting remain the same. Already-admitted physical/metadata resources retain their original disposing parents across yields. Error-only marker storage is one package error value, not per-candidate retained metadata.

## Exact fixture correction selected by root

Root accepted the source-supported proposal in growth-yield-boundary-proposal.md before its application. The original test now has six cancellation cases on the SAME fixture:

| Case | successive record limits |
|---|---|
| before-token | 4 |
| growth-refusal | 4,12 |
| token-only | 4,13 |
| before-pinned-backing | 4,14 |
| before-retain | 4,15 |
| after-retain | 4,15,1 |

Phase1's original12 records are followed by three separately admitted original empty-owner growth headers, one each. Thus15 is its actual allocation boundary. Original exact-record accounting, ready=false/error=nil, refs1 versus refs2, zero-work close refusal, metadata bytes, stable capture count and wrapper-borrow restoration assertions all remain. All turns remain under32/1MiB. No observation API was added.

## Bound verification and limits

All commands/toolchain/environment and per-run all-source manifests are retained in native-build/growth-yield-repair. Each runner records pinned Go1.26.3 compiler SHA, GOTOOLCHAIN=local, GOWORK=off, task-local cache/tmp, GOMAXPROCS2, -p1, -timeout3m and outer230s. It joins the child/process group before taking its after manifest. Original source-baseline.json binds all10,330 starting source files; changed-files.json identifies26 modified paths, with before/after SHA256; repair.diff preserves reviewable bytes.

normal-initial PASS: pure original atomic-growth/yield/unsupported plus unchanged actual33 route and copied-parent controls. All10,330 before/after source files equal. The actual33 route reached phase26/candidate8/seal13 and original work-bearing-caller refusal (157 calls in this first run). No native activation/publication.

normal-complete PASS: the same route plus all six partial-token cancellation cases and pure nested-allocation/pager-growth causal controls. All10,330 before/after source files equal, child joined; 47.5985s including compilation. Actual route159 calls, original caps, phase26/candidate8/seal13; copied-parent stale/cancel controls and existing newer-writer/old-reader/reopen assertions pass. Different call counts reflect ordinary fixture scheduling/layout; neither changes a gate.

race-complete PASS: the exact same focused controls under -race, including all six cancellation boundaries and actual33 phase26 safety, in143.5798s including compilation. All10,330 before/after source files equal and match normal-complete exactly; child joined. No race report. Exact result is recorded in race-complete/result.json and run.log; final freeze receipt binds it. No broad suite, benchmark, native publication, successful fence/Finish/ACK or product-qualification claim follows from this narrow batch. Arbitrarily many bindings can still make an atomic growth unit exceed32 and refuse without progress; this repair preserves that explicit geometry limit and does not broaden supported histories.
