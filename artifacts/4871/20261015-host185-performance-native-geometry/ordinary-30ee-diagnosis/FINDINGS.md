# Read-only 30ee arena Finish diagnosis

Frozen HEAD `30ee954b974077b8968cdfb836e03058d9934ed1`, clean before/after. No product/test source edits, Git/index/GitHub mutations or native cross-port. The one bounded batch used Go1.26.3/GOMAXPROCS2/-p1, count20,90s test timeout/180s outer bound. All20 pass (0.121s package); hosted failure did not reproduce. `logs/arena-finish-diagnostic-01/` preserves argv/env/raw/exit/full before-after identities. `overlay-receipt.json` identifies artifact-only failure-state/size/stack diagnostics; grants,4096turn bound and all original assertions unchanged. This is instrumented diagnostic evidence, not exact-source qualification.

## Confirmed exclusion

The newly added pointer fields are in `StableResourceToken` and `StableNamespaceToken`. This fixture constructs neither: it constructs original PRIMARY Arena/Pager/owner, `StableNamespaceCreationProof`, one retained original parent,80 retainedalloc capture enrollments and one admitted external receipt. Namespace creation proof is a distinct struct, not an embedded namespace token. Its metadata charge uses its own actual sizeof, copied name and original file descriptor charge.

Source diff616fd→30ee is empty for the complete fixture, owner/receipt, arena close, pager close, namespace-proof release and retainedalloc removal paths. Added generic diagnostic pointers cannot enlarge the atomic work reservation or admission in this path. There is no evidence supporting quota expansion.

## Unconfirmed scheduling cause

Actual bounded path: `receipt.ContinueWork` phase0 → Arena.CloseWork phase1 → Pager.CloseWorkInto phase1. Pager first signals its existing growStop, then polls growDone; if worker has not exited it reserves1record/128bytes and returns paused without changing physical custody. The fixture retries up to4097 times without yielding; a bounded poll count does not guarantee independent goroutine scheduling. Ordinary pager close explicitly calls runtime.Gosched between paused turns. Hosted0.00s failure is consistent with this path but contains no phase/stack evidence, and the one allowed local batch did not reproduce it. This remains a hypothesis, not the established hosted root cause.

If paused there: last owner edge is already consumed once (refs0), external admitted receipt retains same owner; actual Arena/Pager, namespace proof/parent and metadata charge remain on original owner. Grower is same producer worker, no new cleanup owner or moved error. No mappings/files/refunds progress before its completion, and ordinary last release remains refused as original test requires. Whole-call measured1record/128bytes is far below32records/1MiB; retry count, not work amount, would fail. Other release phases are monotone over fixed mappings,4096 original chunk slots and80 capture bindings, with actual headers fitting the existing grant.

## Narrow proposed next action

Obtain a deterministic original-grower pause/release control or retained failure-only phase/work diagnostics to prove the refused phase before attributing the hosted failure. If growDone wait is confirmed, add fixture-only runtime.Gosched after paused/debt turn, retain original4096 bound,32records/1MiB grants, repeated refused-bind/no-effects, exact last-edge refusal, multiple-turn requirement, namespace/arena alias scrub, all80 held-capture charges and external final containing refund. Do not change production bounded Close, raise budgets, pre-quiesce away the phase under test or weaken custody assertions.

No source repair is authorized/applied in this diagnosis. Root owns complete current-head inventory and next coherent assignment. No runtime process remains active.
