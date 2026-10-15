# Ordinary lifetime successor handoff

Worktree: `/mnt/fast4tb/gomap-mvcc-4871-20261015/ordinary-repair`.
Unchanged parent HEAD: `a8c52ed66fc587d4823f020cf15ea1c3ee8c1ebc`.
Sole ordinary writer completed source edits; no Git/GitHub mutation, native edits, service action, cleanup or timing campaign.

## Frozen source

`freeze-final.json` covers all 10,242 tracked and untracked source files. SHA256: `a32742f07bc4b8695ffa2c67bc4aafc49879ca205e6fb0cac927ae30deecdf77`.
`changed-files-final.json` lists the 27 changed paths and their exact hashes, including three new source/test files. All remaining source files retain parent bytes. Earlier freeze/receipts remain preserved and are not exact-final evidence.

## R4 / R5 implementation

Every production replacing state publisher has an explicit rootReuse -> db.mu capture boundary. Current-state consumers acquire the SAME selected ValueLogSet before unlocking and release that actual owner after the last consumer; fallible releases join returned errors. The inventory includes rewrite user optimistic/serialized, system and collection application, CompactIndex, ordered-root build, compact audit and both carry readers. Online dictionary preparation owns one actual Snapshot and uses state and pager from that same capture; nil/error paths do not fall back to shallow DB.State. Offline inputs remain protected by their original offline scope. Topology-only audit and leaf state copies preserve the SAME primaryRoot edge, without acquiring or dropping a replacement owner. Snapshot capture cannot overlap replacement publication before its exact Set pin.

`callsite-disposition.md` and raw `all-set-uses.txt` / `callsite-search.txt` retain the complete production-store and Set-consumer audit. DB.State remains shallow borrowed metadata; State, ValueReaderForState and lifecycle docs describe the exact boundary. No new quota domain, publication framework, publisher, or generic budget increase was introduced.

Deterministic actual-path controls cover ten current-state consumers interleaved with real Refresh, real outer-leaf reads, three topology publishers blocked during actual snapshot capture, actual PRIMARY root/slot churn and delayed iterator/metadata/governor drain. Snapshot.Close still invalidates iterators immediately; iterator Close remains the actual last retained-certificate refund boundary.

## Hosted sibling repairs

All shared completed-pair, crash-pair, offline scope and direct offline-operation fixture consumers were audited together. Unsupported platforms exercise the actual operation and typed refusal before injecting unsupported faults, checking unchanged pair, state, persistent inputs, private children and custody. Supported platform recovery/fault/physical/pointer oracles remain. The SAME dependency lease uses the existing bounded directory eligibility interface and actual physical pager capability; unsupported Bind has no ref/charge/phase consumption, and ordinary sticky cleanup is preserved. Failed physical-close fixtures release retained startup LOCK only as explicit modeled process shutdown after all fatal/no-refund/no-retry/LOCK-held assertions.

The first power-loss stable-image observer now filters the actual DB, serializes Observe and Clone, and deactivates/drains callbacks before restore, matching the sibling cut observer's ownership. Completed stable/volatile-cut expectations remain.

The admission fixture that caused Ubuntu3, macOS3 and Windows5 cancellation synchronously refreshed the same DB from inside its fenced admission callback. It now starts a real competing Refresh, proves it cannot cross the capture fence, refuses original admission without Set/reader effects, joins that same Refresh after callback return, verifies original Set final scrub, then captures the new exact view. Production locks and timeout bounds remain. `admission-callback-audit.md` covers analogous callbacks, and API/lifecycle docs state this callback boundary.

Windows C3 cause remains UNRESOLVED. Failure-only stage and full-goroutine stacks were added at the unchanged 10s join; Linux point/version x inline/pointer cells pass. Windows sharing-violation retry backoff is only a coordinator hypothesis, not attributed evidence or grounds for changing a bound.

## Exact-final local validation

Pinned Go1.26.3, GOWORK=off, GOTOOLCHAIN=local, GOMAXPROCS=2, -p=1, task caches/temp; each batch capped at 180s externally. Full argv/environment, stdout/stderr, exit, elapsed and source before/after manifests are retained under `logs/<batch>/`.

- `final-normal-03`: PASS, 50 top-level executions and 103 direct subtests, including Windows siblings on supported Linux, admission/refusal cleanup and original nonlast allocation/sticky controls.
- `final-race-db-02`: PASS count=2, 26 top-level executions and 36 direct subtests; real consumer/Refresh, PRIMARY topology/capture, empty native Set, same-lease eligibility and admission/cleanup controls. No race reports.
- `final-race-public-02`: PASS, 8 top-level executions and 49 direct subtests; actual 40% public vacuum shrink/reopen and close opt-in, negative lookup, empty checkpoint leaf reclamation, completed stable/volatile image, 36 interrupted cuts, three chunked intermediate-root variants and all four C3 cells. No race reports.
- `windows-all-final-01`: final Windows amd64 CGO-disabled compile of TreeDB/db, TreeDB and TreeDB/mvcc; result is recorded in final test inventory. This is compilation, not Windows runtime proof.
- `format-check-final.json`: changed Go files gofmt clean; git diff --check clean.

Each completed exact-final batch must have before == after == freeze-final. `test-inventory-final.json` records that comparison and distinguishes all preserved earlier RED/inexact runs from exact-final results. Early failures were source compile typos, an incorrect post-Snapshot.Close iterator-read test oracle, and an unactivated audit fixture that appended to the existing generation; actual corrected controls are frozen above.

## Remaining gates and coordinator actions

`failed-check-disposition.json` maps every failure in extracted a8 inventory07, all three confirmed admission cancellations, and unresolved M0 variability. Root reports final a8 check totals 43SUCCESS / 10FAIL / 3CANCELLED including required aggregate and CodeRabbit. These are predecessor-head results.

No known unimplemented source repair remains from R4/R5 or the attributed a8 failures. Actual successor Windows C3 runtime attribution, hosted platform/race checks, independent exact-source review, M0 p99 variability bound and new matched PRIMARY guard remain required. Original a8 raw-path readcost gate passed; it is not successor qualification. No local timing or performance acceptance is claimed.

Root owns the prepared three-path PRIMARY harness integration, CI impact refresh, coherent local commit, independent source/harness review, publication and exact-head hosted gates. This handoff does not authorize merge, accept ordinary/native/graph completion, or relax any allocation, lifetime, shrink, matrix or performance gate.
