# #5335 secondary support and evidence ledger

Local evidence index, 2026-10-30. This is not an external report, a final qualification, a new execution plan, or authorization to rerun anything. It records the **old-GoMap vs provisional repaired-GoMap** 24 version/path/format/profile entries required by issue #5335. Actual MongoDB is the primary comparator and is addressed in the companion `EVIDENCE-CROSSWALK.md`, not substituted for any row here.

## Identity and reading rules

Baseline B is `eb9a4ab4a8e7c2c4ff34b53552b2cdde9a9c7144` with disclosed lifecycle patch SHA256 `97fb01345d6ab1b133c7bce82bd2c8a498125fc184427005088a11335583efd3`, tree `0f111793febce209c05f472bcf770151ac9a65db`. Candidate C is provisional integration `802f7cfcf8b69eff1447a5f4833076d428e61e3f`, tree `d6d8abe929c5ef001bb626a8226b6ac940184b60`; exact actual per-cell binary and added harness/scan-limit bindings are in the retained packets, not implied by these commit names. Their 327-commit difference is whole-version, not isolated COW. Actual source/binary identities for the small correctness controls are indexed in `/home/mikers/gomap-5335-public-lifecycle-readiness-20261030/ACTUAL-HANDOFF.md`; measurement manifests remain authoritative for each timed cell.

`measured-valid (provisional)` means the cited existing whole-version packet accepted a timed cell under its own controls, **not** final #5335 acceptance. Three means three independently loaded fixtures. `supported-unmeasured` means support is evidenced by the specified bounded product control, while the indicated timed sample was not run; it does not imply the service is fully qualified. `failed-invalid` preserves the actual failed timed attempt. `unsupported/fail-closed (raw wire)` means a product-side raw-wire refusal and no-effect evidence exist, but the separately specified official-driver refusal remains unproved. Statuses may coexist: a failed attempt is not erased by a separate valid sample or a support control. The numerical profile is not a claim of equal durability across APIs; native uses visible ACK and its checked Close flush, gateway's retained runs use the recorded Mongo concern. JSON's Get/merge/ReplaceBatch can lose overlapping same-key updates; observed-state persistence does not establish linearizability or all random ACKs.

## 24 entries

| Version | Public path / storage | Profile | Retained provisional status |
|---|---|---|---|
| B | native / BSON | durable | measured-valid: 3 fresh fixtures |
| C | native / BSON | durable | measured-valid: 3 fresh fixtures |
| B | native / BSON | relaxed | measured-valid: 3 fresh fixtures |
| C | native / BSON | relaxed | measured-valid: 3 fresh fixtures |
| B | native / BSON | no WAL | measured-valid: 3 fresh fixtures; earlier observer-failed attempt remains invalid |
| C | native / BSON | no WAL | measured-valid: 3 fresh fixtures |
| B | native / JSON | durable | failed-invalid: original 600 s load timeout, 64,728 inserts and 16 termination-era errors; further timing deferred; small support control passed |
| C | native / JSON | durable | measured-valid: 1 unpaired fixture; remaining timings deferred |
| B | native / JSON | relaxed | measured-valid: 3 fresh fixtures |
| C | native / JSON | relaxed | measured-valid: 3 fresh fixtures |
| B | native / JSON | no WAL | measured-valid: 3 fresh fixtures |
| C | native / JSON | no WAL | measured-valid: 3 fresh fixtures |
| B | Mongo API / BSON | durable | measured-valid: 3 fresh fixtures |
| C | Mongo API / BSON | durable | measured-valid: 3 fresh fixtures |
| B | Mongo API / BSON | relaxed | measured-valid: 1 unpaired fixture; remaining old-version timing deferred |
| C | Mongo API / BSON | relaxed | supported-unmeasured in secondary old-version packet; no paired candidate dispatched there; separate current MongoDB comparison exists |
| B | Mongo API / BSON | no WAL | supported-unmeasured; secondary timing deferred |
| C | Mongo API / BSON | no WAL | supported-unmeasured; secondary timing deferred |
| B | Mongo API / JSON | durable | unsupported/fail-closed (raw wire); official-driver control unproved |
| C | Mongo API / JSON | durable | unsupported/fail-closed (raw wire); official-driver control unproved |
| B | Mongo API / JSON | relaxed | unsupported/fail-closed (raw wire); official-driver control unproved |
| C | Mongo API / JSON | relaxed | unsupported/fail-closed (raw wire); official-driver control unproved |
| B | Mongo API / JSON | no WAL | unsupported/fail-closed (raw wire); official-driver control unproved |
| C | Mongo API / JSON | no WAL | unsupported/fail-closed (raw wire); official-driver control unproved |

Six complete paired rows (native BSON all profiles, native JSON relaxed/no WAL, Mongo BSON durable), unpaired cells and exact rates are indexed in `/home/mikers/gomap-5335-perf-campaign-json-timeout-successor-20261030/CHECKPOINT.md`, especially its **priority redirect and terminal boundary**, not its superseded earlier 'active' lines. That file points to retained raw cells; the 1200 s JSON continuation was never launched after redirection. The invalid native BSON no-WAL observer attempt is retained in `/home/mikers/gomap-5335-perf-campaign-fullcensus-20261030/CHECKPOINT.md`; the initial executable-role and scan failures remain separately retained under `/home/mikers/gomap-5335-perf-campaign-20261030/TERMINAL.md` and `/home/mikers/gomap-5335-perf-campaign-successor-20261030/TERMINAL.md`. The deliberately stopped dispatcher and cleanup receipt are also retained; do not infer additional attempted cells. There is no requirement to complete deferred old-version timings before the actual-MongoDB primary comparison.

## Support/lifecycle and official-driver gap

`/home/mikers/gomap-5335-public-lifecycle-readiness-20261030/ACTUAL-HANDOFF.md` documents the 18 product cells, two versions × three supported routes × three profiles, using deterministic exact typed values, public operations, checked checkpoint/original Close and fresh-owner reopen. `/home/mikers/gomap-5335-real-worker-state-20261030/HANDOFF.md` documents separate 18 actual-worker producer/Close and random observed-state preservation controls. `/home/mikers/gomap-5335-public-crash-v2-20261030/HANDOFF.md` documents six ordinary-ACK durable process-SIGKILL recoveries; its own report explicitly does not establish WAL replay or power-loss certification. These product controls are source-bound to the above provisional v4 builds, run with CPU0 affinity and effective runtime GOMAXPROCS 1 with the environment override absent; they do not prove final-head performance. Gateway's task-opt-in owner FlushAll→backend Checkpoint is private instrumentation, not a public Mongo API checkpoint.

The current live issue's matrix explicitly requires **a tiny official-driver support control** for Mongo API/JSON storage using the retained byte-valued workload. The six independently audited `/home/mikers/gomap-5335-json-refusal-20261030/HANDOFF.md` cases did execute against actual gateway products: the dedicated `/home/mikers/gomap-5335-json-refusal-20261030/cell.py` sends BSON subtype 0 binary with w:1 using `/home/mikers/gomap-5335-json-refusal-20261030/mongo_wire.py` raw OP_MSG. Each records `unsupported BSON type binary`, verifies full sentinel rows and collection inventory before/after and on reopened owner, and checks actual original Close. `final-audit.json` and each `actual/treedb_mongo/public-refusal.json` retain per-version/profile/source/results. This is strong actual gateway wire/no-effect evidence, but its handoff explicitly says it is **not official-driver interoperability proof**. The separate maintained deterministic driver's `scripts/ycsb_correctness_fixture.go` locally refuses `target=mongo,format=json` before a request, and `/home/mikers/gomap-5335-public-lifecycle-20261030/lifecycle.py` likewise preflights it; neither supplies that missing client-to-product observation. Official-driver Mongo/BSON success in the lifecycle and worker packets does not test the JSON rejection route. Targeted inspection found no other real official-driver JSON-binary refusal receipt; absence in this index is not proof no such receipt exists elsewhere.

Thus the narrowly remaining issue-specific proof is an actual official Mongo driver issuing the byte-valued insert to JSON-configured gateway and observing its returned refusal, with unchanged data/collection state and applicable checked Close/reopen; bind exact selected source/ELF and request. Reuse the existing six raw-wire controls as product evidence, do not convert fixture-local preflight to a product result, and do not run a 100k failing load. Whether those controls plus a sufficiently scoped official-driver case establish all six secondary entries is an acceptance decision; don't invent six new required runs. The issue states secondary controls are required before their own selected cells, **not** as a prerequisite to primary Mongo/BSON comparison. No runtime operation is authorized or launched by this ledger.

## Final-source and issue reconciliation

`/home/mikers/gomap-5335-maind7-overlay-source-20261030/RESULT.md` binds local combined overlay `d64d0c059b71a507c8890a27cff6be04931be7fc`, tree `47eed0d9e10a0db079f00d9a8f8440b0cf07bfba`, and its exact 57-path diff to reviewed overlay `844930b8b15c963ae7c718d9adcb79108876b88f`. Prior 222 offline tests executed on 844, six Go owner-evidence tests on 326d; neither is a d64 runtime result. No provisional product measurement above is relabeled as d64. Final exact-source/binary/owner/path/concern and lifecycle applicability, predecessor merge, strict cost, current-head CI/review and report/acceptance gates remain. Keep the original failed strict j:true GoMap load (6,735 inserts, 16 errors, no timed GoMap result) separate from common-load/sentinel A/B.

Suggested issue-body correction only, **not applied**: the top checklist still says the maintained MongoDB reference hardcodes w:1 and rejects non-null journal requests. At d64, `scripts/ycsb_mongodb_reference.py:28-30` has separate j:true and j-absent ACK labels; `scripts/ycsb_mongodb_reference.md` documents the separate reference dispatch. Explicit j:false remains unsupported, but j-absent satisfies the issue's permitted relaxed alternative. The generic shell deliberately refuses `TARGETS=mongodb`, consistent with the later issue instruction to use the maintained reference dispatch. Update that stale wording and checked/remaining evidence only when the owner elects to revise the live issue; no issue change or external report publication is made here.
