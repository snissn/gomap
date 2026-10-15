The ordinary MVCC maintenance cost comparison needs a reviewed harness on main before collecting new retained evidence. This PR adds a manual diagnostic pinned to baseline `5bca87ffe6e1bfba2d693264d2dc82936adc234f` and the independently reviewed allocation-packing successor `a17febafb5353897c59a4e17f992e34c47a2585b`, with complete source/toolchain/binary/raw-result retention and bounded process cleanup.

## Scope and behavior

- New manual-only workflow and five Python/documentation files. Fixed products are separate clean checkouts; no arbitrary-ref input, PR/push trigger, product overlay, self-hosted runner, or change to existing acceptance gates.
- Preserves the original fourteen-call schedule: three AB/BA pairs each of durable pointer Checkpoint16x and production foreground-churn Vacuum1x, then candidate cohort8/16 at128x. Selectors, parser, timing boundaries, CPU0,1/GOMAXPROCS2, strict2% quietness threshold,10s preflight and120s/150s benchmark caps remain unchanged. Busy preflight executes zero calls; contaminated measurements fail without retries or selective discard.
- One shared Linux subreaper supervises bootstrap, builds and collection. It records actual leader/adopted-child joins and group emptiness; timeout, interruption or surviving descendants fail closed. A two-second allowance is cleanup only.
- Retains complete SDK/source archives and manifests, compiler/environment/build logs, four emitted binaries/buildinfo, all attempted raw calls and failure/refusal receipts. Ephemeral hosted infrastructure, fresh caches and14-day artifact retention are disclosed. Hard runner loss may prevent cleanup/upload.
- Adds the manual member to the CI impact inventory and conservative input coverage, updating cardinalities to17workflows/59members. Existing execution jobs and required gates remain unchanged.

## Validation and review

Exact harness candidate: `a1db58c790e056a02340c98e7dbae11f40c8ed48`; base `eae34350200668e1aae35d56f7f4c8ddfc2acb47`. All9,948 source files match the reviewed manifest; nine changed paths only.

Local PASS: `refresh_ci_impact_inventory.py --check`, `test_refresh_ci_impact_inventory.py` (15), `test_ci_impact.py` (21), `test_treedb_ci_contract.py` (6), `test_treedb_windows_core_headroom.py` (16), `test_mvcc_m0_observation.py` (10), and `mvcc_maintenance_test.py` (10), under `.github/scripts/` with bytecode disabled. Total78tests plus inventory. Pure controls cover parser/metric rejection, pinned source/schedule/workflow contracts, actual toy-child reaping and refusal on unexpected descendants.

Independent exact-routed `gpt-6.1-sol` `INTERNAL_SUBAGENT_REVIEW`: ACCEPT, no findings; exact commit binding is being finalized. Hosted manual review cannot pin the user-required model, so the task-local mergeable skill's internal fallback applies. Required current-head CI and final thread inventory remain pending. No Go workload or diagnostic dispatch has run for this harness.

## Evidence limits and next gate

Tracked under #4871 and ordinary PR #5127. This harness must land before a separately authorized, frozen dispatch. Three pairs and one cohort sample per size are bounded diagnostics; cohort cleanup is a single event. The vacuum cost fixture supplies no full digest/physical shrink proof. `COMPLETE_BOUNDED_DIAGNOSTIC` is not cost, M0, native, sustained-workload or current-CI qualification. The prior ordinary cost regression and M0 failure remain unresolved. No performance improvement is claimed by this harness-only PR.
