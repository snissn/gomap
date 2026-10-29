# Vacuum M0 CI qualification scopes

This prospective split keeps the ten interleaved production/public fixed-work
samples, benchmark implementations, fixture tests, classifier, numeric limits,
and strict v2/v3 predicates unchanged. No historical packet is reclassified.

Ordinary hosted CI uses `hosted-semantic`. Its successful exit certifies the
existing completion, fixed-work, exposure, overlap and explicit public-status
predicates. The fixture setup still checks deterministic logical digest,
reclaimable debt, offline shrink, unchanged persistent logs and reopen integrity.
All timing observations and strict gate values, including failed CV, remain in
`results.json`. `product_performance_approval` is null. A green hosted job grants
no performance approval and cannot satisfy a controlled qualification obligation.

The default CLI profile remains `controlled-performance`: every existing strict
predicate must pass. The workflow selects it for an actual self-hosted runner.
Runner routing alone does not establish controlled conditions: the CPU admission,
external one-job lease and custody protocol in PR #5281 must be integrated and
prospectively qualified before controlled-environment claims. Existing pause
owner review requirements remain. Strict CV is a stability check, not an A/B
regression detector: a uniformly slower capture can still have low CV. Exact-head
product comparisons and their workload-specific material regression thresholds
remain separately required.

The CI wrapper permits 720 seconds of hosted capture (plus at most 30 seconds to
kill the process group) within the 20-minute job. Controlled capture gets 900
seconds plus the same kill grace. A nonzero exit, timeout, failed test, incomplete
capture or failed applicable gate emits `completed:false` and fails the job.
Partial artifacts upload with a unique run/attempt name; existing packets are
refused. Checkout, toolchain setup and upload occupy the remaining job headroom.
The 75%-of-job *whole-job* target is unproven until hosted runs measure those
phases; the wrapper is a prospective harness bound, not proof of runtime success.

## Alternatives assessed

Paired/normalized hosted timing does not yet have a validated noise model: the
public and production fixtures do different work (160 versus 4096 foreground
operations), so their wall times cannot simply be divided into an acceptance
statistic. Reducing work or repetitions would require new semantic coverage and
statistical-power evidence. Retaining all ten samples is the smaller change.
Raising CV or timeout limits and selective retries do not resolve the contract.
Separating scope retains full semantic coverage while reserving timing acceptance
for controlled qualification.

## Preserved evidence and remaining validation

The #5319 packet `stable-m0-12863932987.zip`, source
`f1b8599f7eddafb66da8d47a5f3a6f13d5715167`, SHA256
`76969ce77f58872d302f947c1d75a8cb4a41ccb68dcdc523c156aa0bf5b9fa48`,
retains a strict v3 failure: foreground-p99 CV 0.1338489707 versus 0.10.
Total CV is 0.0870962616; pause diagnostic CV is 0.3699265708. Every existing
semantic predicate passes; public status is available with ten successful vacuums
and 160 overlapping operations per sample. Replaying its metrics with the
unchanged strict evaluator reproduces its original gate map. This observation
does not prove that host conditions caused the timing failure.

Live ruleset 19140463 requires `TreeDB required gate` with strict current-branch
checks. Its aggregate workflow depends on raw-path, vet, test, race, perf and
snapshot-iterator-perf jobs, not M0. Ruleset 12961194 enforces non-fast-forward
protection. This records observed configuration, changes no policy, and grants
no exception to owner performance or review obligations.

Local parser/contract tests exercise noisy timing with retained strict failure,
every semantic work counter, missing timing observations, public unexpected
errors/exposure misses, all-retry classification, profile constraints, timeout
failure receipts and preservation of existing packets. No timed benchmarks or
controlled qualification were run for this change. #4519 remains open: five
unchanged-head hosted invocations across allocations, whole-job budget evidence,
controlled noise and injected material-regression controls, complete packets,
exact-head review and current-head CI are still required. Integrate #5281's
custody and upload safeguards without replacing its external lease checks.
