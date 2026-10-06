# R1 indexed row-store qualification — pending

This is the report scaffold for [#5061](https://github.com/snissn/gomap/issues/5061)
and [parent #5056](https://github.com/snissn/gomap/issues/5056). **No retained
measurement, performance acceptance, or overall R1 qualification is recorded
here.** The coordinator fills this report from validated source-bound packets
after reviewed tooling has landed. Pending means missing evidence, never zero.
The B/C/D product and tooling integration is packaged in
[PR #5065](https://github.com/snissn/gomap/pull/5065); their acceptance obligations
remain separate. Packaging does not complete the pending E evidence gate.

The authoritative behavior and measurement scopes are the
[selected comparator contract](../../spec/r1-indexed-row-contract.md),
[complete-row reads](../../spec/r1-row-reads.md),
[indexed mutations](../../spec/r1-indexed-mutations.md), and
[lifecycle contract](../../spec/r1-row-lifecycle.md). The
[user guide](../../guides/typed-row-store.md) and
[runnable example](../../../../examples/typed_rows/README.md) cover usage.

## Conclusion and acceptance

| Decision | State / evidence |
| --- | --- |
| Useful supported native row-store behavior | Pending integrated correctness/review/CI record |
| Matching existing-behavior baseline/candidate guardrails | Pending retained comparison and noise interpretation |
| Complete ordinary typed point and newly complete public range costs | Pending candidate public-path evidence |
| Finite mixed lifecycle, old readers, recovery and lawful reclamation | Pending accepted #5060 packet and correctness record |
| Material regressions and unexplained growth reconciled | Pending findings, owner, and disposition |
| Final #5061 / parent acceptance | Pending coordinator decision and durable tracker link |

Fill a brief conclusion only after those gates are evaluated. Unsupported,
inconclusive, failed, and unmeasured cells remain visible. A valid packet or
passing example alone is not acceptance; remaining obligations keep R1 open.

## Provenance and publication

Use separate immutable capture directories for comparator baseline, comparator
candidate, and lifecycle candidate. Populate `publication-manifest.template.json`
into `publication-manifest.json` only when actual capture locations, source and
tooling commits, review/landing links, runtime/harness/fixture/binary hashes,
toolchains, commands, and file checksums are known. This publication inventory
does not replace either harness's packet schema or validation.

| Capture | Measured source and identities | Review / landed tooling | Raw packet / validator / summary |
| --- | --- | --- | --- |
| Comparator baseline | Pending | Pending | Pending |
| Comparator candidate | Pending | Pending | Pending |
| Lifecycle candidate | Pending | Pending | Pending |

Copy the capture tree without editing its packet or raw log bytes. Preserve
relative paths used by its validator. For large host-retained files, record the
absolute host path, immutable archive location if available, checksum, byte
length, owner, and retention policy. A host-local path is not a downloadable
artifact. Publish failures/rejections separately; do not overwrite or average
them away. `SHA256SUMS` covers every published packet, raw log, manifest, and
summary; retained binaries/archives have explicit checksums even when not in Git.
For each `file_inventory` entry record `path`, `sha256`, `bytes`, and whether it
is published or host-retained. Fill capture ownership/retention and referenced
command/environment/validation records with real locations. Keep empty arrays
and nulls explicitly pending until completeness is checked; an empty findings
list at scaffold time is not a clean review.

Proposed layout (directories are created only when evidence exists):

```text
r1-row-store-5061/
  README.md
  publication-manifest.json
  SHA256SUMS
  comparator/
    baseline/<immutable-capture-id>/   # original packet/source/raw/summary tree
    candidate/<immutable-capture-id>/  # original packet/source/raw/summary tree
  lifecycle/
    candidate/<immutable-capture-id>/  # original packet/runs/raw/summary tree
  validation/                         # commands, exit status, raw validator logs
  correctness/                        # exact-head test/review/CI records
  rejected/<immutable-attempt-id>/    # failed, partial, noisy or rehearsal data
```

Record actual runner/load, cache state, durability, toolchain/SQLite/CGO versions,
filesystem/device, temporary DB placement, environment, affinity, and competing
activity. Document unavailable infrastructure and real fallbacks. Freeze
reviewed landed tooling before retained collection. Product/harness drift
invalidates affected evidence; an artifact-only descendant may reuse it only
when measured runtime/harness identities and blob provenance still match.

## Repetitions, fixtures, and timer boundaries

The comparator's selected default is five independent fresh-DB repetitions
**inside one benchmark process**, with each configured engine run serially in
each repetition. It is not five fresh-process measurements. Process/global
allocator state and warm OS cache can carry between cells. The exact packet
records 4,096 rows, 32-row batches, 1,000 calls per phase, one worker, and the
selected read state; these are prescribed dimensions, not observed results here.

Lifecycle defaults launch **five fresh OS processes**, each with five final
epochs, 4,096 live rows, and 1,024 calls per epoch. Preserve each process's raw
one-epoch Go calibration, validate it, and exclude it from final-epoch summaries.
Its fixture hash and repeated-set accounting are separate from the comparator;
do not assume byte equality or pool their samples. The default timed churn
repeats 512 distinct IDs rather than rotating through all 4,096. Fill actual
distinct/new/revisited/cumulative counts and visited-set hashes from the packet.
This finite hot-set diagnostic cannot establish full-population stress or
unlimited sustained capacity.

Separate ordinary public calls from prepared component timings. Retain read-view
open/flush setup, first fetch, template/BSON conversion, complete owned output,
and materialization/fallback counters. Ordinary range selection and full rows
share one publication; the quiescent ID-range-plus-prepared-fetch decomposition
is a different route and has no concurrent shared-snapshot guarantee. Mutation
costs include caller encoding through ACK. Lifecycle call/epoch timers include
their documented caller/oracle bookkeeping; checkpoint, maintenance, census,
and full phase oracles are outside them.

## Comparator results — pending

Expand the table by packet engine, phase, durability, and read state. Keep call
and row denominators distinct. Link raw latency samples/summaries; do not treat
the median of repetition p99s as one pooled p99.

| Route | Baseline | Candidate | Interpretation |
| --- | --- | --- | --- |
| Ordinary complete point (`point_get_into_complete`) | Pending | Pending | Matched route/semantics needed for a before/after comparison |
| Prepared complete point/batch | Pending | Pending | Report setup/first-fetch separately from reuse |
| Ordinary typed complete range (`range_public_complete`) | Starting residual-only route unsupported; raw rejection pending | Pending newly complete public-call timing | No equivalent complete-output before timing; no before/after speedup ratio |
| Quiescent complete range decomposition (`range_complete`) | Pending | Pending | Distinct route; do not relabel as the ordinary range before-time |
| Indexed/nonindexed update, replace, delete and mixed churn | Pending | Pending | Matching fixture/history/ACK and encoding boundaries |
| Atomic mixed upsert | Retained-document cells unsupported; raw skips pending | Pending supported native/SQLite cells | No split delete/insert substitute |
| Setup, checkpoint, common storage boundary | Pending | Pending | Keep separate from call timings and trailing upsert |

For each supported cell report throughput median/min/max, spread, ns/call,
per-repetition p50/p95/p99 with sample count, B/call, allocations/call, output
rows/call, and path/fallback counters. Comparator throughput spread
`(max-min)/median > 15%` is inconclusive under the selected contract. Preserve
all cells and do not invent a regression threshold before accepted baseline/noise
characterization. SQLite WAL/FULL and TreeDB durable ACK are verified from actual
packets; relaxed settings are a separate comparison and typed relaxed admission
remains unsupported.

Allocation figures are process-wide **Go** runtime deltas, including background
Go activity and excluding SQLite **C** allocator bytes. They cannot establish
total-memory superiority over SQLite. Phase heap is not RSS or retained-memory
attribution. If an external sampler supplies RSS, label it **sampled RSS** and
record process scope, sample interval, raw series, and observed sample maximum;
that maximum is not an exact peak. Otherwise RSS is unavailable. Exact unsampled
peak, C allocations, and allocated filesystem blocks remain unavailable unless
separately measured and source-bound.

## Lifecycle, storage, and recovery — pending

| Observation | Actual result / scope / evidence |
| --- | --- |
| Five-process mixed call throughput and p95/p99 | Pending; retain each process and descriptive spread |
| Operation counts, working-set coverage, live row/index parity | Pending |
| Allocation, live/after-GC retained heap and sampled heap high | Pending; diagnostic oracle/sample objects included |
| Sampled RSS and sampler provenance, if collected | Pending or unavailable; no peak-memory claim |
| Index / persistent value-log / leaf-log / typed-asset / other logical bytes | Pending, per ingest/churn/checkpoint/maintenance/release/reopen phase |
| Redo WAL and SQLite transient WAL-index bytes | Pending, separate from persistent payload |
| Checkpoint / maintenance / post-release GC duration | Pending, separate from timed mixed calls |
| Logical history fold, overlay work, typed rewrite debt, protected/retained/reclaimable bytes | Pending, actual aggregate attribution |
| Same-live direct-backend index vacuum work and before/after census | Pending; cached-wrapper overhead excluded |
| Value-log active/pending/referenced/protected/deleted classes | Pending; overlapping classes cannot be summed as unique storage |
| Old warmed views, release, command-WAL cuts, final reopen | Pending accepted correctness record and source binding |

Interpret storage growth against live population, completed work, active reader
pins, recovery roots, retained history, segment utilization, and remaining debt.
Lawful retention is not automatic evidence of a leak; it also does not explain
every increase. Any unexplained growth or failed maintenance gets an explicit
finding, owner, and next causal check. Reader release, no-op maintenance, successful
rewrite/remap, and actual deleted bytes are separate facts. The lifecycle
benchmark's maintenance observations do not replace the correctness tests' typed
rewrite and lawful reclamation proof. Logical latest-row folding and reset
mutation parts do not establish physically bounded storage. A partially live
value/leaf segment needs eligible rewrite/packing rather than whole-segment GC.
The example/comparator use the public cached-leaf native opener; the lifecycle
fixture is a direct durable command-WAL backend with background prune disabled.
Its same-live backend index vacuum excludes cached-wrapper checkpoint/reconcile
overhead and does not qualify high-level `CompactStorage` cost or route
equivalence. Fill the actual eligibility/refusal, recovery/pin retention,
completed work, and component census from the
[canonical lifecycle scope](../../spec/r1-row-lifecycle.md).
Existing resource-closure owners retain authority;
do not add an unconditional destructive shortcut to make a graph close.

## Coordinator filling checklist

- [ ] Record accepted A/B/C/D source snapshots and integrated merge, current-head required CI/reviews,
  exact correctness commands/logs, and example/guide acceptance.
- [ ] Verify reviewed tooling landed, freeze both measured sources/harnesses, and
  validate every retained packet against independently frozen identities.
- [ ] Publish untouched captures/checksums and failures; fill observed dimensions,
  denominators, memory scope, cell exclusions, noise, and same-semantics results.
- [ ] Record new complete-range cost without an equivalent before-time claim;
  consume the accepted lifecycle packet without duplicating its acceptance.
- [ ] Resolve or carry named regressions/growth/findings and remaining obligations.
- [ ] Replace pending conclusions only with coordinator-supported decisions;
  link the final #5061 and parent acceptance or state precisely what remains open.
