# Collections Native Fast-Path Execution Playbook

Status: draft playbook, non-normative.

This document is the pre-execution operating guide for the native cached
collections rewrite.

The proposal defines the target architecture.
The roadmap defines the rewrite phases.
This playbook defines how to start the rewrite without losing rigor, benchmark
signal, or rollback options.

See also:

- `TreeDB/docs/spec/collections-native-fastpath-proposal.md`
- `TreeDB/docs/spec/collections-native-fastpath-roadmap.md`
- `TreeDB/docs/spec/collections-native-fastpath-baseline-template.md`
- `TreeDB/docs/spec/collections-native-fastpath-pr-note-template.md`

## 1. Purpose

The rewrite should start only after the following are true:

- the baseline is frozen,
- the benchmark discipline is fixed,
- the branch and PR structure is clear,
- the success and stop conditions are explicit.

This avoids repeating the previous pattern of doing architecture work and only
later discovering that the measurement model or execution model was wrong.

## 2. Pre-Execution Readiness Checklist

Before opening `R0`, complete and record all of the following.

### 2.1 Freeze the semantic reference

- identify the exact oracle branch that remains the semantic reference,
- do not land further architectural work on that branch,
- allow only correctness fixes required to keep the oracle usable.

Before `R0`, the semantic reference should be reset to the latest acceptable
`main` base if a large upstream landing has materially changed TreeDB caching,
publication, maintenance, or benchmark behavior.

Do not drag a long-lived experimental branch forward blindly when the divergence
is large. Instead:

- create a fresh main-based prep branch,
- carry forward only the planning and benchmark-preparation work,
- recapture all baseline artifacts on the new base before implementation
  continues.

### 2.2 Freeze the benchmark harness inputs

For rewrite comparisons, freeze these defaults unless a PR explains why they
change:

- host machine,
- `GOMAXPROCS`,
- `-keys`,
- `-valsize`,
- `-batchsize`,
- durability profile,
- whether the run is mixed-under-debt or settled-after-checkpoint.

Recommended baseline inputs for raw TreeDB anchors:

- `-keys 500000`
- `-valsize 100`
- `-batchsize 8000`
- `-read-require-hit`
- `-checkpoint-between-tests`

### 2.3 Freeze the benchmark suites

The rewrite uses two benchmark families:

- **raw TreeDB anchors** from `unified-bench`
- **collection-focused benches** from the oracle worktree first, then from the
  native execution branch once `R0` lands the harness there

The minimum raw TreeDB anchor bundle is:

- `write_seq`
- `write_rand`
- `batch_write`
- `batch_random`
- `batch_delete`
- `delete_rand`
- `random_read`
- `random_read_parallel_acquire_snapshot`
- `full_scan`
- `prefix_scan`
- `-suite flushdrain`

The minimum collection-focused bundle is:

- `BenchmarkCollectionInsertBatchProvidedID`
- `BenchmarkCollectionInsertBatchWithSecondaryIndexes`
- `BenchmarkCollectionInsertBatchCheckpointWithSecondaryIndexes`

Important:

- current `main` may not yet contain the collection benchmark harness,
- before `R0`, the collection baseline may therefore be oracle-only,
- `R0` must introduce the native-path harness on the main-based branch and
  recapture the native side of the baseline before `R1`.

### 2.4 Freeze the profiles used for gating

Every performance-relevant phase should report:

- `fast`
- `wal_on_fast`

`fast` is the throughput ceiling indicator.
`wal_on_fast` is the “still in the same direction under a more realistic
durability shape” indicator.

### 2.5 Freeze the artifact format

Every rewrite PR should attach:

- exact commands used,
- raw stdout or markdown summary,
- profile artifact directory,
- `benchprof_results.md` or equivalent,
- a short delta table against the previous phase.

For collection-focused artifacts, the attachment must also record:

- which worktree or branch the command ran from,
- the exact HEAD SHA for that worktree,
- the emitted bundle directory if the helper script creates a temporary output.

Use `TreeDB/docs/spec/collections-native-fastpath-baseline-template.md`
verbatim for the initial freeze record.

### 2.6 Tooling and harness preflight

Before capturing any baseline, verify the required tooling on the relevant
worktree:

```bash
make unified-bench
test -x ./bin/unified-bench
```

For collection benchmarks, verify whether the harness exists on the worktree
being measured:

```bash
test -x ./scripts/bench_collections_report.sh
```

If the native execution worktree does not yet contain the collection benchmark
harness, record the native collection bundle as `N/A before R0 harness
bring-up` and do not fabricate a native baseline.

## 3. Branch and PR Topology

The rewrite should use one stacked sequence rooted from the frozen semantic
execution base branch.

Recommended stack:

- `pr/native-fastpath-r0-oracle-baseline`
- `pr/native-fastpath-r1-root-runs`
- `pr/native-fastpath-r2-vector-probes`
- `pr/native-fastpath-r3-grouped-publish`
- `pr/native-fastpath-r4-warm-apply`
- `pr/native-fastpath-r5-batch-planner`
- `pr/native-fastpath-r6-single-doc-cutover`
- `pr/native-fastpath-r7-default-flip-cleanup`

Rules:

- each PR targets the immediately previous branch,
- no merges during the stack build,
- the oracle branch is never a merge target for this stack,
- every PR states whether it is:
  - scaffolding,
  - correctness,
  - performance,
  - cleanup.

## 4. Phase Classification

Each phase must declare one of two performance expectations.

### 4.1 Scaffolding phase

A scaffolding phase may legitimately show no improvement.

It still must:

- avoid material regression on raw TreeDB anchors,
- avoid material regression on focused collection benches,
- explain why the phase is prerequisite-only.

### 4.2 Performance phase

A performance phase is expected to produce a measurable benefit in the benchmark
family it targets.

It must:

- name the target benchmark family before implementation,
- state the expected win shape,
- prove at least one material improvement before the phase is considered done.

## 5. Canonical Command Set

### 5.1 Raw TreeDB write/read/scan anchor capture

```bash
OUT=$(mktemp -d /tmp/gomap_profiles_XXXXXX)

./bin/unified-bench \
  -dbs treedb \
  -profile fast \
  -keys 500000 \
  -valsize 100 \
  -batchsize 8000 \
  -test sequential_write,random_write,dataset_write_random,dataset_write_sorted,batch_write,batch_random,batch_small_seq,random_read,random_read_parallel_acquire_snapshot,full_scan,prefix_scan \
  -checkpoint-between-tests \
  -read-require-hit \
  -profile-dir "$OUT" \
  -progress=false
```

Repeat with:

```bash
-profile wal_on_fast
```

Use the dataset-write support tests in this bundle. They keep the read/scan
anchors valid without mixing delete-heavy phases into the same run.

### 5.2 Raw TreeDB delete-anchor capture

Capture delete-focused anchors separately so read-hit guarantees remain valid.

Batch delete:

```bash
OUT=$(mktemp -d /tmp/gomap_profiles_XXXXXX)

./bin/unified-bench \
  -dbs treedb \
  -profile fast \
  -keys 500000 \
  -valsize 100 \
  -batchsize 8000 \
  -test random_write,batch_delete \
  -checkpoint-between-tests \
  -profile-dir "$OUT" \
  -progress=false
```

Random delete:

```bash
OUT=$(mktemp -d /tmp/gomap_profiles_XXXXXX)

./bin/unified-bench \
  -dbs treedb \
  -profile fast \
  -keys 500000 \
  -valsize 100 \
  -batchsize 8000 \
  -test random_write,random_delete \
  -checkpoint-between-tests \
  -profile-dir "$OUT" \
  -progress=false
```

Repeat both delete captures with:

```bash
-profile wal_on_fast
```

### 5.3 Deferred-work anchor capture

```bash
OUT=$(mktemp -d /tmp/gomap_profiles_XXXXXX)

./bin/unified-bench \
  -dbs treedb \
  -profile wal_on_fast \
  -suite flushdrain \
  -keys 500000 \
  -valsize 100 \
  -batchsize 8000 \
  -profile-dir "$OUT" \
  -progress=false
```

### 5.4 Collection-focused capture

```bash
ORACLE_WORKTREE=/path/to/oracle-worktree

TREEDB_COLLECTION_BENCH_ENGINE=cached \
BENCHTIME=1s \
COUNT=1 \
(cd "$ORACLE_WORKTREE" && git rev-parse HEAD && scripts/bench_collections_report.sh) | tee /tmp/oracle_collection_bench_stdout.txt
```

Native-path equivalent:

```bash
NATIVE_WORKTREE=/home/mikers/dev/snissn/gomap

TREEDB_COLLECTION_BENCH_ENGINE=backend_direct_fast \
TREEDB_COLLECTION_BENCH_BATCH_SIZE=8000 \
BENCHTIME=1s \
COUNT=1 \
(cd "$NATIVE_WORKTREE" && git rev-parse HEAD && scripts/bench_collections_report.sh) | tee /tmp/native_collection_bench_stdout.txt
```

If the native worktree does not yet contain `scripts/bench_collections_report.sh`,
record the native collection bundle as `N/A before R0 harness bring-up` and do
not fabricate comparability. For phase-local work, add focused `go test -bench`
commands when needed, but the report bundle remains the summary artifact.

## 6. Per-PR Deliverables

Every rewrite PR should include:

### 6.1 Tests-first commit

- failing tests or benchmarks proving the targeted behavior is not present yet

### 6.2 Implementation commit

- the minimum implementation to make those tests pass

### 6.3 PR notes

Use `TreeDB/docs/spec/collections-native-fastpath-pr-note-template.md`
verbatim unless a phase-specific reason requires additive fields.

### 6.4 Architectural counters

Every performance-relevant PR should report the architectural counters that
prove the phase is not winning through a forbidden fallback path.

At minimum, report when applicable:

- tiny-batch fallback batch or key count,
- per-item key probe fallback count,
- per-item prefix probe fallback count,
- detached-batch replay fallback count,
- warm apply rebuild fallback count,
- warm apply per-key retention-lookup fallback count.

If a counter does not yet exist for the targeted forbidden path, add the
counter or add an explicit note explaining why that path cannot exist in the
phase under review.

## 7. Go / No-Go Checklist Per Phase

Do not advance a phase unless all of the following are true.

### 7.1 Correctness

- the relevant tests are green,
- no semantic parity regression is known,
- crash/reopen/maintenance expectations remain intact where applicable.

### 7.2 Benchmark discipline

- raw TreeDB anchors were rerun if the phase can affect TreeDB fast paths,
- collection-focused benches were rerun if the phase can affect collections,
- before collections is reintroduced on the rewrite branch, seam-level
  microbenches are supplemental only and do not replace the raw end-to-end
  TreeDB anchor bundle,
- before collections is reintroduced on the rewrite branch, every relevant
  perf review must attach the full raw TreeDB `fast`, `wal_on_fast`, and
  `flushdrain` snapshot bundle for that branch tip,
- mixed-under-debt and settled-after-checkpoint behavior were both considered
  when relevant,
- deferred-work behavior was measured if the phase could shift work across the
  checkpoint boundary.

### 7.3 Result quality

- the phase is clearly classified as scaffolding or performance,
- scaffolding phases show no material regression beyond the documented noise
  margin,
- performance phases show at least one material improvement in the intended
  benchmark family, or explicitly document why the phase is prerequisite-only,
- performance phases show that the targeted architectural counter moved toward
  zero or remained at zero on the focused benchmark family,
- phases that claim to remove a forbidden translation path do not advance while
  the relevant fallback counter remains materially non-zero.

Where the phase touches a collection benchmark family with a direct raw TreeDB
analog, the PR notes must also state:

- the raw TreeDB anchor being tracked,
- whether the phase is expected to improve parity or preserve it,
- whether the collection benchmark moved closer to or farther from the raw
  anchor.

## 8. Noise Margin Discipline

Use one small documented noise margin for raw TreeDB anchors.

The exact number may be tuned for the host, but the rule is:

- do not wave away regressions casually,
- do not claim wins on measurement noise,
- require multiple runs or focused reruns when deltas are small.

As a starting point:

- raw TreeDB anchors:
  - treat changes smaller than `3%` as suspect until rerun,
- collection benchmarks:
  - treat changes smaller than `5%` as suspect until rerun,
- treat larger regressions as blockers unless there is a documented tradeoff.

When a result falls inside the noise band, rerun at least `count=3` and compare
medians before calling it a win or loss.

## 9. Stop Conditions

Pause the rewrite and re-evaluate if any of the following happens:

1. two consecutive performance phases fail to improve their target benchmark
   family,
2. raw TreeDB anchors regress repeatedly while collection benches improve,
3. the native path starts depending on oracle-derived or transitional helpers
   for correctness,
4. focused profiles are still dominated by forbidden translation paths after the
   phase that was supposed to remove them,
5. no-index collection write or read benchmarks move materially farther away
   from their raw TreeDB anchors for two consecutive relevant phases.

In that case, update the proposal or roadmap before more implementation work.

## 10. Cutover Readiness

Before flipping the native path to default, require:

- one full acceptance sweep,
- one full raw TreeDB anchor sweep under `fast`,
- one full raw TreeDB anchor sweep under `wal_on_fast`,
- one deferred-work `flushdrain` sweep,
- one full collection benchmark report bundle,
- focused no-index single-read and parallel-read collection captures,
- explicit confirmation that the native path no longer depends on oracle-derived
  or transitional execution helpers.

## 11. Recommended First Move

Before `R0` implementation starts, create one baseline note or issue comment
that records:

- the frozen semantic reference branch,
- the main-based prep branch and exact `origin/main` SHA,
- the frozen benchmark command set,
- the first baseline artifact directories,
- the noise-margin rule,
- the planned PR stack names.

That turns the rewrite from an open-ended refactor into a controlled experiment.
