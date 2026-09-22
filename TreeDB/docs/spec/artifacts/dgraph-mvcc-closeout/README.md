# Dgraph MVCC closeout evidence

This directory contains the compact exact-target evidence for gomap #3673.
The raw-path and adapter target is
`dbea38e0e8ad0c7d1e0bb05ac564bd9b57dd747a`; the paired comparison base is
`f9c9b2a37838909d0e669818cfa2840c0a8d5f85`. The corrected closeout-matrix
target is `103f9c5af85d8d6a5801119fc2247be3b9c87fad`, which stops the prune
benchmark timer before all fixture setup. The final pull-request head is an
evidence descendant of that correction and does not change production MVCC
code. The raw-path and adapter benchmarks were not rerun; the closeout matrix
was rerun exactly once after a fresh passing quiet audit.
Dgraph must still pin the first merged-main descendant containing this PR, not
the worker-branch commit.

## Host, method, and retained evidence

- local host: `mikers-B560-DS3H-AC-Y1`, Linux `6.8.0-124-generic`;
- CPU: 11th Gen Intel Core i5-11400F, 6 cores / 12 threads;
- Go: `go1.26.0 linux/amd64`;
- timing CPU: `0`, `GOMAXPROCS=1`, `GOWORK=off`;
- adapter preflight: exact clean `dbea38e0` target, 92.21% average idle, 0.38%
  iowait, no `simd`, and no competing benchmark/build/test job;
- corrected-matrix preflight: exact clean `103f9c5af` target, 95.64% average
  idle, 0.36% iowait, no `simd`, and no competing benchmark/build/test job;
- the unrelated Ironbird durability campaign completed 15/15 accepted rows
  before the local adapter and matrix measurements started.

Raw logs, environment/process captures, checksums, and profiles are retained
outside git at:

- hosted raw-path artifact:
  `/mnt/fast4tb/gomap-3673-evidence/hosted-raw-dbea38e0`;
- local adapter artifact:
  `/mnt/fast4tb/gomap-3673-evidence/adapter-dbea38e0`;
- superseded local closeout matrix, invalid for prune timing because fixture
  setup entered the single timed sample:
  `/mnt/fast4tb/gomap-3673-evidence/closeout-matrix-dbea38e0`;
- corrected local closeout matrix:
  `/mnt/fast4tb/gomap-3673-evidence/closeout-matrix-103f9c5af`;
- passing corrected-matrix quiet audit:
  `/mnt/fast4tb/gomap-3673-evidence/quiet-window-103f9c5af`.

Large raw logs, test binaries, and binary CPU profiles are deliberately not
committed. The generated Markdown and JSON summaries in this directory are
copied without changing their verdicts or samples.

## Raw-path gate: EQUIVALENT

The hosted exact-target raw gate used eight benchmark-group-paired samples per
revision, exactly four AB and four BA pairs, at `-benchtime=2s`. Its verdict is
**EQUIVALENT**, with the measured threshold observation preserved as **FAIL**:

- point read: +1.12%, PASS;
- batch write: +0.36%, PASS;
- snapshot seek: -0.47%, PASS;
- repeated iterator: -1.76%, PASS;
- durable synced write: +27.94%, measured FAIL.

Every row-producing base/head test binary was byte-identical: `db`, `caching`,
and root `treedb`. The checker therefore accepts the result as EQUIVALENT—no
observed delta can be attributed to candidate code—without relabeling the
durable timing observation as PASS. See [raw-path-summary.md](raw-path-summary.md)
and `raw-path-summary.json`.

## Adapter-overhead gate: FAIL, scoped acceptance

The local exact-target adapter gate used eight benchmark-group-paired samples
per revision at `-benchtime=2s`, plus 1-second candidate profiles. Its gate
verdict is **FAIL** and remains a FAIL in the committed generated summaries.
Four base/head timing rows exceeded the 5% ceiling:

- direct commit +9.77% and MVCC commit +7.03%;
- physical all-version iteration +18.88% and MVCC iteration +13.41%.

Direct and MVCC controls co-moved within each failing operation family. The
candidate adapter/direct ratios all passed the independent 2x ceiling:
0.936x commit, 1.008x get, and 1.121x iteration. Allocations were unchanged in
all six rows, and bytes were flat within the gate tolerance. Point reads also
remained within the revision threshold: +2.63% direct and +4.35% MVCC.

The four revision-ceiling misses are accepted only for this restricted
pre-alpha downstream benchmark closeout, not erased or promoted to a passing
gate, because all of the following scoped evidence agrees:

- the base-to-target production diff is test/conformance harness, benchmark,
  documentation, scripts, and workflow code; it does not change TreeDB's
  production read, commit, or iteration implementation;
- direct and MVCC rows co-moved while all candidate adapter/direct ratios
  passed, with a maximum of 1.121x;
- allocations were unchanged and bytes remained flat;
- the hosted raw-path production binaries were byte-identical and received an
  EQUIVALENT verdict; and
- the durability-matched closeout matrix completed successfully.

This rationale does not establish a general performance guarantee. It accepts
the measured closeout risk for the first restricted Dgraph Alpha benchmark and
keeps the exact samples and profiles available for follow-up. See
[adapter-overhead-summary.md](adapter-overhead-summary.md) and
`adapter-overhead-summary.json`.

## Durability-matched closeout matrix: PASS

The corrected exact-target closeout matrix used five samples and
`-benchtime=750ms` for regular rows; each prune row used one fresh fixture per
sample, with the benchmark timer stopped before the parent temporary directory
and all fixture setup. Only `PruneVersions` is timed. The prior matrix is
superseded because its single prune sample included pre-loop temporary-directory
setup. The corrected summarizer completed successfully with all 24 benchmark
rows, ten measured invocations, 611,264 KiB maximum RSS, 102.36 seconds
aggregate user CPU, and 6.97 seconds aggregate system CPU. Representative
durable-sync medians were:

- commit: 149.5 mutations/s at batch 1 and 5,271 mutations/s at batch 32;
- point read: 937,621 lookups/s at depth 1 and 670,037 lookups/s at depth 64;
- all-version iteration: 3,485,151 versions/s at depth 1 and 4,119,271
  versions/s at depth 32;
- prune: 13,505 pruned versions/s at floor 4 and 25,211/s at floor 12.

Prune delete write amplification was 0.8286 in every row. Durable-sync,
WAL-on relaxed, and WAL-off relaxed results remain separate acknowledgement
classes; relaxed rows are not described as durability-equivalent to synced
rows. See [closeout-matrix-summary.md](closeout-matrix-summary.md) and
`closeout-matrix-summary.json`.

## Reproduction

```sh
BASELINE_HASH=f9c9b2a37838909d0e669818cfa2840c0a8d5f85 \
CANDIDATE_HASH=dbea38e0e8ad0c7d1e0bb05ac564bd9b57dd747a \
RUNS=8 BENCHTIME=2s CPUSET=0 GOWORK=off \
OUT_DIR=/absolute/raw-path ./scripts/mvcc_raw_path_gate.sh

BASELINE_HASH=f9c9b2a37838909d0e669818cfa2840c0a8d5f85 \
CANDIDATE_HASH=dbea38e0e8ad0c7d1e0bb05ac564bd9b57dd747a \
RUNS=8 BENCHTIME=2s PROFILE_BENCHTIME=1s CPUSET=0 GOWORK=off \
OUT_DIR=/absolute/adapter ./scripts/mvcc_adapter_overhead_gate.sh

CANDIDATE_HASH=103f9c5af85d8d6a5801119fc2247be3b9c87fad \
RUNS=5 BENCHTIME=750ms CPUSET=0 GOWORK=off \
OUT_DIR=/absolute/matrix ./scripts/mvcc_closeout_matrix.sh
```
