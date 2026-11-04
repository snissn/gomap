# Fixed non-gate steady-work process usage capture

This isolated, nonmerging diagnostic branch replaces the existing registered manual
maintenance workflow only on this ref. Main and the acceptance perf-smoke workflow
remain unchanged. ROOT must review the exact source, publish a transport-only
`[skip ci]` commit, and select one dispatch with `expected_head` equal to the full
reviewed harness commit. The job checks that SHA against event and workflow SHAs,
ROOT actor1981537, fixed ref and attempt1 before checkout/helper use. It requires
its sole parent to be the qualified4dcf source, with product and acceptance inputs
unchanged, and verifies completed focused job126934524414 plus exact artifact
13372144927 metadata/digest before capture. Metadata access is not a re-execution
of those controls. Full current-head CI and merge eligibility remain separate.

The capture copies the original `treedb-tests.yml` six balanced off/auto pairs,
all workload flags, seed1, lifecycle, checker and model-conditioned thresholds.
Original go-run package invocation is retained, with `go -C` supplying the original
source cwd; absolute output/profile paths direct evidence to the fresh task root.
Only one absolute `GOMAP_DIAGNOSTIC_STEADY_RUSAGE` receipt is added per benchmark.
The receipt is WORK_INTERVAL only. The first read follows profiler startup; the
second follows work timer capture and successful original final checkpoint,
before profile stop. Read windows/observer cost and sampling overlap are retained;
no subtraction, pprof/process equivalence, checked-Close or server claim follows.
Zero exit of each original benchmark remains a separate requirement. There are no
new Stats/GC calls, warmups, retries, affinity or GOMAXPROCS/GOMEMLIMIT overrides.
Inherited context and actual affinity are recorded. A fresh task temp directory,
pinned Go1.26.10 and the outer sampled guard are new diagnostic context/overhead.

All original row stdout/stderr remain combined in their original row logs; actual
row exits/order and checker exit are retained. The original checker still consumes
all six pairs. A FAIL remains FAIL, with postprocessing allowed afterward. All12
standard `pprof -top -unit=seconds -nodecount=0 -symbolize=none` outputs and literal
exits are retained and emitted to the guard console; their Total samples values
are the original sampled denominators. The complete twelve raw usage receipts are
also retained and emitted. No custom profile decoder or per-operation estimator
is added. Source-level receipt validation is not a causal attribution or gate.

One exact portable guard plus its three transitive imports are vendored without
changes and authenticated before helper import. Linux pidfd/subreaper ownership,
joined status/cleanup and refusal semantics remain those of the maintained guard.
Selected deadline660s includes context, normal Go compilation, twelve rows,
checker, standard tool materialization and twelve text views. Cleanup has up to three10s passes (initial drain, TERM, KILL),
interval0.2s, sampled owned-descendant RSS24GiB and filesystem free floor10GiB
remain unchanged. The12-minute capture step reserves660s active + up to30s cleanup +30s startup/receipt margin. The20-minute hosted job leaves room
for bounded setup/ledger/upload; canceled/incomplete cleanup or output is never
accepted. No185 lease or local logical-HOLD authority transfers to hosted runner.

There is no claimed whole-root/log byte cap or live hard RSS bound. Output/tmp
are task-local; setup-go input downloads/toolcache, existing standard Go caches
and checkout are explicitly outside child guard setup scope. The final ledger
hashes all retained task files and separately measures task root, checkout,
GOROOT/GOCACHE/GOMODCACHE logical bytes/counts, without summing overlapping roots
or claiming a peak. Shared standard caches are preserved; no scratch1GiB policy
or hidden setup exemption is imported. Free floor is sampled while the guard is
active. Final ledger/upload failure, timeout or inability to authenticate any
prerequisite is a failure/inconclusive result, never grounds for a favorable retry.

Original CPU efficiency0.9438303267 FAIL and decode logical HOLD remain. Even a
successful new capture is non-gate diagnostic evidence and cannot qualify an
ordinary acceptance rerun, repair, merge or historical cause. All runtime is
UNRUN at source seal. ROOT alone publishes/admit/dispatches after independent
exact-source review and current authority/resource checks.
