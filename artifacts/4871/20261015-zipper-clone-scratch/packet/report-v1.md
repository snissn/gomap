# #5133 early public performance evidence

Baseline: ordinary source `177fb0c8db1d19c79d3d975126b4b0b64019568e`.
Candidate: same source plus the focused zipper scratch-owner change; source and
binary SHA256 identities are recorded in `receipt-v1.json`. A metadata retention
trim and new public test are pending, so this packet is not the final source
freeze or merged-main qualification.

## Public caller and representation regression

`TreeDB/db/batch.go:403` creates `idx.zipper.CloneWithAllocator(tracker)` for every
optimistic public write. The prior scratch cache belonged only to that clone.
`TestAllocatorCloneReusesApplyScratch` fails on baseline because a subsequent
allocator clone receives new scratch. The concurrent bounded reuse test also
fails, reusing zero scratches instead of the existing maximum nine. These are
selection regressions: pure behavioral equivalence cannot show whether a scratch
allocation was reused. The actual public caller is exercised by the benchmark,
rate-1 exact-stack profile, and pending public retained-snapshot test.

The selected implementation extracts the existing per-Zipper cache into a
shared clone-family owner. Acquire/release remains exclusive and mutex protected.
The owner retains only reset mergeScratch objects, with existing limits; it
cannot retain a pager, allocator, input batch, or operation-local leaf cache.
Unrelated zipper roots receive separate owners. No global scratch pool is added.

## Early matched public comparison

Go1.26.3, Linux111, taskset CPU0, GOMAXPROCS=1, CGO_ENABLED=1, GOWORK=off,
GOTOOLCHAIN=local. Same precompiled benchmark/harness, `-test.benchtime=1000x`,
three alternating baseline/candidate pairs. Setup seeds 10,000 keys; timed groups
perform eight public writes, then stopped-timer Checkpoint drains publication.
The benchmark fails if the asynchronous root publisher runs during a timed group.
All six invocations passed that activation/boundary guard. Exact argv and raw
rows are in `pairs-commands.json` and `pair-*.raw`.

| Quantity | Baseline | Candidate |
|---|---:|---:|
| B/op, median |154839|143710|
| allocs/op |286|274|
| ns/op, median |47556|47559|

Public bytes decrease 7.19%, objects decrease by 12, and median latency differs
by +0.006%. The first baseline and candidate invocations were slower than the
remaining pairs, so these are directional measurements, not a speedup claim or
final eight-repeat qualification.

## Exact public allocation attribution

The separate rate-1 invocations use the same fixed 3000x harness. Filter the exact
`github.com/snissn/gomap/TreeDB/db.(*Batch).Write` frame, excluding seed WriteSync
and stopped-timer Checkpoint stacks. Raw whole-profile totals are not the public
denominator. Use the cumulative exact Write row in the saved pprof output.

| Exact public Write allocation | Baseline bytes | Candidate bytes |
|---|---:|---:|
| Total |464099984|430702520|
| newMergeScratch |33323104|0|

The public profile removes all recurring `newMergeScratch` bytes (100%), and
total public allocated bytes decrease 7.196%. Seed work still constructs the
family scratch; this is ordinary warm reuse already available on a long-lived
Zipper, now preserved through the actual caller clone lifetime. The fixture and
setup/ACK/maintenance boundaries are unchanged. Observer timing is not compared.

## Ownership, retention, and limits

Each concurrent Apply borrows a distinct scratch. The cache retains at most the
existing `applyScratchKeep + 1` (nine) reset scratches across all clones; it
allocates fallback scratch for extra concurrent borrowers and drops excess on
release. The cache lives only while any member of its clone family is reachable.
Returned root IDs, retired page lists, owning Get results, and persisted page
bytes remain independent of scratch reuse. Split keys remain internal borrows
only through Apply; reset clears their references.

Existing per-scratch caps remain: split-key arena 1MiB, split arena 32768 entries,
outer build pages128, leaf scratch128 pages, node-key scratch128 buffers at most
1MiB each, leaf-ref cache4096 pages, pending/batch/child-ref scratch64 buffers each
at most512 entries, span range/root arrays4096 entries. These are independent
bounds, not a low global memory budget. Scratch caches already retain these
capacities for a persistent zipper; sharing prevents a multiplier per clone.
Leaf-ref pages are interior pointers into existing 64-page allocation chunks;
retained heap can exceed the logical page count through chunk reachability. This
change neither adds chunk pooling nor claims a 16MiB whole-heap ceiling per
scratch. Active operations may exceed retained limits transiently. The in-flight leaf-ref
cache continues to own stable bytes until endLeafRefCache; no ownership copies
were removed. Prior #3524/#3537 and merged #3532 were inspected explicitly.

The end-of-process inuse profile is after DB close and cannot quantify live DB
retention. It shows no scratch retained after close, but is not a live-memory
acceptance claim. The clone cap tests and pending oversized-pointer metadata
regression establish the retention policy structurally.

## Validation collected

- Baseline representation RED: expected failure in clone scratch reuse and
  bounded reuse; repeated Apply/error retry/retained-root behavior passed.
- Focused GREEN: clone tests, existing Apply scratch, leaf-ref cache, and page
  cache ownership tests passed.
- Focused race: clone tests and existing Apply scratch tests passed.
- Full `./TreeDB/zipper` passed.
- Forced-pointer checkpoint/write-sync reopen and value-log GC removal tests
  passed across `./TreeDB` and `./TreeDB/db`.
- Candidate short CPU profile is saved. It is diagnostic attribution, not a
  matched throughput result. Exact public Write accumulated4110ms, with freelist
  candidate materialization1890ms and zipper ApplyWithOptions1120ms cumulative;
  cumulative stacks overlap.

## Remaining gates and custody

Finalize metadata trim/public test, then root transplants the narrow patch onto
current main and rebinds final source tests/benchmarks. Ordinary177 has unrelated
native primary-directory additions; copying the whole zipper.go onto main is
incorrect. Scratch code and maintained test helpers are otherwise identical on
main3f87. Root owns commits, push, PR/review/CI, integration, and acceptance.
No generated paths have been released or deleted. All files under this evidence
root and the source worktree remain evidence-bearing until root accepts custody.
