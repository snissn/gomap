# #4090 TreeDB vector serving on-ramp profile

This packet is the bounded before-state for the vector serving hot-path
overhaul. It measures the production public route on one 250k graph-overlap
fixture and proves that catalog/lifecycle work, not local ANN search or raw TCP,
dominates current latency. It is not a replacement for the frozen five-system
gold matrix and makes no post-optimization performance claim.

The committed packet is about 6 MiB. The 67+ GiB scratch bundle containing
copied databases, binaries, raw pprof files, and traces stays outside the
repository at
`/mnt/fast4tb/gomap-4090-vector-onramp-profile-6c83bd1e`.
`RAW_PROFILE_SHA256SUMS` and `RAW_EXTERNAL_SHA256SUMS` bind the omitted files.

## Exact identity

| Item | Value |
| --- | --- |
| Base | `be80a3ff55ef2255afe21a77bf00fb1f61ba65c6` |
| Instrumented head | `6c83bd1e1f8b0a0aab29bf0fe1cc846fdc944580` |
| Benchmark binary SHA-256 | `00b6594b4d14b13169bd4cdc114020d79904facb5c139359602d661a9a7ce99b` |
| Container image | `sha256:63845dd3d628b68514606286b49cd4bbb1fb488011532c836ea36ee184cde80d` |
| M3 descriptor SHA-256 | `f2ce9f0928e6dbb84793462962639c414f790e7cd4a3b34b323c3b123180ef47` |
| M3 artifact SHA-256 | `57ad36d923c5fdb701a082727fd24efdcf0c6ac0e24efeda28ca11f232a65f1d` |
| Fixture checksum | `d0c7c82ba868853aae9a4280161003d72714ad1701d41ed3169c2fa94d470d69` |
| Truth artifact SHA-256 | `5a518c1cb8182edc685ab692dc17a6974655572f426a4b97c10482fd1643f04e` |
| Toolchain | `go1.26.0 linux/amd64`, `CGO_ENABLED=0`, `vcs.modified=false` |

The host was Linux 6.8.0-124-generic on one Intel Core i5-11400F socket with
six physical cores, 12 logical CPUs, one NUMA node, and 31 GiB RAM. Evidence,
copied databases, Raft stores, and profiles were on the same `/mnt/fast4tb`
NVMe filesystem.

The exact-head M3 rebuild completed once with exit 0 in 13:57.28 and peak RSS
2,172,696 KiB. The fixture has 250,000 source vectors, 16 partitions, overlap
0.20, EF128, router candidate budget 256, p2, top-k 10, and 1,000 truth-bound
queries. Every measured cell retained recall 0.9247 and exactly matched selected
partition/group/candidate/edge work across controls.

## Method

Four TreeDB controls ran serially with three fresh repetitions each:

- one process owning all four data groups;
- four native processes with the host scheduler's default ownership;
- four native processes pinned to disjoint three-CPU sets with `GOMAXPROCS=3`;
- four containers pinned to the same disjoint CPU sets.

Each repetition measured p2 at c1 and c32 through
`vectorpartition.OperationsV1.Search`. The reducer verifies exact source,
binary, fixture, truth, topology, node readiness, database-root isolation,
client command, exit status, generation, raw durations, QPS, percentiles,
counters, catalog before/after deltas, Raft log movement, and process-wide
runtime deltas. Concurrent stage timings are aggregate request work and are not
mislabelled as serial wall time.

One additional pprof/trace capture was made for each control at c1 and c32.
Those one-shot rows are diagnostic only. The first single/c1 profile attempt
failed before readiness because the external runner had not created the
configured profile directory. Its logs are retained; the runner was fixed,
the failed root was not reused, and all eight subsequent profile rows completed
with exit 0.

## Reduced throughput

`vector-onramp-profile.json` is intentionally `tainted`, not invalid: every
correctness and attribution check passed, but at least one three-run QPS/p95
spread exceeded 10%. The medians below are diagnostic before-state evidence,
not promotion numbers.

| Cell | Single | Native default | Native budgeted | Container |
| --- | ---: | ---: | ---: | ---: |
| c1 QPS | 38.75 | 34.96 | 35.27 | 34.92 |
| c1 p95 ms | 43.74 | 49.55 | 49.60 | 49.66 |
| c32 QPS | 163.31 | 110.84 | 127.19 | 119.42 |
| c32 p95 ms | 242.39 | 382.79 | 426.06 | 402.62 |

CPU pinning did not reliably remove the native gap. Explicit daemon/runtime
ownership remains a separate production control, but it is not the primary
latency root.

## What one c1 query costs today

The single-process c1 median provides the cleanest serial accounting:

| Owner | Time per query |
| --- | ---: |
| End-to-end elapsed | 25.807 ms |
| Operations catalog health | 7.310 ms |
| Coordinator lifecycle check | 7.543 ms |
| Shard dispatch/RPC span | 10.388 ms |
| Summed shard generation-open work | 16.684 ms |
| Summed local HNSW search | 0.628 ms |
| Summed network work | 0.481 ms |
| Process CPU | 4.296 ms |
| Allocation | 954.6 KiB |
| Context switches | 161.9 |

The raw Linux rows contain sampled `/proc/self/task/*/schedstat` run-queue and
timeslice fields. They are retained for byte provenance but excluded from the
reduced result: exited OS threads disappear from that interface, so endpoint
subtraction cannot qualify a complete process delta. Current instrumentation
publishes those compatibility fields as unavailable and relies only on
process-wide `getrusage` CPU/context-switch counters, which retain exited
workers.

The catalog attribution is exact: p2 performs 3.834 catalog reads and 3.834
catalog `LogBarrier` calls per query--one Operations health check, one
coordinator lifecycle check, and 1.834 shard lifecycle checks. Those calls
advance the catalog Raft log by 3.834 entries per query. Their summed work is
31.386 ms/query, of which 30.619 ms is barrier time; internal groups can run
concurrently, so summed work may exceed end-to-end wall time.

The same shape persists in every control. Native-default c1 spends 34.709 ms
of summed catalog work for 28.604 ms end-to-end wall time, while local HNSW is
only 0.604 ms. At c32, stage fields are aggregate request work rather than wall
time; the throughput unit is 9.022 ms/query native-default versus 6.123 ms/query
single, while the catalog work totals 308.687 and 235.796 ms/query respectively.

## Profile proof

The text reports under `profile-summaries/pprof/` are derived from the raw
profiles bound by `RAW_PROFILE_SHA256SUMS`.

- Single c1 CPU: storage-barrier/lifecycle authority loading consumes 5.12 s
  cumulative, generation pinning 4.61 s, manifest integrity/encoding 4.45 s,
  and JSON marshal 3.34 s. Local HNSW is about 1.42 s cumulative.
- Single c1 allocation delta: 8.028 GiB across warmup plus the measured cell.
  JSON marshal is 2.407 GiB, physical asset reads 1.251 GiB, native uint64
  scratch 570 MiB, typed-column offsets 547 MiB, and candidate scratch 271 MiB.
- Native c32 public node: the full manifest membership scan used to derive
  candidate rows is 1.07 CPU-s flat / 1.14 s cumulative, about 15% of sampled
  CPU on that node. This is a distinct local algorithmic target after the
  serving snapshot removes repeated lifecycle reconstruction.
- Native c32 syscall delay: 67.37 of 97.67 seconds is `fdatasync` below
  `raft-boltdb.StoreLogs`. This directly proves that the normal read path is
  causing durable catalog Raft writes; it is not ordinary socket cost.

Repeated lifecycle validation also replays checkpoint state, recomputes and
JSON-encodes manifest digests, reloads physical column assets, resizes typed
column scratch, and reopens local search sources. Those are publication-time
or generation-activation costs currently paid on the query path.

## Optimization graph and hard gates

The profile supports a structural fast path, not barrier overlap or a chain of
small percentage tweaks:

1. **#4092 + #4096, one coherent serving-snapshot/proof change.** Publish one
   immutable snapshot containing generation, catalog epoch, topology/ready-set
   digests, applied floors, router handle, and shard handles. One query pins it
   once. Raft apply replaces/invalidates it and old pins drain safely. A single
   request proof is propagated through Operations, coordinator, and shards.
   A background current-term proof/lease refresh makes the default leased path
   perform zero catalog I/O.
2. **#4091, explicit runtime ownership.** Keep daemon CPU/memory ownership
   deterministic and observable, but do not treat pinning as the catalog fix.
3. **#4097, local request-pipeline work after the control plane is gone.**
   Precompute immutable per-partition row counts instead of scanning all
   memberships per query, retain prepared typed-column/search scratch in the
   serving snapshot, then remove the remaining wire encode/copy allocations.
4. **#4093, focused revalidation.** Re-run the same TreeDB controls against the
   frozen gold rubric before requesting any full five-system matrix rerun.

The structural promotion gates are:

- zero catalog `LogBarrier` entries per normal search;
- no more than one no-log catalog proof for `linearizable_generation`;
- zero catalog network calls for a valid `leased_generation` request;
- exactly one immutable serving-snapshot pin per query;
- no per-query lifecycle checkpoint replay, manifest digest encode, or physical
  search-asset reopen;
- mutations fail closed, replace the snapshot on apply, and drain old pins;
- shards reject mismatched or expired proofs without a catalog round trip;
- retain data-group `ReadIndex` until separately justified by evidence;
- preserve recall, exact work counters, generation identity, and fail-closed
  behavior before comparing throughput.

## Evidence and validation

- `runs/` retains all 12 normal result rows with topology, readiness, exact
  client command, runner resources, and exit/timing records.
- `profiles/` retains the eight one-cell result/topology/readiness records but
  omits database copies and binary pprof/trace payloads.
- `profile-summaries/pprof/` retains text CPU, allocation-delta, block, mutex,
  scheduler, synchronization, and syscall summaries for every profiled node.
- `m3/` retains the exact descriptor, compact build report, whitespace-normalized
  command, timestamps, exit record, and resource time. The raw command bytes and
  large raw M3 JSON are hash-bound externally.
- `tools/` retains the exact external runners and rebind helper source. The
  successful native runner SHA-256 is
  `0a300b4a70fc554a235b2b9fb23ec9cfd654c552b3fcd84eaa52a87bb51c4f82`;
  the container runner SHA-256 is
  `15bb82a424b7ce43e5dec4e39f956140a05e42e430562897e5dfa98f69e5874d`.
- `SHA256SUMS` binds every committed evidence file except itself.

Validate from the repository root:

```sh
(cd TreeDB/docs/evidence/vector-partition-onramp-profile-4090/6c83bd1e && sha256sum -c SHA256SUMS)
(cd benchmarks/vector_db_compare && python3 -m unittest test_topology_tax.py test_vector_onramp_profile.py)
```

The raw profile inventory can be checked while the scratch root is retained:

```sh
(cd /mnt/fast4tb/gomap-4090-vector-onramp-profile-6c83bd1e && sha256sum -c /mnt/fast4tb/gomap-4090-vector-onramp-profile/TreeDB/docs/evidence/vector-partition-onramp-profile-4090/6c83bd1e/RAW_PROFILE_SHA256SUMS)
```

## Limitations

- One Linux host, one deterministic 250k corpus, same-host TCP/bridge networking.
- Three normal repetitions, but the result is timing-tainted by spread above
  10%; profiles are one-shot diagnostic captures.
- The c32 stage values are aggregate request work, not serial wall components.
- The allocation profiles include the p16-shaped warmup and therefore establish
  owners, not exact bytes per measured p2 query.
- No full comparator matrix was regenerated; the previously frozen matrix
  remains the performance rubric until the focused TreeDB fast path passes.
