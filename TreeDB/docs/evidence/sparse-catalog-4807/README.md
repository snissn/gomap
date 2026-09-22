# Sparse catalog P1 evidence (#4807)

Code candidate: `9597ba93be209c7319541885bcd0631244b5020f`; exact control:
`0d10c1605f8887f9a1cd7e3cb910014a5b23cfe3`. Go 1.26.8, Linux/amd64, AMD EPYC 7763,
GOMAXPROCS 4. Both binaries ran on the same hosted runner with the identical
test-only harness (SHA-256 `9eb752fbe4dae4acdd91d958db675ca1d79b1fbecf52fcacc10252517813b8da`).

[Focused conformance and race](https://github.com/snissn/gomap/actions/runs/35746870025/job/106810642812)
passed: three fixed-peer/sparse repetitions and one sparse race repetition.
The new idle-Raft-connection shutdown regression passed all four runs.
[Paired performance job](https://github.com/snissn/gomap/actions/runs/35746870025/job/106810643124) passed and retained its raw logs
as the workflow artifact. [Machine-readable samples](measurements.jsonl) retain
all 42 measured samples, per-node memory/runtime snapshots and lifetime CPU
records; Go's initial one-operation calibration is excluded.

## Matched control and enabled cost

Medians of three runs, 25 fresh create-collection operations per run. Four real
processes, same remote two-voter data group, command WAL, production Raft commit
and durable local apply; no mock owner, durability weakening or mutation shortcut.

| Case | Write ms/op | Route ms/op | Client B/op / allocs/op | Nodes B/op / allocs/op | Nodes RSS MiB | Goroutines / FDs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Base, 4 catalog voters | 6.189 | 1.172 | 86,080 / 489 | 1,240,202 / 5019 | 169.3 | 112 / 123 |
| Candidate, 4 catalog voters | 6.052 | 1.165 | 84,926 / 489 | 1,237,594 / 5018 | 175.6 | 112 / 123 |
| Candidate, 3 voters + consumer | 6.054 | 1.179 | 84,731 / 488 | 1,241,991 / 4985 | 175.8 | 96 / 112 |
| Candidate, 40 inventory endpoints | 5.975 | 1.173 | 83,422 / 488 | 1,235,830 / 4974 | 174.2 | 96 / 112 |

The equivalent all-voter candidate/control write difference is -2.2%, not a
statistically established speedup. Client and node allocation counts are
essentially unchanged. Retained RSS was 3.7% higher in the candidate all-voter
sample while live heap was approximately unchanged (84.6 MB in both); these short
runs do not establish a peak-capacity claim.

The consumer mode removes a local catalog provider: aggregate goroutines fell
from 112 to 96 and FDs from 123 to 112, with similar observed route/write latency.
The catalog voting set intentionally changes from four voters to three; the
matched before/after control is the four-voter pair, not that enabled-mode
comparison. The 40-endpoint case has only four running processes and 36 dormant
endpoints. It demonstrates inventory overhead, not 40-machine throughput.

## Startup allocation growth

Constructor measurements use 100 operations per sample; medians of three.
The 4-node/2-group before/after control improves from 120,800 B and 1,454
allocations to 113,713 B and 1,242 allocations per construction. It avoids the
full configuration JSON encode/decode copy and parses numeric addresses without
DNS resolution.

| Candidate inventory / groups | Encoded config bytes | Constructor ms/op | B/op | allocs/op |
| --- | ---: | ---: | ---: | ---: |
| Nodes4Groups2 | 2526 | 0.544 | 113713 | 1242 |
| Nodes4Groups32 | 9900 | 5.890 | 1182903 | 12777 |
| Nodes4Groups128 | 33546 | 22.601 | 4791763 | 49663 |
| Nodes40Groups2 | 4118 | 0.603 | 124903 | 1278 |
| Nodes40Groups32 | 11492 | 5.873 | 1232389 | 12811 |
| Nodes40Groups128 | 35138 | 22.820 | 4857233 | 49695 |
| Nodes1024Groups2 | 56868 | 1.318 | 534803 | 2292 |
| Nodes1024Groups32 | 64242 | 6.597 | 1723912 | 13826 |
| Nodes1024Groups128 | 87886 | 23.445 | 5382553 | 50706 |

Construction still validates every declared group's features and storage-path
isolation through existing raftcluster validation. These bounded startup costs
are not per-request allocations and do not open remote stores. Per-call endpoint
lookup is indexed. See the [allocation/lifetime audit and replay commands](../../spec/fixed-peer-tcp-runtime-v1.md#sparse-catalog-qualification-and-allocation-audit).

## Correctness and interpretation

The first semantic red was `440e721`: the real public client rejected a node
outside catalog voting membership. [First-red log](https://github.com/snissn/gomap/actions/runs/35741357046/job/106791635348).
Candidate `d931301` later exposed idle Raft socket handlers during race cleanup;
one targeted retry passed, but the underlying socket ownership defect was fixed
rather than suppressed. The current candidate closes accepted/dialed sockets
and cancels pending dials.

Client allocation counters exclude children. Node allocation deltas include
background Raft and bounded metrics instrumentation. Per-node RSS is sampled;
the sum of individual high-water marks is an upper bound on simultaneous peak,
not a simultaneous-peak measurement. Child CPU includes startup/shutdown.
The workload has empty collections, one writer and loopback transport; it is
not an ANN, data-capacity, contention, network/AZ or EC2 qualification.
#4250 and #3983 retain distributed performance and fault verdict ownership.
