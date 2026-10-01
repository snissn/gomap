# Accepted partition comparative cost diagnostic

`GOMAP_ACCEPTED_COMPARATIVE_COST_V1=1` enables cost observations only in
`TestMultiOwnerTCPAcceptedDisjointRetained100KV1` and its existing
`testMultiOwnerTCPDomainSearchWithQualificationV1` harness. Leave it unset for
the small integration preflight and ordinary correctness tests. Other nonempty
values and incompatible qualification settings fail explicitly.

The same declared 512 queries run once locally and once through the public
strict TCP API, in the same order, on the same retained generation. The fixed
settings remain exact routing C256, P5, EF96, TopK10 and Merge256. The opt-in
adds **16 local warm calls** before the existing local reference pass; the TCP
arm keeps its existing **16 warm calls**. Thus each arm has 528 calls, including
512 observed calls. Existing refusal, loss and reopen controls remain outside
the observed passes. No repeated correctness corpus, ranker change,
retuning, M3 rebuild, TLS/startup change or `readCost` mode is introduced.

Requests and result slices are allocated before the observed windows. Each
window includes the actual search call, per-request deadline update, error
check and result retention. Result validation, truth maps, recall calculation,
JSON encoding and receipt writes occur after the windows. All existing
source identity, exact-five native-call, no-exact-scan, ordered ID/score-bit,
algorithm counter and 95% recall assertions still apply. With the opt-in
unset, validation and per-query receipts retain their existing position.

The instrumented `accepted-comparative-cost` receipt contains:

- Local and TCP-client elapsed batch time and process-global B/query and
  allocations/query from two `runtime.ReadMemStats` boundaries per arm.
  Parent OS reader work is outside the allocation boundaries. The local arm
  includes the in-process coordinator and services; the TCP-client arm includes
  the client process only. Background activity in either process is included.
- Before/after `DiagnosticsV1` for every real daemon, its node ID and PID,
  and process-global allocation deltas divided by 512. These intervals are
  staggered and include diagnostics and background work. Their sum plus the
  client delta is labeled `TCPAllProcess*`; it is an observed process total,
  not precise query or stage attribution.
- Parent and daemon RSS snapshots and each process's lifetime peak RSS.
  All-daemon snapshot sums are staggered observations, not a simultaneous
  peak. Unsupported daemon RSS aggregates are `null`; inspect each diagnostic's
  `Unavailable` list. No lifetime peaks are summed as a simultaneous peak.
- Peer-transport stream write deltas when an enabled inventory exists on
  every daemon. Missing inventories produce `null`, not a measured zero.
  Writes are counted without adding endpoint reads. They include control,
  Raft, diagnostics and background traffic; exclude public nativewire sockets,
  IP/TCP headers and retransmissions; and are not unique RPC bytes. This option
  never activates another transport mode to make byte counters available.
- Local/public candidate-byte totals, with the existing
  `accepted-public-parity` receipt retaining all 512 algorithm counters and
  timings. Candidate bytes are semantic accounting, not allocated bytes.

The same receipt also records the following test-only observations:

- `ShardRequestFrameBytes` and `ShardResponseFrameBytes` subtract the two
  ingress dispatcher connection counters. They count successful plaintext
  writes and reads once, including the four-byte frame length prefix and any
  retries, across the declared ingress-to-owner shard boundary. Installation
  precedes all shard connections; the 16 warm queries precede the before
  boundary. HTTP diagnostics/control, Raft, the public-client connection,
  TLS records, IP/TCP headers and retransmissions are excluded. Reads and writes
  represent opposite message directions, so endpoint reads are never added to
  sender writes for the same message. These are actual framed bytes at this
  boundary, not unique logical payload bytes or all deployment network traffic.
- `ServingRSS` samples the fixed four owned daemon PIDs through Linux procfs
  at a nominal 10ms interval over an envelope surrounding the TCP pass. It retains each node's maximum
  sampled RSS, the maximum sum observed in a serial round, sample count, first
  and last round timestamps, and maximum round duration. The PID order matches
  `Daemons`; the aggregate excludes the client process. Both maxima are sampled
  lower bounds. Serial round skew and scheduling delay prevent interpreting the
  sum as a simultaneous or exact serving-window peak. A missing/malformed PID
  sample refuses this collection instead of reporting zero. The observer starts before the client counter boundary; its final in-flight
  round can finish after the ending boundary. The recorded round timestamps
  expose this envelope. It is canceled and drained on completion or failure.
- `AllocationProfiles` names two local and two per-daemon cumulative sampled
  allocation snapshots, with `MemoryProfileRate` and `DaemonProfileBoundaries` (before nodes in PID
  order, then after nodes in the same order). Child rates must match the parent.
  Two GCs flush samples before each snapshot, outside search timers and global
  counter boundaries. Use before/after differences and navigation/merge stack
  locations to inspect statistical attribution; retain the full profiles.
  These diagnostic test profiles are not unified-bench/benchprof inputs.
  Interval differences also include background, observer, diagnostics and
  profile/control allocations. They do not give exact query or stage costs.

The observation controls use only the existing child test stdin pipe and
trusted parent-owned output directory; they add no production endpoint. The
three-command sequence and total command input are bounded. With the cost flag
unset, no frame wrapper, sampler, profiles or child command reader is enabled.

Atomic frame counters and the RSS observer change the instrumentation envelope.
RSS parsing/control allocations contaminate client process-global allocation
numbers, and profiling GCs can change subsequent cache/GC behavior. Compare
this collection's correctness and declared observations with retained history;
do not treat its timings or allocation totals as an apples-to-apples regression
or speedup measurement against the earlier uninstrumented cost samples.

Exact navigation allocations, exact merge allocations, wire bytes outside the
declared shard-frame boundary, simultaneous all-daemon peak RSS,
query-attributed allocations and repeated-run latency statistics remain
explicitly unavailable. These observations alone do not support a final
performance or speedup claim.

The opt-in adds one retained JSON receipt to the current correctness contract
(516 instead of 515 for this qualification). Collection must bind the exact
product and harness identities, keep the historical builder043 DB as immutable
artifact input, and amend the existing verifier for this additional receipt and
its fields. A successful integration preflight is required on the final
composed source, with the cost option unset. Retained collection remains
`ROOT_PENDING` until review, landing, exact-source freeze and runner admission;
the reviewed existing 8 GiB, zero-swap, offline, GOMAXPROCS=4, `-p=2` limits
and single bounded scope remain applicable.


The next retained contract is one reviewed and landed instrumented harness,
one exact-source freeze and one admitted observation of the unchanged builder043
53DB input tuple. Preserve the 516 JSON receipts and additionally retain all ten
named allocation snapshots. The artifact verifier must check positive monotonic
frame deltas, child/profile rate consistency, each profile's existence and seal,
RSS PID order/nonempty rounds/timestamps and sampled-maximum arithmetic bounds.
Do not invent exact attribution from these fields. Keep the existing semantic
parity, recall, five-native-call, owner-loss and reopen assertions unchanged.

Before that allocation, the focused source guardrails are
`TestFixedPeerCostObservationBoundariesV1`,
`TestMultiOwnerTCPAcceptedModelFreshIndexEpochV1`, and
`TestMultiOwnerTCPCostSetupAfterActiveFreshIndexEpochV1` with the comparative
cost flag unset.
The observation top enables the flag only within scoped child subcases. A real
non-ingress runtime checks setup/before/after acknowledgements, sampling rates,
timestamp order, nonempty profile files and clean EOF shutdown; an out-of-order
command must fail without creating its profile or acknowledgement. A minimal
ingress topology tests installation before connection pools, occupied-pool
refusal and actual shard framing through the installed dial wrapper. The RSS
subcase checks sampled-sum arithmetic and canceled observer drain. The existing
64-row fixture exercises ordinary serving with the option unset. The added
fresh64 top scopes `GOMAP_FIXED_PEER_COST_SETUP_PREFLIGHT_V1=1` to enable only
the existing child setup pipe: setup follows successful ACTIVE lifecycle warm,
while the real ingress dispatcher still has no shard connections, and precedes
readiness/status checks and all public searches. Setup acknowledges zero frame
bytes and an enabled sample rate; EOF retains the existing clean child drain.
It creates no allocation profiles or comparative receipts, and refuses retained
input/receipt paths, comparative mode, and changed query settings. Leave this separate
preflight flag unset in ordinary tests and retained collection. These bounded
checks qualify the observation mechanisms, not 100K ANN profile attribution or
the retained fixture's performance. The pinned 512-query qualification guard
remains unchanged.

For retained profile inspection, use the existing Go pprof tool on each matched
pair, for example `go tool pprof -sample_index=alloc_space -base
accepted-cost-node-0-before.pprof accepted-cost-node-0-after.pprof` and inspect
navigation and coordinator merge stacks; repeat with `alloc_objects`. Run tools
only under the runner's allocation policy. Preserve unfiltered profile evidence
and the sampling rate, and label displayed stage values as sampled estimates.
