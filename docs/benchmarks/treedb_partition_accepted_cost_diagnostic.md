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

The additional `accepted-comparative-cost` receipt contains:

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

Exact navigation allocations, exact merge allocations, unique RPC bytes,
simultaneous all-daemon peak RSS, query-attributed allocations and repeated-run
latency statistics remain explicitly unavailable. These observations alone do
not close #4809 or support a final performance or speedup claim.

The opt-in adds one retained JSON receipt to the current correctness contract
(516 instead of 515 for this qualification). Collection must bind the exact
product and harness identities, keep the historical builder043 DB as immutable
artifact input, and amend the existing verifier for this additional receipt and
its fields. A successful integration preflight is required on the final
composed source, with the cost option unset. Retained collection remains
`ROOT_PENDING` until review, landing, exact-source freeze and runner admission;
the reviewed existing 8 GiB, zero-swap, offline, GOMAXPROCS=4, `-p=2` limits
and single bounded scope remain applicable.
