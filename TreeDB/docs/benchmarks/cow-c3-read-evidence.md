# C3 read-admission evidence tooling

The fixed-history public Store benchmark and retained evidence validators are
documented in [scripts/cow_c3_read](../../../scripts/cow_c3_read/README.md).
Run the 54-leaf 1x correctness/schema smoke before freezing expensive matched
collection. This tooling is instrumentation only; no measured improvement,
bounded pruning, C4 or parent completion follows from landing it.

Use the documented three durability profiles, three memtable modes, two value
layouts and three actual-call workloads. Retain exact-key/history, ordinary
ACK, source-rotation and COW ownership counters with time/allocation evidence.
Concurrent reader phase rates and individual latencies are separate from
combined writer-dominated elapsed time. Workload overlap does not identify an
internal prepared-publication phase.

Retained collection uses a reviewed landed harness/schema and frozen product,
binary, full source/build/module input identities. Preserve failed packets and
all noise; no fallback, benchmark output reinterpretation or gate waiver.
