# Durable raw COW transactions: admission and 16-client progress

Phase 1 measured the public read/modify/write call, not raw KV or a native
semantic transaction. It established that six stock fixed-concurrency
16-client cohorts refused before completing the requested work. Each
acknowledged prefix was independently reopened. This follow-up addresses two
specific refusals without changing log assignment, durability, or caller retry.

## What changed

1. A nonempty ordinary raw durable commit reserves its own accepted entry and
   an ordinary spare for a possible group marker together. No partial
   reservation survives before the existing bounded checkpoint/recheck. The
   original group owner adopts the spare or refunds it before append.
2. A qualified raw Begin or pre-assignment ordinary commit now joins the
   original checkpoint condition rather than returning capacity merely because
   an already-active checkpoint has marked its basis for refresh. Terminal WAL
   readiness retains precedence, an unreconciled basis is still refused, and
   no extra writer or replay mechanism was added.

The first change by itself passed the deterministic slot fixture but did not
complete the full sustained gate. The follow-up diagnostic reproduced six
Begin refusals at the refresh guard, before budget, snapshot, or assignment.
These instrumented times are not performance comparisons.

## Qualification

Base runtime `217803a6befcb4becbb67b4a9e06274bca5c284e`; pinned stock comparator `1b710ee3f135c0666c246b7680a1e4709a246fad` has the identical runtime (the only changed Go sources are the benchmark and its tests).
Exact pre-evidence source tree `8af80b9b88fa9bfedb0ecb0362d549c1d4e47e49`; executable SHA-256 `4de327f892caca122c95ccc5b4e519b5b405779cb526b67736a0253c2c157f47`.
Same driver source SHA-256 `388e7be968e5329a33c16cffc749d257bce6994d3e3748c62800e6d3d1856776`. Go 1.26.11, Linux/amd64, GOMAXPROCS 2, ordinary durable Commit, stock limits, automatic maintenance enabled, separate fresh processes.
All 15 focused top-level tests passed both normal and race-instrumented; the latter are not timed performance samples.

The candidate completed all six large 16-client cells, with in-window automatic maintenance, exact raw/WAL/logical counts, verified checkpoint and a separate successful reopen for every cell:

| Workload | Completed in each repeat | Logical ops/s | p99 (ms) | p99.9 (ms) | Full foreground checkpoint entries |
|---|---:|---:|---:|---:|---:|
| Disjoint | 400,000 (3/3) | 11,716–12,035 | 9.71–10.33 | 170.95–179.34 | 735–737 |
| Shared hot pair | 800,000 (3/3) | 13,493–13,838 | 9.90–10.09 | 140.92–153.02 | 1,302–1,342 |

Logical latencies include retries; the hot cohort observed 87,034–88,755 conflict rejections per 800,000 completed transfers. No generic capacity retry or new busy loop is used. The fixed concurrency percentile is not open-loop service latency.

Completed stock/candidate controls used alternating fresh-process order; each row is the median of three runs per side:

| Workload | Stock / candidate ops/s | Stock / candidate p99 (ms) | Stock / candidate allocations/op | Stock / candidate bytes/op |
|---|---:|---:|---:|---:|
| 1 client × 200,000 disjoint | 4,870 / 4,851 | 0.603 / 0.675 | 69.32 / 69.32 | 11,960 / 11,992 |
| 16 clients × 250 disjoint | 18,488 / 18,260 | 3.782 / 3.043 | 74.56 / 74.57 | 15,804 / 15,840 |

The uninstrumented singleton p99 increased in all three pairs: 0.603 ms to 0.675 ms at the medians (+11.9%, +72 µs), exceeding the predeclared +10% gate. **It did not pass that gate.** The review selected a bounded exception for the opt-in raw durable progress repair: the six sustained 16-client workloads completed with preserved ACK semantics where stock completed none, while singleton goodput (−0.38%), allocation count, and physical sync count per operation stayed near matched. This is not a claim of a latency improvement or an exception for other work.

Separately, the one-client ABBA attribution observer matched a single observed command-WAL file-sync span wholly within the same Commit for over 99.99% of operations, and at least 99.65% of the unfiltered top 1%. Its logical p99 reversed direction (stock 896/1739 µs, candidate 628/602 µs), with corresponding actual physical file-sync p99 767/1356 µs versus 546/523 µs and Commit outside the observed file sync 129/212 µs versus 116/104 µs. No processing-path regression was observed by this observer; it does not prove the absence of one, establish a provisioning cause, or supersede the uninstrumented miss.

Separate held-reader runs verified the independent opening view, current balances, an explicit full Checkpoint and fresh reopen. The promoted 48,000-operation held-reader row intentionally crosses the original ordinary prefix pressure threshold.

Reproduction and raw receipts: [admission-results.tar.gz](admission-results.tar.gz), SHA-256 `74474d9fb497d083f30fbb84f8ce4c1fee8645e6d3512c4219059d20a1c94bac`. The archive includes exact plans, variant executable hashes, tests, receipts, and earlier negative and diagnostic cells with their classifications.

This is not a general tail SLA or a full-speed ratio against stock, which did
not complete the same 16-client sustained workload. Any opportunity to avoid
redundant full foreground checkpoints or use the existing pressure-only durable
seal and join requires its own WAL-cleanup and bounded-space qualification;
neither is part of these runtime changes.
