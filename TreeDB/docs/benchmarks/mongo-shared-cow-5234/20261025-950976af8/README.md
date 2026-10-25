# Ordinary standalone Mongo shared-COW qualification

> Historical developmental evidence for source `950976af8688ba0a5ed7eaced44b82da25c44181`. The final reviewed runtime and fresh complete qualification are in [the d040 report](../20261025-d0407930e/README.md). Original outcomes and dispositions below remain source-bound; this packet does not qualify later runtime revisions. Reproduction now uses the landed maintained driver and frozen structured vector commands described in that report.

Source/runtime: `950976af8688ba0a5ed7eaced44b82da25c44181`.
Baseline: `f906b8a2aa1279ed6b6627028cba71d491db04c3` with the identical
three maintained Go harness files. Go 1.26.0, GOMAXPROCS=4, Linux/ext4 on a local
i5-11400F host. No dedicated runner was reserved; the environment and unavoidable
background services are disclosed in [environment.json](environment.json).
Graph-owned builds/tests/checkouts/cleanup were paused during retained collection.

The coordinator accepts the measured first Mongo slice with the explicit rare
maximum-request latency and vector allocation tradeoffs below. This is not
parent #5225 completion or universal collection/default adoption. Exact-head
CI/review and merge are separate delivery gates.

## Primary Mongo results

The official Mongo driver uses the actual product standalone opener, w:1,
j unspecified, retries disabled, 64 rows/writer, 256-byte documents and two
secondary indexes. All 64 fresh-process samples validate activation, fixture,
count, process/PASS and metrics. Eight alternating pairs per cell all meet
the predeclared median paired time/bytes/allocations objectives.

Ratios are candidate/baseline; smaller is better. All individual paired ratios
are retained in [primary-analysis.json](primary-analysis.json).

| Profile / writers | Time | B/op | Allocs/op |
|---|---:|---:|---:|
| command_wal_durable / 1 | 0.89468 | 0.45002 | 0.55837 |
| command_wal_durable / 8 | 0.19090 | 0.21623 | 0.23444 |
| command_wal_relaxed / 1 | 0.23696 | 0.37949 | 0.56691 |
| command_wal_relaxed / 8 | 0.03871 | 0.19716 | 0.25920 |

The interval charges all requested ACKs and whole-process driver/gateway
allocations. Setup and disconnect/Close are separately recorded. It is not a
raw storage microbenchmark or a comparison across weaker durability profiles.

## Held-reader resources and latency

All 24 runs complete 10,000 updates with 4 KiB payloads and a retained ordinary
read view. Old membership/documents remain stable. Reader release, two seconds
of natural idle maintenance, and Close/GC are separately observed. No forced
checkpoint is inserted between phases. All counts/latencies are valid; full
phase values/counters and individual pairs remain in
[resource-analysis.json](resource-analysis.json) and the raw archive.

| Profile / writers | ACK time | p99 | Max | Sampled heap peak | RSS peak | File bytes peak |
|---|---:|---:|---:|---:|---:|---:|
| command_wal_durable / 1 | 0.8414 | 0.5231 | 4.5292 | 0.5697 | 0.2450 | 0.0414 |
| command_wal_durable / 8 | 0.6495 | 0.5217 | 1.5112 | 0.4602 | 0.4435 | 0.3029 |
| command_wal_relaxed / 1 | 0.2845 | 0.0772 | 8.9897 | 0.4957 | 0.2311 | 0.0367 |
| command_wal_relaxed / 8 | 0.1179 | 0.0812 | 1.3034 | 0.5436 | 0.4445 | 0.1804 |

Peaks cover all observed phases, including setup/Close; they are sampled rather
than guaranteed maxima. Reservations include fixed maintenance and codec/live
ownership and are not RSS or a measurement of reclaimable native bytes.
Regular-file sizes include WAL and are not allocated disk blocks or WAL-excluded
storage certification. Three pairs provide qualitative evidence, not a precise
5% tail or RSS certificate. Missing closed counters remain unavailable.

**Accepted limitation:** candidate maxima are 119–146 ms, with median paired max
ratios 1.30–8.99. The exact `950` ELF CPU/runtime tracing identifies the slow request
waiting for the synchronous producer-pressure checkpoint's dependency-closed
file/directory sync sequence. No safe redundant sync omission was established.
This accepts a rare maximum-latency cost alongside lower p99, ACK time and
footprint; it does not claim maximum parity or an absolute latency bound.
No established maximum-request SLO is changed. Durability, hard capacities and
old-reader custody remain unchanged. See [disposition.json](disposition.json).

A source-reviewed attempt to wake the existing checkpoint worker at an earlier
reservation watermark was rejected. Its exact `7abcab5eb` public relaxed/one probe
completed 10,000 operations without correctness failure and reduced max to 21.15 ms,
but performed 104 real handoffs, raised p99 to 10.77 ms and ACK time to 4.144 s
(950 relaxed/one ACK is about 1.78 s). Fixed/live/codec reservations cannot identify
reclaimable debt. The attempt's production delta and regression test are removed
from the selected tree; its patch, red/green checks, failed performance probe and
source consultation remain in diagnostics-and-controls.tar.gz. The original
shared pressure boundary and configured WAL-size worker policy are retained.

## Ordinary vector guardrail

All 18 fresh processes validate the actual product route/profile, 512 BSON rows,
8 dimensions, cosine/M=16, exact benchmark/count, successful result checks and PASS/exit 0.
The maintained harness uses TopK=5 and disables exact fallback. Three alternating
pairs separately time 10 Build/Save/hotLoad operations, 10 Save/physicalClose/
reopen/Load operations and 10,000 warmed Search operations. Calibration is not
an additional final sample. Detailed boundaries are in the benchmark README.

| Operation | Time | B/op | Allocs/op |
|---|---:|---:|---:|
| BuildSaveLoad | 0.99259 | 0.34280 | 0.40794 |
| SaveReopenLoad | 0.74546 | 0.22554 | 0.89516 |
| Search | 0.33020 | 0.47012 | 1.26667 |

Search's 19 versus 15 allocations are explicitly accepted for its ~67% time and
~53% byte reductions and source-owned private query/result storage. This is
independent of the primary Mongo allocation objectives, which all pass. Build/
Save/hotLoad time is roughly unchanged. No universal 5% vector timing claim is
made from three pairs. See [vector-analysis.json](vector-analysis.json).

## Source, compatibility and failure history

The constructor/materializer correction passes twelve JSON/BSON × declared/
later-DDL × three-profile HNSW lifecycle cases and actual repeated finite
create refusal/no-effects/release/retry under race (7.900 s). Whole collections
(544.524 s), gateway (91.010 s) and harness (8.339 s) pass individually. The original
multi-package command included a nonexistent extra gateway path and retains its
overall setup failure. Dictionary/atomic ownership repairs retain causal red,
race, capacity/lifetime and public concurrent Set/read evidence. Public held-
reader pressure passes with several handoffs and final-zero ownership. Earlier
auth/TLS, process-exit/reopen, partial outcomes, compound queries, 16 MiB BSON and
catalog cutover controls remain applicable and are recorded in PR #5235 and the
linked checkpoint. This report does not promote text/service/typed classes.

All prior 246/be187/c51 campaigns are preserved. They are superseded, not
replacement samples: 246's primary numeric pass predates a deadline correction;
be187 includes three real trained-dictionary failures and two graph-I/O
isolation-inconclusive observations; c51 intentionally stops for the constructor
correction with 9 valid resource samples, one interrupted raw/null and 14 unstarted/
null rows, and no vectors. Their plans, raw outputs and original dispositions
are in prior-campaigns.tar.gz. No failed/null observation was discarded or
retried into acceptance.

Pushed 950 CI completed 55 successful checks. Its strict public PRIMARY gate
passes (baseline 98,754→candidate 98,753.5 B/op; 372 allocations both; paired time
+0.409%). Runtime-input-equivalence is explicitly false. Earlier be187/c51
strict byte failures remain retained under external owner #5191; the new pass
is not causal attribution or an estimator repair. CI PRIMARY summaries and all
eight raw pairs for 246/be187/c51/950 are included in diagnostics-and-controls.
No threshold relaxation or failure waiver was used. Existing vacuum stability
history under #4519 is not a merge waiver. Artifact-only descendants must
still pass their own current-head CI/review.

## Evidence and reproduction

- [manifest.json](manifest.json): exact commits, Go harness blobs, ELF hashes,
  driver hash, counts, commands and baseline overlay identity.
- [raw-950.tar.gz](raw-950.tar.gz): all 64+24+18 raw logs, original receipts,
  plans, driver outputs and sanitized executable-name process inventories.
- [prior-campaigns.tar.gz](prior-campaigns.tar.gz): complete earlier campaign
  raw logs/receipts/failures/nulls, without private process arguments.
- [diagnostics-and-controls.tar.gz](diagnostics-and-controls.tar.gz): original
  trace/CPU profile and reduced causal analysis, rejected experiment, relevant
  failure/control logs and CI PRIMARY raw pairs. Huge regenerable trace text
  expansions and ELF binaries are omitted; exact ELF hashes/recipes remain.
- [evidence-index.json](evidence-index.json): archive/member SHA256 and explicit
  private full-process-argument omissions with their original SHA256. Published
  process inventories retain IDs/CPU/memory/elapsed/executable basename only.

Reproduce using the maintained measurement spec and
`cmd/mongo_gateway_bench/scripts/run_shared_cow_qualification.py`. Build each exact
source with Go 1.26.0 after installing the three identical harness files on the
baseline, verify hashes, freeze a fresh manifest and run its fixed batches in a
quiet slot. Original local receipts use absolute artifact paths; substitute new
paths explicitly rather than editing a past receipt. Failed runs remain failed.
The vector collector and its pre-review are included in raw/prior artifacts.

[publication-binding.json](publication-binding.json) records that the published
descendant changes only this evidence directory and the protocol clarification;
all runtime and harness bytes remain those of the measured source.

The original analysis JSON files retain their pre-disposition wording. The
coordinator's later acceptance and limitations are in disposition.json and this
report; raw receipts are unchanged. Parent #5225 remains open for actual named
consumer adoption and remaining supported capability classes.
