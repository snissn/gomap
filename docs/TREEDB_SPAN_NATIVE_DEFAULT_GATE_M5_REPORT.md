# TreeDB span-native default gate M5 report

Issue: #2788. Parent tracker: #2782.

## Decision

M5 enables a conservative TreeDB default admission policy for the span-native
flush/apply + backlog coalescing stack:

- default/unconfigured `FlushAdmissionPolicy` is `auto`;
- `auto` admits the measured `c4` span-native + backlog candidate with adaptive
  write-side outer-leaf cache admission when the low-concurrency and durability
  guardrails pass;
- `auto` declines low-concurrency/c1-shaped configurations and WAL-off unsafe
  durability;
- `FlushAdmissionPolicyOff` remains the immediate rollback knob;
- `FlushAdmissionPolicyExplicit` preserves explicit opt-in/override behavior for
  c4/c16/immediate/cache-disabled comparison rows and experiments.

The chosen default is **not** the write-only ceiling row. `c16` and
cache-disabled rows remain explicit comparison/diagnostic rows because they carry
higher allocation/contention or remove the read-cache axis.

## Artifact roots

Public paths intentionally redact the benchmark host, user, address, and raw
mount path.

- Final gate root:
  `<remote-profile-root>/2788_final_gate_20260617_034229/final_10mm`
- Final parsed summary:
  `<remote-profile-root>/2788_final_gate_20260617_034229/final_10mm/2788_final_gate_summary.md`
  and
  `<remote-profile-root>/2788_final_gate_20260617_034229/final_10mm/2788_final_gate_summary.json`
- Write/checkpoint parser summary:
  `<remote-profile-root>/2788_final_gate_20260617_034229/final_10mm/write_checkpoint/m14_matrix_summary.md`
  and
  `<remote-profile-root>/2788_final_gate_20260617_034229/final_10mm/write_checkpoint/m14_matrix_summary.json`
- Settled-scan rechecks:
  - `<remote-profile-root>/2788_final_gate_20260617_034229/scan_recheck_10mm`
  - `<remote-profile-root>/2788_final_gate_20260617_034229/scan_recheck2_10mm`
  - `<remote-profile-root>/2788_final_gate_20260617_034229/scan_recheck3_reversed_10mm`

## Matrix command policy

Because the branch changes unified-bench's default
`-treedb-flush-admission-policy` to `auto`, explicit comparison rows must pass an
explicit policy flag:

- unconfigured candidate row: no admission-policy flag, relying on default auto;
- rollback rows: `-treedb-flush-admission-policy=off`;
- explicit c4/c16/cache-disabled rows:
  `-treedb-flush-admission-policy=explicit` plus the row-specific knobs.

Common final 10MM shape:

```text
-dbs treedb
-keys 10000000
-valsize 128
-batchsize 8000
-path-label m8-m14-10mm-gate
-treedb-journal-lanes=1
-progress=false
```

Write/checkpoint rows used:

```text
-test sequential_write,batch_random,random_write
-checkpoint-between-tests
```

Settled point-read rows used:

```text
-test sequential_write,random_write,random_read
-checkpoint-between-tests
-settle-before-scans
-treedb-cache-stats-before-reads
-read-require-hit
```

Settled scan rows used:

```text
-test sequential_write,random_write,full_scan,prefix_scan
-checkpoint-between-tests
-settle-before-scans
-treedb-cache-stats-before-reads
-range-queries 200
-range-span 100
```

Mixed/debt read+scan rows used the same read/scan tests without a settle
boundary before reads/scans.

## Write/checkpoint gate

| Row | Policy | Admitted | Reason | Cache admission | random_write | Δ random | RW checkpoint | checkpoint efficiency | write+checkpoint | Δ write+checkpoint | post-run checkpoint |
| --- | --- | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `main_default_unconfigured` | explicit | false | `no_explicit_opt_in` | immediate | 159,527 | +0.0% | 11.20s | 0.893 Mops/s | 135.3 kops/s | +0.0% | 9.29s |
| `branch_default_auto` | auto | true | `auto_admitted_c4_adaptive` | adaptive | 272,400 | +70.8% | 7.89s | 1.267 Mops/s | 224.2 kops/s | +65.7% | 6.44s |
| `branch_forced_off` | off | false | `policy_off` | immediate | 152,289 | -4.5% | 11.06s | 0.904 Mops/s | 130.3 kops/s | -3.7% | 7.43s |
| `branch_auto_c1_decline` | auto | false | `low_concurrency` | immediate | 149,759 | -6.1% | 9.03s | 1.107 Mops/s | 131.9 kops/s | -2.5% | 8.82s |
| `branch_explicit_c4_adaptive` | explicit | true | `explicit_opt_in` | adaptive | 271,405 | +70.1% | 7.85s | 1.274 Mops/s | 223.7 kops/s | +65.3% | 8.35s |
| `branch_explicit_c4_immediate` | explicit | true | `explicit_opt_in` | immediate | 288,985 | +81.2% | 9.96s | 1.004 Mops/s | 224.4 kops/s | +65.8% | 10.65s |
| `branch_explicit_c16_immediate` | explicit | true | `explicit_opt_in` | immediate | 295,189 | +85.0% | 7.24s | 1.381 Mops/s | 243.2 kops/s | +79.7% | 5.04s |
| `branch_explicit_c4_cache_disabled` | explicit | true | `explicit_opt_in` | diagnostic disabled cache | 303,888 | +90.5% | 7.84s | 1.276 Mops/s | 245.4 kops/s | +81.3% | 9.04s |

Interpretation:

- The default-auto row materially improves random-write throughput and
  write+checkpoint throughput versus current main default.
- Checkpoint boundaries remain multi-second. Per #2794, this is accepted as a
  bounded pause-profile model tradeoff when normalized checkpoint efficiency and
  write+checkpoint throughput remain strong.
- `branch_forced_off` and `branch_auto_c1_decline` preserve fail-closed behavior
  and do not admit the known M14 c1 regression shape.
- `c16` remains an explicit ceiling/alternative, not the default, because it has
  higher allocation/contention tradeoffs even when its write+checkpoint row is
  strong.

## Read/scan guardrails and scan recheck

Original final rows:

| Workload | Row | random_write | random_read | Δ read | full_scan | Δ full | prefix_scan | Δ prefix | RW checkpoint |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| settled point-read | `main_default_unconfigured` | 407,138 | 254,220 | +0.0% | | | | | 1.06s |
| settled point-read | `branch_default_auto` | 587,433 | 259,675 | +2.1% | | | | | 856.79ms |
| settled scan | `main_default_unconfigured` | 406,160 | | | 4,477,346 | +0.0% | 4,647,435 | +0.0% | 1.04s |
| settled scan | `branch_default_auto` | 599,736 | | | 4,267,533 | -4.7% | 4,292,895 | -7.6% | 817.72ms |
| mixed/debt read+scan | `main_default_unconfigured` | 345,161 | 233,041 | +0.0% | 4,360,130 | +0.0% | 4,696,435 | +0.0% | |
| mixed/debt read+scan | `branch_default_auto` | 560,678 | 241,249 | +3.5% | 4,756,140 | +9.1% | 4,602,588 | -2.0% | |

The original settled-scan row showed a material-looking scan regression and was
not accepted as-is. Three focused same-host 10MM settled-scan rechecks were run:

| Recheck root | Order | main full | branch full | Δ full | main prefix | branch prefix | Δ prefix |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `<remote-profile-root>/2788_final_gate_20260617_034229/scan_recheck_10mm` | main first | 4,512,678 | 4,443,442 | -1.5% | 4,674,164 | 4,588,975 | -1.8% |
| `<remote-profile-root>/2788_final_gate_20260617_034229/scan_recheck2_10mm` | main first | 4,539,130 | 4,408,511 | -2.9% | 4,690,126 | 4,635,810 | -1.2% |
| `<remote-profile-root>/2788_final_gate_20260617_034229/scan_recheck3_reversed_10mm` | branch first | 4,485,289 | 4,649,065 | +3.7% | 4,464,942 | 4,529,067 | +1.4% |

Recheck median branch-default delta: full-scan -1.5%, prefix-scan -1.2%. The
reversed-order recheck was positive for both scan metrics, so the original
`-7.6%` prefix-scan row is treated as a noisy outlier rather than a reproducible
material regression. The report intentionally keeps both the original row and
rechecks visible.

Point-read and mixed/debt read+scan guardrails are flat to faster for the
default-auto row in the final gate.

## Required counters

Write/checkpoint counters from row-local JSON stats:

| Row | old-leaf B/op | leaf merges/op | append frames/op | span used ops | close/checkpoint fallback ops | backlog runs | backlog extra ops | cache stores | write stores/skips | index.db | leaf_vlog |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `main_default_unconfigured` | 1609.048482 | 0.356899 | 0.357579 | 0 | 0 | 0 | 0 | 13,843,600 | 0/0 | 605 MiB | 3.7 GiB |
| `branch_default_auto` | 1367.772644 | 0.313250 | 0.313807 | 29,686,164 | 218,612 | 2 | 986,896 | 1,445,504 | 1,445,504/10,698,674 | 443 MiB | 3.3 GiB |
| `branch_forced_off` | 1609.050863 | 0.356898 | 0.357579 | 0 | 0 | 0 | 0 | 13,843,599 | 0/0 | 605 MiB | 3.7 GiB |
| `branch_auto_c1_decline` | 1609.047905 | 0.356899 | 0.357579 | 0 | 0 | 0 | 0 | 13,843,617 | 0/0 | 605 MiB | 3.7 GiB |
| `branch_explicit_c4_adaptive` | 1367.801864 | 0.313251 | 0.313808 | 29,686,163 | 218,612 | 2 | 986,896 | 1,448,365 | 1,448,365/10,695,840 | 433 MiB | 3.3 GiB |
| `branch_explicit_c4_immediate` | 1403.267800 | 0.320052 | 0.320612 | 29,688,493 | 218,612 | 2 | 986,896 | 12,410,958 | 0/0 | 442 MiB | 3.3 GiB |
| `branch_explicit_c16_immediate` | 1403.690578 | 0.319872 | 0.320435 | 29,196,523 | 710,839 | 2 | 986,896 | 12,415,627 | 0/0 | 459 MiB | 3.3 GiB |
| `branch_explicit_c4_cache_disabled` | 1367.752190 | 0.313250 | 0.313807 | 29,686,165 | 218,612 | 2 | 986,896 | 0 | 0/0 | 427 MiB | 3.3 GiB |

Default auto uses adaptive write admission: about 1.45M write-side cache stores
and 10.70M write skips in the write/checkpoint row, versus 12.41M stores for the
explicit c4 immediate row. This preserves the M4 cache-churn rationale while
keeping read-miss admission intact in read guardrails.

Raw route support counters are exported in addition to the aggregate
`treedb.flush_apply.span_native.*` counters. Backend raw apply rows use
`treedb.raw.span_native.route.<route>.*` for point puts, point deletes, mixed
point batches, range-delete batches, mixed range-delete batches, empty batches,
and close/checkpoint drains. Default-auto admitted point rows can report
`used_ops_total`; unsupported or rollback rows report named fallback reasons
such as `range_delete_barrier`, `below_threshold`, `disabled`,
`admission_policy_decline`, and `close_or_checkpoint`. Public command-WAL
`Update` and `UpdateSync` rejections return before backend apply, so they are
reported separately under
`treedb.raw.span_native.public.route.<route>.fallback.reason.command_wal_barrier.*`.

## Focused validation

Final-root focused test logs are under
`<remote-profile-root>/2788_final_gate_20260617_034229/final_10mm/focused_tests/`.

Commands:

```text
git diff --check
go test ./TreeDB/db ./TreeDB/caching ./TreeDB/zipper ./cmd/unified_bench -count=1
go test ./TreeDB -run 'TestReopenVerify_(WALOn_Checkpoint|WALOn_WriteSync|WALOn_Checkpoint_LeafPagesInValueLog|LeafPageLogGroupedFrameCRCIntegrityModes)$' -count=1
go test ./TreeDB/db -run 'Test(NormalizeFlushAdmission|FlushAdmissionStats|ParseFlushAdmission|LeafPageReadCache(AdaptiveWriteAdmission|ConcurrentSetAssociativeAccess|WriteAdmissionImmediate))' -count=1
go test -race ./TreeDB/db -run 'Test(NormalizeFlushAdmission|LeafPageReadCache(AdaptiveWriteAdmissionSkipsWhenSlotLockContended|ConcurrentSetAssociativeAccess))' -count=1
```

All focused commands passed.

Local validation before this report:

```text
git diff --check
go test ./TreeDB/db ./TreeDB/caching ./TreeDB/zipper ./cmd/unified_bench -count=1
```

## Rollback and caveats

Rollback:

- API: set `Options.FlushAdmissionPolicy = FlushAdmissionPolicyOff`.
- unified-bench/CLI: pass `-treedb-flush-admission-policy=off`.
- The rollback row force-disables span-native apply, backlog coalescing, and
  flush-apply concurrency.

Caveats:

- The default auto policy requires the low-concurrency guardrail to pass and
  declines c1-shaped configurations.
- WAL-off unsafe durability is declined by default auto. Explicit policy remains
  available for experiments that intentionally choose unsafe durability.
- Checkpoint pauses remain visible multi-second events. They are accepted here
  because normalized checkpoint efficiency and write+checkpoint throughput pass
  the #2794 model gate; this is a pause-profile tradeoff, not a claim that
  checkpoint debt disappeared.
- The original final settled-scan row is retained because it looked material.
  Focused rechecks did not reproduce a material scan regression.
- Cache-disabled remains diagnostic only and is not a default candidate.

## Handoff

M5 gives the parent tracker a default-on decision with rollback. Future work can
still revisit the policy if production workloads show scan-tail sensitivity,
checkpoint pause sensitivity, or allocation/contention issues outside this gate.
