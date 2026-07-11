# TreeDB Ironbird Attribution Contract

This document defines the TreeDB lifecycle counters and Ironbird sampling
boundaries needed to interpret low-fanout `plain-send` and `small-multisend`
benchmark rows. It is a reporting contract. It does not change TreeDB
durability, cleanup, value-log, or checkpoint behavior.

## Measurement Goal

Ironbird attribution must separate these classes before a TreeDB optimization
claim is made:

- command-WAL append and sync work;
- write publication and visibility work;
- checkpoint and frontier flush work;
- value-log generation, GC, and rewrite work;
- cache residency and eviction behavior;
- foreground backpressure, throttling, and queue debt;
- Go runtime allocation and GC pressure;
- host I/O wait, lock contention, and harness or non-ABCI time.

If a row cannot collect one of these classes, the result must carry an explicit
missing-field reason. Missing counters must not be silently attributed to
TreeDB, goleveldb, Cosmos, CometBFT, or Ironbird.

## Snapshot Boundaries

Downstream harnesses should capture TreeDB stats at these boundaries:

| Snapshot | Required meaning |
| --- | --- |
| `pre_load` | after chain setup and before the load generator starts |
| `load_start` | accepted TPS window start, or the closest timestamped proxy |
| `load_end` | accepted TPS window end, before dwell or artifact copy |
| `post_dwell` | after the configured dwell interval, with nodes kept alive if the row is testing background cleanup |

Each snapshot must include the same selected TreeDB stats keys when the backend
exposes them. Derived reports should use deltas between snapshots for monotonic
counters and raw values for gauges.

## Counter Semantics

Counter suffixes should follow these rules:

| Suffix | Semantics |
| --- | --- |
| `_total` | monotonic counter; compare by delta across snapshots |
| `_ns_total` | monotonic nanosecond counter; compare by delta and render as seconds or milliseconds |
| `_bytes_total` | monotonic byte counter; compare by delta |
| `_bytes` | gauge unless the key name also ends in `_total` |
| `_seconds` / `_ms` | gauge unless the key name also ends in `_total` |
| `_active`, `_pending`, `_queued`, `_resident` | gauge |

Compatibility exception: existing selected value-log raw I/O and codec accounting
keys are monotonic counters even though they predate the `_total` naming
convention. This exception covers `treedb.cache.vlog_writev.bytes`,
`treedb.cache.vlog_writev.syscalls`, `treedb.cache.vlog_writev.iovecs`,
`treedb.cache.vlog_writev.flushes`, `treedb.cache.vlog_write.bytes`,
`treedb.cache.vlog_write.syscalls`, `treedb.cache.vlog_write.calls`,
`treedb.cache.vlog_io.bytes`, `treedb.cache.vlog_io.syscalls`,
`treedb.cache.vlog_write_mode.raw_bytes.*`,
`treedb.cache.vlog_write_mode.stored_bytes.*`,
`treedb.cache.vlog_write_mode.frames.*`,
`treedb.cache.vlog_write_mode.bucket.raw_bytes.*`,
`treedb.cache.vlog_write_mode.bucket.stored_bytes.*`,
`treedb.cache.vlog_write_mode.bucket.frames.*`,
`treedb.cache.vlog_payload_kind.raw_bytes.*`,
`treedb.cache.vlog_payload_kind.stored_bytes.*`,
`treedb.cache.vlog_payload_kind.frames.*`,
`treedb.cache.vlog_payload_split.raw_bytes.*`,
`treedb.cache.vlog_payload_split.stored_bytes.*`,
`treedb.cache.vlog_outer_leaf_codec.raw_bytes.*`,
`treedb.cache.vlog_outer_leaf_codec.stored_bytes.*`,
`treedb.cache.vlog_auto.bytes.*`, `treedb.cache.vlog_auto.frames.*`,
`treedb.cache.vlog_auto.probe_attempts`,
`treedb.cache.vlog_auto.probe_successes`,
`treedb.cache.vlog_auto.hold_enters`, `treedb.cache.vlog_auto.hold_exits`,
`treedb.cache.vlog_auto.bypass_bytes`, and
`treedb.cache.vlog_auto.switches.*`. Ironbird must report these keys as
load-window deltas until TreeDB exposes replacement `_total` aliases. Derived
ratio, fraction, bytes-per, and per-byte timing fields remain gauges. New
selected counters should use the `_total` suffix instead of extending this
exception.

String-valued policy or reason keys are allowed only when they are bounded
tokens. Free-form diagnostic text must not be selected into benchmark result
JSON.

## Required TreeDB Counter Families

The exact implementation may include more detailed counters, but the selected
stats allowlist and Ironbird reports must preserve these families when present:

| Family | Required prefixes or keys | Why it matters |
| --- | --- | --- |
| command-WAL | `treedb.command_wal.*`, `treedb.applied_command_lsn` | separates command frame append, active bytes, covered bytes, and sync visibility from later checkpoint work |
| write publication | `treedb.cache.write_barrier.*`, `treedb.write_barrier.*`, `treedb.publish.*` | separates the write path from backend publication and root visibility |
| checkpoint/frontier | `treedb.cache.checkpoint.*`, `treedb.cache.flush_span_run.*`, `treedb.cache.flush_apply.*`, `treedb.flush_apply.*` | identifies commit-time and background checkpoint/frontier flush cost |
| queue and backpressure | `treedb.cache.queue_len`, `treedb.cache.queue_backlog_bytes`, `treedb.cache.backpressure.*`, `treedb.backpressure.*` | shows whether foreground writes wait on internal debt |
| value-log lifecycle | `treedb.cache.vlog_generation.*`, `treedb.vlog.lifecycle.*`, `treedb.vlog.gc.*`, `treedb.vlog.rewrite.*` | distinguishes live bytes, reclaimable bytes, scans, rewrites, and bytes reclaimed |
| value-log cache and buffers | `treedb.cache.vlog_*`, `treedb.vlog.*` selected cache/buffer prefixes | separates cleanup debt from resident cache or buffer memory |
| memory and runtime | `treedb.process.memory.*`, `treedb.process.memtable_residency.*`, `treedb.cache.memtable_residency.*`, `treedb.process.append_only.*`, `treedb.cache.append_only.*` | connects alloc profiles and RSS to TreeDB-owned residency |
| cleanup decisions | `treedb.cleanup.*`, `treedb.vlog.gc.skip.*`, `treedb.vlog.rewrite.skip.*` | explains whether cleanup was scheduled, skipped, blocked, or completed |
| contention and waits | `treedb.lock_wait.*`, `treedb.wait.*`, `treedb.cache.checkpoint.*wait*` | distinguishes CPU work from serialized waiting |

Required issue #3627 implementation should add missing selected prefixes only
when the repo can expose them safely with negligible overhead. If a family is
not available, the implementation must document the missing signal and the
lowest-risk follow-up.

## Current Selected TreeDB Names

The current TreeDB stats surface already emits the following low-overhead
families that Ironbird should preserve when present:

- command-WAL append, flush, sync, live coverage, and cleanup:
  `treedb.command_wal.*`;
- command-WAL physical write/file-sync/directory-sync calls and time:
  `treedb.command_wal.write.*`, `treedb.command_wal.file_sync.*`, and
  `treedb.command_wal.directory_sync.*`;
- cached command-WAL durability mode and checkpoint publication split:
  `treedb.cache.command_wal.*`;
- foreground write wait-for-checkpoint totals:
  `treedb.cache.write.wait_for_checkpoint.ns_total`,
  `treedb.cache.write.wait_for_checkpoint.count_total`;
- checkpoint/frontier stages, debt, shared drain, and wait telemetry:
  `treedb.cache.checkpoint.*`;
- flush apply, span-run, and backlog coalescing work:
  `treedb.cache.flush_apply.*`, `treedb.flush_apply.*`,
  `treedb.cache.flush_span_run.*`, `treedb.cache.flush_backlog_coalescing.*`;
- auto-checkpoint and queue pressure selected for delta-style artifacts:
  `treedb.cache.auto_checkpoint.count`,
  `treedb.cache.auto_checkpoint.last_reason`, `treedb.cache.queue_len`,
  `treedb.cache.queue_backlog_bytes`, `treedb.cache.backpressure_mode`,
  `treedb.cache.queue_laneid_misses`;
- value-log generation, GC, rewrite, mmap, read, template, buffer, and cache
  telemetry: `treedb.cache.vlog_generation.*`, `treedb.cache.vlog_mmap.*`,
  `treedb.cache.value_log.sync.*`, `treedb.cache.value_log.file_sync.*`,
  selected `treedb.cache.vlog_read.*_total`,
  selected `treedb.cache.vlog_template.*_total`,
  selected `treedb.cache.vlog_template_def_cache.{hits,misses}`,
  legacy monotonic write I/O keys such as `treedb.cache.vlog_writev.bytes`,
  other selected `treedb.cache.vlog_*` families, and selected backend
  `treedb.vlog.read.*_total` and
  `treedb.vlog.template_def_cache.{hits,misses}` families;
- process memory, memtable residency, append-only residency, and publish
  watermark telemetry: `treedb.process.memory.*`,
  `treedb.cache.memtable_residency.*`,
  `treedb.process.memtable_residency.*`, `treedb.cache.append_only.*`,
  `treedb.process.append_only.*`, `treedb.publish.watermark.*`.

Numeric gauges such as materialization lag, EWMA rates, template-cache ratios,
and last-event timestamps remain useful raw snapshot signals for Ironbird, but
they are intentionally not selected by `cmd/internal/treedbstats` when that
helper feeds delta-only benchmark artifacts.

## Ironbird Timeline Fields

Ironbird rows should emit timestamped phase spans so the final report can
quantify non-ABCI time instead of treating it as an unnamed residual:

- `row_start` and `row_end`;
- `chain_ready`;
- `stats_sampling_start` and `stats_sampling_end`;
- `profile_start` and `profile_end`;
- `load_generator_start` and `load_generator_end`;
- `first_successful_tx` and `last_successful_tx`;
- `accepted_load_window_start` and `accepted_load_window_end`;
- `artifact_copy_start` and `artifact_copy_end`;
- `dwell_start` and `dwell_end` when dwell is enabled.

If ABCI method start/end intervals are available at acceptable overhead, the
report should compute an ABCI busy union and overlap. If only per-method summed
durations are available, the non-ABCI residual must be labeled approximate.

## Required Report Tables

The final Ironbird report should include these tables for every accepted row:

| Table | Required columns |
| --- | --- |
| load-window accounting | wall seconds, ABCI observed sum, ABCI busy union when available, ABCI overlap, validator CPU seconds, validator core equivalent, load-generator wall seconds, approximate or exact non-ABCI seconds, residual formula, missing reasons |
| TreeDB lifecycle deltas | command-WAL bytes/syncs/time, checkpoint/frontier time and debt, value-log live/reclaimable/reclaimed bytes, cleanup scheduled/running/skipped/completed counts, cache residency, backpressure waits |
| dwell classification | load-end size, post-dwell size, live bytes, reclaimable bytes, bytes reclaimed, GC/rewrite counts, checkpoint debt, cleanup classification |

The report may conclude that a row remains under-instrumented, but it must name
the missing signal and link the follow-up issue before using the row as
optimization evidence.
