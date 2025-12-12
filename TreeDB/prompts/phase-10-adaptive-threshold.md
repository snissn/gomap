You are Codex CLI in the TreeDB repo root. Follow `AGENTS.md` and implement only **Phase 10 — Adaptive Inline Threshold Controller** in `internal/adaptive`, wired into the Phase‑7 commit pipeline.

Idempotent execution contract:
1. Validate prerequisites:
   - Commit pipeline exposes per‑commit telemetry hooks (Phase 7).
   - Root `Stats()` infrastructure exists.
   - Inline vs slab decision already uses a threshold value.
   If missing, explain and stop without changes.
2. Detect existing adaptive controller/telemetry in `internal/adaptive` and compare to checklist below. Summarize done vs remaining.
3. If checklist is satisfied, re‑run adaptive tests and stop.
4. Otherwise implement missing controller features and Phase‑10 tests only.

Implementation tasks (per `specs/spec.md` 5.6):
- Config struct:
  - enable flag, evaluation interval `K`, weights, `step`, `alpha`, hard bounds.
- Telemetry (EWMA) maintained at commit boundaries:
  - `leaf_fill_avg`, `split_rate`, `index_write_bytes`,
  - `slab_write_bytes`, `slab_dead_ratio`, `compaction_io_bps`.
- Pressure functions and bounded‑step update every `K` commits.
- Threshold latch per commit start; enforce hard min/max and “always out‑of‑line” for `len(value) > InlineHardMax`.
- Export Stats keys listed in spec when enabled.

Definition of done (Phase 10 checklist):
- Controller evaluates only every `K` commits, moves by ≤`step`, respects bounds.
- Commit uses one latched threshold value.
- Stats expose required telemetry keys.
- All Phase‑10 tests pass.

Tests to add (per `specs/test-spec.md` 1.6 and 2.5):
- Latch semantics per commit.
- Bounded‑step + hard‑bound enforcement.
- Low‑overhead evaluation frequency counter.
- Mixed workload convergence smoke.

Verification:
- Run `go test ./...` (adaptive + commit pipeline).

Stop after adaptive controller only.

Phase completion marker:
- Marker file: `@PHASE_10_COMPLETE` in the repo root.
- If during this run Phase 10 was already complete **or** you made only trivial adaptive tweaks (minor bugfixes, test nits), then create/leave the marker (`touch @PHASE_10_COMPLETE`).
- If you implemented substantial telemetry/controller logic or added major tests/files, **do not** create the marker; if it already exists, delete it.
