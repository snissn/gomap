# Comparison consumer evidence contract

This provisional harness consumes client cleanup receipts from
`snissn/go-ycsb` PR #7, merged commit
`3c8148495b8d4490cf0ec30e9eb92d14a7a71bac`. Its validation status describes
this consumer's evidence checks, not full #5335 matrix qualification. Server
witness production and lifecycle orchestration remain integration obligations.
The consumer never fabricates those witnesses from command flags/startup text.

Each invocation uses a fresh `OUT_DIR`. `manifest.json` records campaign identity,
selected targets, formats, profile, record/operation/thread counts, repeats and
client build binding. `CLIENT_BUILD_MANIFEST` names an independently retained
JSON build record with `schema_version: 1`, `producer_commit` (the merge above),
`source_commit`, `source_patch_sha256` (SHA256 of `git diff --binary HEAD`),
`exe_sha256`, nonempty `build_id`, `toolchain`, and
`compatibility_configuration`. Disclose the wire-minor-v1 overlay here; do not
silently change client builds between arms. The launcher verifies source ancestry,
patch and ELF hashes and rejects untracked source inputs. It retains the patch,
build record and `go version -m` output. A hash binding does not itself approve
an overlay or replace the producer's tests/review.

For each target and phase `load`/`run_N`, outputs are `.out`, `.err`,
`.close.json` and `.launch.json`. The producer creates each close receipt
exclusively; launch supplies fresh phase/build/profile IDs and retains actual
child PID, Linux `/proc` start ticks, executable SHA256 and observed absence of
`GOMAXPROCS`. It records checked exit status, start/end UTC, stdout and receipt
hashes. Native receipts require original successful dial, FlushAll and connection
close with `applicable=true`, `flush_enabled=true`, integer `ack=1` (visible).
All backends require successful command, DB Close and workload Close. The
producer's command result intentionally has zero timestamps; DB/native/workload
close intervals are validated separately. Native TOTAL remains unmodified and
excludes Close.

Every selected target needs the following additional files. They are observations
from the actual selected server/owner, not launcher defaults. Current production
servers do not emit this schema; integration must supply trustworthy observation
and orchestration before a cell can validate.

* `server.launch.json`: observed `process` object and absolute `data_dir`.
* `server.exit.json`: same `process` and checked `exit_code: 0`, emitted only after
  the task-owned service stops. Cleanup failure prevents valid terminal status.
* `runtime.json`: `schema_version: 1`, `campaign_id`, `target`, `profile`,
  `format`, `process`, `data_dir`, nonempty `owner_id`, ordered `phase_ids`, actual
  integer `observed_gomaxprocs > 0`, actual `backend` (`cow` for TreeDB or
  `mongodb`) and nonempty `observation` identifying the retained instrumentation
  source. All identities/settings must match launch/manifest. Backend must come
  from the serving collection/owner path, not an option or helper name.
* `lifecycle.json`: the same identity/settings/owner/phase fields plus
  `server_exit_code: 0`, `checkpoint`, `server_close`, `reopen`, and
  `random_state_preservation` call results. Each call result uses producer
  `CloseResult` shape: `attempted`, `success`, `error`, `start_utc`, `end_utc`,
  `elapsed_ns`; all must describe actual successful calls. Include matching
  `preclose_state_sha256` and `reopened_state_sha256` of canonical typed snapshots,
  and `random_state_scope: "observed-state-preservation-only"`. Include actual
  `preclose_state_utc` and `reopened_state_utc` timestamps; checkpoint must follow
  settled clients, close follows checkpoint, reopen follows close, and snapshots
  must be inside their respective settled-client/close and reopen/preservation
  intervals. Capture preclose
  snapshot after timed client closes and before final server close, then reopen
  the same database. This checks preservation of observed random state, not every
  acknowledged random value. Separate deterministic expected-value acceptance
  and profile-specific crash recovery are still required by #5335.

A `process` object has positive integer `pid` and `start_ticks`, 64-character
lowercase `exe_sha256`, and boolean `gomaxprocs_unset: true`. Reopen observations
must identify the original owner/database they verify; reopening can create a
new process, but must not replace the original process identity in the linkage.
MongoDB uses an owned container, with actual container inspection/image metadata
retained. External MongoDB is refused until explicit owner/process integration
exists. The current launcher does not run independent reopen/snapshot procedures;
without their independently produced evidence the cell intentionally fails.

Run offline controls with:

```sh
bash -n scripts/ycsb_compare_mongodb_treedb.sh
python3 -m unittest discover -s scripts -p test_ycsb_compare.py -v
PARSE_ONLY=true OUT_DIR="$COPIED_FIXTURE" bash scripts/ycsb_compare_mongodb_treedb.sh
```

Parse only writes summaries and validation status; always use copies of retained
inputs. `ALLOW_YCSB_ERRORS` never waives validity checks. Missing/malformed/stale
receipts, row/count errors, wrong owner/backend/runtime, and failed lifecycle or
cleanup all exit nonzero. Original failures remain invalid.

The small real stdout fixtures in `testdata/ycsb_compare` were copied unchanged
from `/home/mikers/gomap-mongo-ycsb-20261029/`:

* `direct-ycsb-mongo-20261029-2052/command_wal_relaxed/treedb_mongo/{load,run_1}.out`
* `direct-ycsb-native-json-20261029-2115/command_wal_relaxed/treedb_native/load.out`
  (retained as `error-load.out`, 95,229 INSERT errors).

`native-close-failure.json` is the upstream merged producer's retained
`docs/close-receipt-evidence/native-close-failure.receipt.json`. Synthetic
observation/control objects in tests are explicitly offline fixtures; they do
not certify any product cell or stand in for the upstream original-return fault
injection test.
