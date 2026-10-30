# Comparison consumer evidence contract

This provisional harness consumes client cleanup receipts from
`snissn/go-ycsb` PR #7, merged commit
`3c8148495b8d4490cf0ec30e9eb92d14a7a71bac`. Its validation status describes
this consumer's evidence checks, not full #5335 matrix qualification. Server
observation and lifecycle integration is provisional and needs exact-head review.
The consumer never fabricates those witnesses from command flags/startup text.

All retained JSON readers reject duplicate object keys, NaN/Infinity, and numeric
overflow to infinity. Supporting copies are compared with JSON types preserved;
`true`, `1`, and `1.0` are distinct. Each Process, Observation, and Result copy
is also checked independently before comparison. Schema versions, PIDs, process
start ticks, exits, inode/device IDs, runtime counts, and elapsed nanoseconds use
exact integer types; owner stats map strings to strings. UTC chronology uses
integer nanoseconds (up to nine fractional digits), and copied timestamps must
match their original strings exactly. The Result wall-time versus monotonic
elapsed-time consistency check deliberately retains a 2 ms tolerance; interval
ordering and owner-lifetime bounds have no such tolerance.
The timeout/deadline `*_seconds` fields are the sole numeric comparison
exception: each side must be a positive finite integer or float (never a bool
or string), and equal numeric durations such as shell `600` and argparse
`600.0` match. This exception does not apply to Result elapsed nanoseconds,
identities, counts, schemas, exits, or whole supporting-copy comparisons.

Each invocation uses a fresh `OUT_DIR`. `SERVER_BUILD_MANIFEST` is mandatory
and names a schema-v1 JSON object with `servers` mapping each selected target to
`schema_version: 1`, `version` (for example `baseline`/`candidate`), exact
`source_commit` (40 lowercase hexadecimal characters), `exe_sha256`, and
`expected_owner` (`legacy` or `cow` for TreeDB; `mongodb` for the reference),
absolute `source_dir`, and `source_patch_sha256`. The latter is SHA256 of
`git diff --binary HEAD` in the actual compiled server tree; untracked inputs
are refused. Retain `server-source.patch` and `server-buildinfo.txt` for each
arm. Historical baselines require an explicitly disclosed instrumentation
overlay compiled against their own exact source; this branch does not claim
that any historical overlay has been compiled or accepted.
Retain this independently accepted build binding in `server-build.json`. The
supplied server executable must match its selected version's ELF hash. Runtime
and lifecycle witnesses must repeat that version/source and actual process ELF;
the actual serving backend must match `expected_owner`. Historical baselines
are not required or permitted to pretend they activated COW. A version label
alone does not establish source or owner identity.

`manifest.json` records campaign identity,
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
close with `applicable=true`, `flush_enabled=true`, integer `ack=1` (requested visible policy). This is the producer
configuration/outgoing ACK, not a decoded server-returned ACK witness. The
actual original FlushAll return must independently succeed.
All backends require successful command, DB Close and workload Close. The
producer's command result intentionally has zero timestamps; DB/native/workload
close intervals are validated separately. Native TOTAL remains unmodified and
excludes Close.

Every selected target needs the following additional files. They are observations
from the actual selected server/owner, not launcher defaults. The opt-in
`YCSB_SERVER_RECEIPT_DIR` observer is absent from request hot paths. It records
`CaptureNativeSemanticState` success plus actual COW cache/sequence/profile
stats, or the actual unsupported result for legacy, procfs identity, canonical
data device/inode, and actual runtime GOMAXPROCS. It wraps the original owning
Close exactly once and joins evidence I/O failures after that Close still runs.
No option or startup label selects the observed backend.

Before launching any measured client or fixture/snapshot helper, startup waits
boundedly for the actual `owner.open.json`, in addition to TCP readiness. It
checks that observation against the selected backend/profile, launch PID/start/
ELF/environment, current procfs identity, and actual data directory device/inode.
Only then is `<prefix>.ready.json` retained and startup returns. Missing or
mismatched owner evidence writes a readiness failure and invokes owned cleanup;
TCP readiness alone cannot release clients.
The Go writer exclusively creates the receipt before writing its JSON body.
Readiness therefore retries missing or incomplete JSON within the same bounded
live process identity, rather than treating file existence as complete
publication. Persistent malformed/incomplete JSON times out; complete invalid
schema or mismatched identity fails before client release.

* `server.launch.json`: observed `process` object and absolute `data_dir`.
* `server.exit.json`: same `process` and checked `exit_code: 0`, emitted only after
  the task-owned service stops. Cleanup uses Linux pidfds, pins the process
  before checking its observed start ticks/ELF, and revalidates before TERM and
  any KILL. An identity mismatch refuses signaling and invalidates cleanup.
  Missing pidfd support fails closed. Docker cleanup uses the full immutable
  container ID, rather than a potentially reused name. Cleanup failure prevents
  valid terminal status.
* `runtime.json`: `schema_version: 1`, `campaign_id`, `target`, `profile`,
  `format`, `version`, `source_commit`, `process`, `data_dir`, nonempty `owner_id`, ordered `phase_ids`, actual
  integer `observed_gomaxprocs > 0`, actual `backend` (`legacy`, `cow`, or
  `mongodb`, matched to the version-bound expected owner) and nonempty `observation` identifying the retained instrumentation
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
exists. Integration must retain a separate independently verifiable reopened process
and owner identity and canonical typed random snapshots with structural validation;
matching supplied digest strings do not establish those missing observations.
`manifest.lifecycle_contract: 2` is mandatory, along with `fixture` binary/source
SHA256 binding and retained `fixture-source.go`. `FIXTURE_BIN` names the compiled
reviewed fixture plus snapshot adapter. Every helper child has an observed
PID/start/ELF, unset GOMAXPROCS, checked exit, original client-close receipt and
receipt-bound snapshot/expected-file hashes. The fixture is integrated from
reviewed source 17a0d3d03829294fd984e9fdd3fe95fde8689ebc; its additional snapshot
mode reads the actual random `ycsb.usertable` through the selected public driver.

Required raw service observations live in `{server,reopened,fixture-write,
fixture-reopened}.evidence/{owner.open,owner.before-close,owner.close}.json`.
Each prefix has a separate `.launch.json` and checked `.exit.json`. Random and
deterministic reopen require distinct owner tokens and observed process
identities, same ELF and same canonical database device/inode. The deterministic
database is fresh and separate from the random database. Aggregates are checked
against original owning Close and explicit checkpoint receipts. The aggregate
`reopen` interval bounds the original Close completion and newly observed live
owner; it is an observation interval, not a new storage timing measurement.

The consumer reads `snapshot_{loaded,preclose,reopened}.json` and validates the
exact original hashed key set (go-ycsb FNV1a64, prefix `user`, start 0, padding 1),
exact ten field names, canonical JSON base64 strings or BSON subtype-0 binaries,
and 100 decoded bytes per field. It compares canonical typed preclose/reopened
values directly. Matching digest strings cannot replace the raw snapshots.
The separate deterministic fixture retains all ACK/readback outcomes and exact
load/updated/reopened snapshots and is validated independently.

The launcher performs these scans outside TOTAL, calls the real native
Checkpoint after settled clients, closes the original service, independently
reopens it, scans again, then runs the deterministic fixture in another fresh
database with its own independent reopen. The gateway has no public Mongo
checkpoint API. For this task only, the additional explicit environment opt-in
`YCSB_OWNER_CHECKPOINT=1` starts a private loopback evidence control, requiring
`YCSB_SERVER_RECEIPT_DIR` and the actual live observer. The ordinary gateway
without that opt-in creates no control and executes no extra storage call. The
control admits one request and invokes the actual owning
`standalone.Collections.FlushAll`, followed on success by
`standalone.Backend.Checkpoint`. A flush failure leaves backend checkpoint
unattempted. Actual errors and separate call timings are retained; failed calls
invalidate the cell. These are explicit preclose calls, separate from shutdown.
The endpoint is closed and any accepted call settles before original owner Close.

For Mongo gateway preclose snapshots and deterministic writes, retain
`{server,fixture-write}.evidence/owner.checkpoint-{control,request}.json` and
`owner.checkpoint.json`. The schema-v1 `explicit_live_owner_checkpoint` receipt
contains the actual live `owner` observation, the accepted `request`, separate
`collections_flush_all` and `backend_checkpoint` results, and overall `result`.
The request binds a fresh random request ID, observed client PID/start/ELF/env,
actual owner token, source/fixture IDs, and the completed snapshot file SHA256.
The supervised fixture embeds that exact raw server receipt as `owner_checkpoint`
and binds its overall result as `checkpoint`; the consumer checks both retained
copies, original request, complete live-owner lifetime, flush-before-checkpoint
order, helper process/source/snapshot identities, and settled measured phases.
A request label or shutdown FlushAll alone cannot qualify a checkpoint. The
backend call has no context cancellation API, so the existing bounded external
owner supervisor remains responsible for a hung owner; no success is fabricated.

Native deterministic write now retains a strict timed `checkpoint` Result around
its single original public `Checkpoint(ctx)` call. The actual checked return
sets success/error, and attempted/succeeded flags derive from that same Result.
The consumer requires it within the helper's actual interval. Flags-only retained
receipts remain historical and cannot acquire missing Result evidence by
relabeling. Client elapsed time is not a server-internal checkpoint duration.

The separate [public process-crash control](ycsb_crash_recovery.md) has offline
packet/CLI/custody controls for ordinary ACK and profile-specific loss guarantees.
No live product crash run was performed. Automatic checkpoint exclusion and
actual WAL replay remain unproven; this fixture does not certify power loss.
Owned reference Mongo lifecycle/owner evidence, baseline overlay compilation/
review, full orchestration review and the frozen matrix remain separate gates.
No product campaign has run as part of this implementation.

Real MongoDB 8 reference lifecycle support remains outstanding. Execution with
`TARGETS=mongodb` (also in mixed lists) is refused before output creation,
builds, helper calls or service/client launch in every `MONGODB_MODE`. Retained
`PARSE_ONLY` remains driven by its existing manifest and starts no service.
Execution `TARGETS` must be a single line: LF/CR, unknown targets and duplicate
targets are refused before side effects, so Bash cannot silently drop later
lines containing an unsupported or invalid target.

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

The run parser requires both READ and UPDATE rows with positive actual counts,
and requires their exact sum and TOTAL to equal `operationcount`. It accepts the
observed random split rather than inventing exact 95/5 counts. Missing/zero rows,
malformed rows, operation errors, duplicate rows and count mismatches are invalid.

Bounded implementation controls (not product campaign evidence) are retained at
`/home/mikers/gomap-5335-close-consumer-build-20261029/owner-control-1793319825466985732/`.
Its report binds source `c396a20f125ffefed7b4c5124d2076572dd36f7f` plus retained
patch SHA256 `795ee8bb65547e7e80da20d7d04d04daa07822a15d5d651859fa675e26cba535`.
Native COW/JSON, the same current source's explicit legacy/JSON route, and gateway
COW/BSON each passed 64-record deterministic write/checkpoint-where-supported,
original Close/checked exit, separate process/owner reopen, exact-value verify
and original Close again. This is not historical-baseline acceptance. The
native BSON attempt at sibling `owner-control-1793319781025408373` failed with
`nativewire: remote error code 3: unsupported feature` on this base; its original
close/exit was retained and no successful product cell was declared. Earlier
invalid startup-observation/adapter attempts remain in sibling directories.
The full random-snapshot shell pipeline has offline checks and requires an
independent exact-head review plus supported dependent-feature integration;
these tiny fixture controls do not claim that pipeline or matrix complete.

Each child is bounded by `PHASE_TIMEOUT_SECONDS` (default 600) or
`HELPER_TIMEOUT_SECONDS` (default 180). `CHILD_CLEANUP_TIMEOUT_SECONDS` defaults
to 10 per TERM/KILL/reap step. The shell passes an explicit outer wall-work
budget of child timeout + three cleanup windows + 10 seconds to each Python
phase/helper entry point, including assembly. A one-shot SIGALRM interrupts
outer Python work through the same cleanup path. Once cleanup starts, that
alarm is disabled so it cannot strand a child between TERM and KILL; cleanup
has its own finite windows. All five budgets are retained in
`manifest.deadlines`; actual child, cleanup and outer budgets are recorded and
checked on successful phase/helper launch receipts. All must be finite/positive.

A direct Popen child is pinned with a pidfd before any observation or wait.
Timeout/interruption cleans precisely that descriptor and reaps that child,
including when its first procfs observation fails or it execs another image.
External service cleanup retains the separate start-ticks/ELF revalidation rule.
`<stem>.failure.json` retains the timeout/error, original observed identity when
available, pidfd ownership and checked cleanup/reap result. It never replaces or
invents `<stem>.close.json`. A timeout remains invalid even if TERM yields exit
zero; any retained launcher/helper failure invalidates the target on reparse.

Validation requires the entire measured load/run sequence to be nonoverlapping
and ordered, including across selected targets. Every measured start/end and
helper interval must lie between its actual owner's open and before-close
observations. Loaded scans lie after load and before the first run; all runs
settle before the preclose scan. Aggregate snapshot/reopen timestamps must match
raw calls/owner observations, and state comparison follows the completed
reopened scan. The deterministic owner starts after the random reopened owner
closes; all four service lifetimes have distinct process and owner identities.

Startup cancellation is queued across Popen return and pidfd acquisition, inside
the cleanup guard. If pin acquisition fails, the launcher verifies its direct
child relationship using `waitid(WNOWAIT)` before any numeric-PID signal; the
unreaped child retains its PID until this launcher performs its bounded wait.
The failure receipt truthfully records `pidfd_pinned: false` and this fallback
method. Failed spawn records no PID and does not claim a child was reaped.
Cleanup blocks INT, TERM and ALRM until bounded TERM/KILL/reap and failure-receipt
writing finish, then restores the previous signal mask and delivers pending
cancellation. Known pending signals are retained in the original failure
receipt; a later cancellation does not overwrite the original failure reason.
The child inherits its normal signal mask during startup.

Read-only revalidation of the retained d189 tiny controls under the strict parser
preserves their exact snapshot, actual checkpoint call, and original Close
function evidence. Their gateway deterministic write/verify and random reopened
helper launches preceded the raw owner.open observation, so those three added
full launch-lifetime checks are rejected. They are not full orchestration
qualification. The bounded owner-readiness gate is covered by offline delayed,
missing, and stale-owner controls; no product/service rerun accompanies this
parser/orchestration repair.

The deadline/publication regression includes an actual successful phase launcher
CLI and live-helper CLI packet reaching the final shell parse-only validator.
Its owner/storage records and Python client/helper stubs are explicitly synthetic;
no product service is launched. Additional shell controls publish owner evidence
with actual create-before-write semantics, including delayed partial JSON,
permanently malformed JSON, and complete invalid schema, and verify owned cleanup.

The separate provisional [MongoDB reference lifecycle](ycsb_mongodb_reference.md)
defines its own image/process/public-command/observed-exit contract. It does not
borrow TreeDB owner, COW, profile or Close-return semantics. The maintained shell
continues refusing `mongodb` execution until reference integration and authorized
support controls are reviewed; parse-only consumption is independent.
