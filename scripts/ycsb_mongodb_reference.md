# MongoDB reference lifecycle

The maintained explicit `ycsb_mongodb_reference.py` route completed the
[final twelve-arm comparison](../docs/benchmarks/ycsb_mongodb_gomap_final_2026-10-30.md)
against the repaired GoMap Mongo BSON gateway. This route retains public
correctness/readback, checked client Close, public fsync, graceful shutdown,
fresh reopen and actual process/container/runtime bindings. Earlier failed
preparations and readiness/validation attempts remain preserved; they are not
retroactively qualified. The general TreeDB shell still refuses direct execution
selecting `mongodb`; the reference uses its separate explicit launcher. Synthetic
`PARSE_ONLY=true` coverage is parser evidence, not a product measurement.

`ycsb_mongodb_reference.py` defines a separate singleton manifest kind
`mongodb-reference-checkpoint-close-reopen`, lifecycle contract integer 1 and
BSON format. New prepared manifests bind `profile` and `acknowledgement_class`
to the explicit measurement request:

| Measurement write concern | Profile and acknowledgement class |
| --- | --- |
| `{"w":1,"journal_requested":null}` (j omitted) | `mongodb_w1_journal_unspecified` |
| `{"w":1,"journal_requested":true}` (explicit j:true) | `mongodb_w1_journaled` |

`journal_requested=false` is unsupported by the reference manifest/preparer.
Journal unspecified does not mean j:false. These classes do not establish
TreeDB durable-profile equivalence, uncheckpointed WAL replay, crash or
power-loss certification.

New manifests separately retain `fixture_profile="mongodb_w1_default"` for the
frozen fixture ELF's compatibility contract. The public fixture still uses the
existing BSON driver with explicit W1; that compatibility label does not select
the measurement request. Its Disconnect is checked and does not invoke a
checkpoint. Legacy manifests without `acknowledgement_class` remain accepted
only with `profile="mongodb_w1_default"` and journal unspecified. The timed
client's original checked DB/workload Close receipts stay separate: each phase
must retain successful command/DB Close/workload Close results, matching
campaign, phase, build and declared measurement profile, and settled launcher
times. Original TOTAL still excludes Close.

Preparation must retain an actual source/binary-bound client build manifest,
client patch/buildinfo, fixture and public-control source copies and ELF hashes,
this launcher's source copy/hash, and a `mongodb_image_build` manifest. The latter
requires an immutable local image ID and repository digest, exact observed
version and `git_version` (buildInfo.gitVersion), separately reviewed public
`source_commit`, Linux/amd64, actual mongod and Docker executable hashes,
and engine/engine-commit/containerd/runc/storage-driver/cgroup/default-runtime
versions plus inherited immutable image labels. OCI image version labels are
retained as image metadata; they are not the MongoDB buildInfo version.
Docker Engine/containerd/runc records are version metadata only. They do not
attest daemon/runtime executable ELF custody; `runtime_elf_attested` is false.
There is no default inferred mongod ELF hash, tag resolution, image
pull, external-address mode, credentials, or fallback to TREEDB_MONGO_ADDR.
Only explicit `127.0.0.1:port` is accepted as the public address.

The execution/custody implementation is Linux-specific: it observes `/proc`
process identities and ELF hashes and uses pidfd child custody. The reviewed
reference image contract is Linux/amd64. The client runtime observer requires
an exact supported client hash/symbol binding and ELF64 little-endian x86_64
`ET_EXEC`; an arbitrary rebuilt or PIE client is insufficient. This is not a
portable macOS/Windows launcher or support for arbitrary executable formats,
architectures or MongoDB versions. Retaining runtime version strings does not
expand that scope.

`run OUT --docker /absolute/docker` expects a prepared manifest and source/build
copies in OUT and an empty OUT/mongodb directory. It is a separate explicit
entry point, not enabled by the guarded shell. It checks the actual supplied
tool hashes, local image inventory, runtime and raw CLI outputs. All Docker
helper children and public driver children use the existing finite pidfd child
custody and remove GOMAXPROCS from their actual environments. Per-child phase,
helper, cleanup and outer bounds are explicit. A total monotonic run budget is
checked before each action; it does not replace the finite cleanup budgets.

Four independently identified lifetimes cover random original/reopened and
fresh deterministic write/reopened services. Each uses an explicit same-UID/GID
direct `/usr/bin/mongod` entry point, two task-owned bind directories, loopback
port publication, no restart and no automatic removal. Evidence binds the full
immutable container ID, exact image/config/runtime, host PID/start ticks/ELF,
PID/mount/network namespaces and actual mounted DB device/inode. Readiness
requires both TCP and successful public hello/buildInfo/serverStatus/
getCmdLineOpts/getParameter observations matching that actual container/process. The same
public identity is checked before every measured/helper operation and before
close. Reopen uses the same image, binary and mounted directory with independent
container, process and server-generated topologyVersion processId.

The read-only getParameter command requests actual `journalCommitInterval` and
`syncdelay`, retaining canonical BSON numeric types. Both before/after replies,
every subsequent control and all four lifetimes must agree. A prepared campaign
must also match its retained support values. Missing fields, nonfinite values,
type substitution or settings changes fail closed; no default value is inserted.
MongoDB documents that journaling is always enabled from 6.1 onward, with the
disable option removed; that is a version/source contract, not an observed
per-write journal ACK. See [journaling](https://www.mongodb.com/docs/manual/core/journaling/)
and [runtime parameters](https://www.mongodb.com/docs/manual/reference/parameters/).
Ordinary w:1 with j absent remains distinct from j:true.

## Opt-in common load and public journal sentinel

A reference preparation request may include `common_load`. The maintained
TreeDB shell consumes the same policy through an explicit `COMMON_LOAD_POLICY`
file and records `MONGODB_JOURNAL_REQUEST` for the BSON gateway measurement
request. This opt-in path is scoped to the MongoDB-binding BSON command route;
the recorder requires the `mongodb` binding and an exact URI. It does not supply
a generic native-client or mixed-target common-load route, and does not lift
the ordinary shell's refusal to execute `mongodb`.

The policy requires common load concern `{"w":1,"journal_requested":null}` and
hash-bound, reviewed control A artifacts scoped to
`bson-binary-and-pointer-bearing-prefix`. Its explicit `comparison_class` is
either `same-request-jtrue` or `durable-ack-differing-requests`. Both require
MongoDB measurement j:true; the former also requires GoMap measurement j:true.
The latter requires GoMap measurement j omitted and separately hash-bound,
reviewed control B scoped to `public-bson-command-wal-ack-crash-replay`.
The GoMap policy requires `command_wal_durable`. These names and review gates
describe scoped requests/controls; they do not certify equivalent durability.

With this policy, load uses `mongodb://ADDRESS/ycsb?w=1` with no journal URI
option. After the load client exits successfully following its original Close
calls, a public loaded-state snapshot/census must complete. Full retained Close
receipt and phase validation occurs during final qualification, rather than
before the sentinel. A separate raw public
OP_MSG insert then writes one marker document into a campaign-specific
`prefix_sentinel_...` collection with `writeConcern:{w:1,j:true}`. It requires
numeric `ok:1`, exact integer `n:1`, and no `writeConcernError` or `writeErrors`.
The sentinel checks the original owner before and after the request and binds
the endpoint, process identity, campaign/manifest, raw request/response hashes
and matching wire request IDs. It uses the public command equivalent to the
reviewed official-driver sentinel; it is not a direct owner Close call.

Validation requires settled load → completed loaded census → completed public
sentinel → first run launch, with no overlap. Each phase's
`*.actual-command.json` retains its argv and request concern; validators check
the exact load/run binding and URI against the policy. Timed run URIs carry
`&journal=true` when requested; the differing-request GoMap class omits it.
Without `common_load`, the reference runner uses its selected measurement
concern for both load and run and does not insert this sentinel.

The sentinel and census remain outside timed run operations. They do not
replace original client Close validation, explicit later fsync checkpoints,
graceful server shutdown, random-state comparison or deterministic reopen
checks. No sentinel receipt alone proves crash replay, power-loss survival,
arbitrary BSON-route coverage or a complete performance matrix.

After all clients settle, a separate helper invokes the real public admin
`fsync:1, lock:false`, retaining canonical Extended JSON requests/replies,
actual monitored connection IDs and checked timed results. It is bound to the
preclose snapshot, active process and public identity. It does not invent a
MongoDB TreeDB-owner checkpoint API or relabel shutdown as this checkpoint.
Only MongoDB 8.2.7 source commit
`9fbea53313e85fbbf864a963ff17b0bd6dafa0be` currently has a reviewed source
contract: fsync calls storage-engine flushAllFiles, which selects the standalone
WiredTiger checkpoint path. Other versions fail closed pending source review.
That public mirror commit explicitly declares
`GitOrigin-RevId: 8f6785264a643d2eeb5e4046f7876a23e40e1a09` in its
[commit metadata](https://github.com/mongodb/mongo/commit/9fbea53313e85fbbf864a963ff17b0bd6dafa0be).
The supported pair requires public `source_commit=9fbea533...` and reported
`git_version=8f678526...`; they remain distinct identities, with no
caller-selected mapping. Direct public-mirror lookup of the origin SHA was
unavailable; no independently fetched origin tree is claimed.
The commit-bound chain is
[fsync.cpp](https://github.com/mongodb/mongo/blob/9fbea53313e85fbbf864a963ff17b0bd6dafa0be/src/mongo/db/commands/fsync.cpp#L125),
[storage_engine_impl.cpp](https://github.com/mongodb/mongo/blob/9fbea53313e85fbbf864a963ff17b0bd6dafa0be/src/mongo/db/storage/storage_engine_impl.cpp#L506),
and [wiredtiger_kv_engine.cpp](https://github.com/mongodb/mongo/blob/9fbea53313e85fbbf864a963ff17b0bd6dafa0be/src/mongo/db/storage/wiredtiger/wiredtiger_kv_engine.cpp#L961).

The fixed internal port is 27017. At that default port,
[serverStatus](https://github.com/mongodb/mongo/blob/9fbea53313e85fbbf864a963ff17b0bd6dafa0be/src/mongo/db/commands/server_status_command.cpp#L133)
uses [prettyHostName](https://github.com/mongodb/mongo/blob/9fbea53313e85fbbf864a963ff17b0bd6dafa0be/src/mongo/util/net/socket_utils.cpp#L307),
which returns the bare hostname. The validator requires that exact owned
hostname; an appended port or another hostname is rejected. Actual parsed
internal port27017, public connection endpoint, PID, ELF, namespaces and mounted
directory remain independently checked.

## Retained actual readiness failure

The one-use `833b9b2` correctness packet expected public mirror commit9fbea533
as buildInfo.gitVersion. Both actual replies returned version8.2.7 and origin
gitVersion8f678526; the old assertion rejected them before any workload. Its
serverStatus.host also used the source-defined bare hostname, while the old
assertion demanded a `:27017` suffix. The actual image/ELF remained pinned.
Forced exact-owned cleanup completed and independent absence/reap checks passed.
The full packet remains failed/incomplete and cannot qualify through corrected
identity assertions. The earlier959 Init failure also remains preserved.

The selected actual replies and upstream origin mapping are retained in
`testdata/ycsb_mongodb_reference/mongodb827-*`, with full original receipt paths
and hashes. Offline tests reproduce both assertion failures, accept the actual
identity shapes after correction, and reject wrong/missing origins, swapped
mirror/origin IDs, appended ports, other hosts/PIDs/ports and consistently
tampered full synthetic packet copies. This establishes parser behavior only;
successful public fsync, exact-value/random reopen and graceful close remain
required in a fresh independently reviewed packet.

Graceful shutdown retains the actual full-ID Docker SIGTERM action, Docker wait
exit status, terminal inspect and source-bound WiredTiger-close (4795901) then
mongod-complete (8423404) structured log records. The reported interval is the
observed process shutdown/wait interval. `close_return_observed` is always false:
this is not a measured public Go Close API return. Checked full-ID removal is
separate. Forced cleanup permanently retains a failure marker and cannot
produce qualifying close evidence; signals/removal require matching complete
ownership/configuration, never an unrelated container name or reusable PID.
Before its first Docker action, the runner exclusively publishes
`reference-incomplete.json`. It removes this marker only after full validation
and publication of a manifest-bound `reference-complete.json`; validation
requires completion and rejects an incomplete marker even if other receipts
look successful. Failed attempts keep the marker, including when later writes
fail with ENOSPC. These are process/error-path controls, not power-loss claims.

Failure publication and resource custody are independent. Emergency cleanup
does not use the artifact-writing launcher: bounded Docker CLI children retain
pidfd/direct-child custody, remove GOMAXPROCS, and collect at most 1 MiB of stdout
in memory. Exact full-ID ownership/image/configuration/mount checks precede
SIGKILL, checked wait, terminal inspect and removal. Unknown create results may
be recovered only through the exact task name plus the complete ownership
contract; signals and removal then use the recovered immutable full ID.
Emergency actions are always nonqualifying. Their records and the original,
publication and cleanup errors are published best effort after custody; if
storage remains unavailable they remain in memory and errors reach stderr.
Unresolved custody is an error, never a successful cleanup receipt.

The validator independently checks raw CLI output copies and arguments,
nonoverlapping settled-client chronology, public-command results and routing,
canonical typed random snapshots across reopen, exact deterministic expected
values/acknowledged outcomes, same-volume independent reopen, and checked
shutdown/removal. Any retained parent/helper failure marker invalidates the
attempt. Output is labeled `validated-reference-lifecycle-evidence`, separate
from TreeDB baseline evidence, with COW inapplicable, no observed Close return,
no WAL replay proof, no power-loss certification and no complete matrix claim.

## Offline preparation, still gated

`scripts/ycsb_compare_mongodb_treedb.sh --prepare-mongodb-reference REQUEST.json /absolute/fresh/OUT`
constructs an input packet without invoking Docker, a helper, client workload or
service. The explicit mode ignores execution environment selectors (including
TARGETS/PARSE_ONLY/BUILD); endpoint and binaries come only from the checked
request. It refuses missing/extra arguments and a relative output before
dispatch, removes GOMAXPROCS from the preparation controller, and never creates
the ordinary OUT_DIR. Ordinary `TARGETS=mongodb` execution remains refused.
The underlying `python3 scripts/ycsb_mongodb_reference_prepare.py` CLI is also
available. Preparation invokes only bounded Git inspection and supplied `go version -m`,
with GOMAXPROCS removed. Its CLI installs a scoped SIGTERM-to-exception handler
so cancellation reaches the metadata child's bounded cleanup/reap, and restores
the previous handler on exit. The explicit request has schema integer 1, kind
`mongodb_reference_preparation_request`, and these fields:

- `support_dir`: absolute complete independently retained **tiny** reference
  lifecycle packet (at most 1024 records/operations and one run). It must pass the
  full current validator, including raw buildInfo/settings/journal commands,
  image/runtime/config/process/mount identity, explicit fsync, checked observed
  close/removal and independent same-volume reopen. A free-form summary, image
  environment or version label is insufficient. A support packet cannot itself
  depend on nested preparation.
- `mongodb_addr`: explicit `127.0.0.1:port` corresponding to MONGODB_ADDR, exactly
  matching the support endpoint. It never reads TREEDB_MONGO_ADDR or falls back to
  any environment address. The normal shell guard still rejects reference runs.
- Absolute `client_source_dir`, `client_build_manifest`, `client_binary`,
  `fixture_binary`, `control_binary`, `docker_binary`, and `go_binary`. The source
  must descend from the merged producer receipt commit; HEAD, complete overlay,
  no untracked input, actual ELF and original build manifest are checked. The
  client and helper records/ELFs/source and Docker CLI must match support.
- Exact positive integer `recordcount`, `operationcount`, `threadcount`,
  `run_repeats`, and the existing explicit `deadlines` object. Task UID/GID are
  observed locally and must match support. No build or tool download is implicit.
- Optional `journal_requested`: omitted/null selects journal unspecified;
  `true` selects journaled. A journaled preparation retains the actual support
  packet's concern separately as `support_write_concern`; it does not relabel
  that support packet's writes as j:true. Optional `common_load` must pass the
  reviewed-control policy above. Optional `bounded_container` accepts only
  `true`, adding the runner's explicit memory/swap, CPU and log bounds.

Preparation copies only root and per-target regular evidence files, never
service DB directories or symlinks (at most 2048 files, 16 MiB per file and
128 MiB total; metadata child output is capped at 1 MiB). It revalidates the copied complete packet,
then binds its actual raw buildInfo, endpoint, process/container/public identities,
settings and journal parameters in `reference-support.json`. Runtime records stay
version metadata with `runtime_elf_attested=false`. Source dependencies
(including the maintained shell entry and, when opted in, the public sentinel), original
client patch/build manifest, helper sources and inspected executable buildinfo
are copied and hash-bound in `reference-preparation.json`; the final manifest
binds that receipt and exact fields. Preparation has an incomplete marker, and
both run preflight and final validation verify all prepared artifact copies.
Run preflight additionally matches actual current source dependencies.
Original absolute helper output argv are retained verbatim. A hash-bound original
support evidence root supplies only those expected output paths when validating
the relocated copy; evidence reads use the copy, and endpoint/identity/command
checks remain exact. The original support directory need not remain available.

Output status is `prepared-reference-inputs-not-activated` and
`activation_authorized=false`. Hashes/revalidation bind evidence but do not
authenticate fabricated files or authorize a campaign. The separately reviewed
`e574a468` bounded correctness packet now supplies actual accepted support:
four independent lifetimes, actual image/process/ELF/buildInfo/settings, two
public fsync checkpoints, canonical random preservation and deterministic
exact values across checked graceful close/reopen/removal. Its terminal audit
verified four removed full IDs and 101 recorded children reaped. The earlier
consumed failed manifests remain immutable and nonqualifying; the accepted
successor was fresh and separately bound, not an automatic retry. A bootstrap packet omits the new
preparation binding and cannot substitute for shell activation/preparation.
Synthetic preparation controls use clearly labeled fake tool metadata and raw
packets; their success establishes parser/control behavior only.

The separately authorized `0fb48d4` correctness attempt reached all four
lifetimes, public fsync calls and graceful exits/removals, then failed final
validation because actual MongoDB close-log timestamps used `+00:00` instead of
`Z`. The packet retains its failure/incomplete markers and remains nonqualifying;
the parser correction does not retroactively qualify it. Its independent
terminal audit verified all four full IDs absent, original process identities
absent and all recorded helper/parent children reaped.

Only Mongo close-log timestamps accept either `Z` or explicit `+00:00`, with
exact integer nanoseconds and unchanged signal/close/completion/exit ordering.
Nonzero offsets, unknown `-00:00`, missing zones and malformed dates fail closed.
Receipt and Docker timestamps remain strict `Z` only. The pinned
[JSONFormatter](https://github.com/mongodb/mongo/blob/9fbea53313e85fbbf864a963ff17b0bd6dafa0be/src/mongo/logv2/json_formatter.cpp#L343)
uses `DateStringBuffer::iso8601`, whose
[local and UTC branches](https://github.com/mongodb/mongo/blob/9fbea53313e85fbbf864a963ff17b0bd6dafa0be/src/mongo/util/time_support.cpp#L184)
emit an explicit offset and `Z`, respectively. This supports the observed
zero-offset syntax without claiming a nonzero local clock is UTC.

The final comparison completed the separately prepared explicit reference
route, with actual per-arm support/build/runtime bindings and review. Direct
`TARGETS=mongodb` execution through the general TreeDB shell remains disabled.
Offline preparation alone never enables or authorizes a product run. The retained
failed controls remain scoped source/identity/operation evidence and invalid
attempts. Runtime metadata remains version
metadata, with no runtime ELF attestation. This contract alone supplies no
performance qualification; actual complete measurements are identified below.

## Final-source comparison delivery

The [October 30 final report](../docs/benchmarks/ycsb_mongodb_gomap_final_2026-10-30.md)
and [compact evidence directory](../docs/benchmarks/ycsb_5335_final_evidence/)
bind twelve actual valid samples to source
`8bea490ad5a2d89a14f02f428b4283579a6dd864`. Relaxed median run rates are
MongoDB 42,753.6 and GoMap 59,245.2 ops/s; durable-B rates are 19,879.2 and
11,766.6 respectively. This is mixed, short-run evidence, with the exact
requests and resource asymmetry described below. Thirteen actual attempts include
one explicitly rejected observer-era GoMap sample. Its failed terminal remains
excluded and preserved alongside the reviewed same-budget continuation.

Relaxed timed requests use `w:1` with `j` absent on both arms. In `durable-B`,
MongoDB timed requests use `w:1,j:true`; GoMap timed requests omit `j` under
`command_wal_durable`, relying on the separately reviewed ordinary ACK/WAL
control. Both durable-B arms use the declared common load/census and public
same-owner `j:true` sentinel. Preserve this differing-request label and the
original actual control source; it does not replace the failed strict
same-request `j:true` ingestion experiment.

Current GoMap private checkpoint receipts use receipt schema 2 and the
`collections_checkpoint_storage` result from the actual owning collection
manager. Their request, endpoint and owner observation schemas remain 1.
Historical schema 1 `backend_checkpoint` receipts are not owner-complete aliases.
MongoDB continues to use its distinct public `fsync` and checked shutdown route.
Original producer DB/workload Close and all checkpoint/shutdown/reopen durations
remain separately reported outside the client's TOTAL throughput interval.

The shared raw wire helper has a monotonic total budget. Default-concern lookup
uses its existing three-second default; the public sentinel explicitly supplies
the manifest's helper budget. Connect is capped at the smaller of three seconds
and that total; later I/O uses the remaining total, preserving longer synchronous
`j:true` response waits without allowing incoming bytes to restart the budget.
Client Close remains in the existing `finally` paths. These tooling controls do
not change the measured request concern or imply equivalent crash guarantees.

Final publication retains all thirteen actual attempt statuses and twelve selected
sample slots, raw phase/Close and
runtime evidence, separate deterministic exact-value versus random observed-state
preservation scope, and every earlier failed or incomplete attempt. The
[existing provisional report](../docs/benchmarks/ycsb_mongodb_gomap_2026-10-30.md)
keeps its actual source and request identities. Only actual complete rows and
analysis accepted by ROOT/#5225 can close #5335; an unmet parent performance
objective remains open.

## Retained actual-MongoDB comparison

The [October 30 report](../docs/benchmarks/ycsb_mongodb_gomap_2026-10-30.md)
retains the completed j-omitted relaxed pairs, separately labeled common-load
j:true / scoped-durable-ACK pairs, and strict-j:true load failure, with original
source/build/request/Close bindings and raw phase evidence. All measured GoMap
rows bind to provisional 802f, not the later maintained integration head. The
reference implementation's two concern classes do not retroactively qualify a
sample or change its emitted request. Final-source applicability, dependency,
review and acceptance are recorded in the later final-source report and its PR;
these provisional samples retain their old identities. The parent performance objective remains open. This is not a new
dispatch plan.
