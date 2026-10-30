# Provisional MongoDB reference lifecycle

The maintained shell still refuses execution selecting `mongodb`, before output
creation or commands. This separate implementation remains provisional. Its
earlier retained live readiness failure started no workload or performance sample.
A complete synthetic
packet may be consumed through the ordinary shell's `PARSE_ONLY=true` entry.
That is parser coverage, not MongoDB readiness or product evidence.

`ycsb_mongodb_reference.py` defines a separate singleton manifest kind
`mongodb-reference-checkpoint-close-reopen`, lifecycle contract integer 1,
profile `mongodb_w1_default`, BSON format, and write concern
`{"w":1,"journal_requested":null}`. This ordinary w:1 request is not a claim
of j:true, TreeDB durable-profile equivalence, uncheckpointed WAL replay, crash
or power-loss certification. The public fixture uses the existing BSON driver
with explicit W1; its Disconnect is checked and does not invoke a checkpoint.
The timed client's existing checked DB/workload Close receipts stay separate.

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

Preparation copies only root and per-target regular evidence files, never
service DB directories or symlinks (at most 2048 files, 16 MiB per file and
128 MiB total; metadata child output is capped at 1 MiB). It revalidates the copied complete packet,
then binds its actual raw buildInfo, endpoint, process/container/public identities,
settings and journal parameters in `reference-support.json`. Runtime records stay
version metadata with `runtime_elf_attested=false`. Source dependencies
(including the maintained shell entry), original
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

Still outstanding: a preparation binding from that accepted real reference
packet, followed by review and end-to-end lifecycle activation in the maintained
shell. The offline preparation route alone never enables a run. The retained
failed controls remain scoped source/identity/operation evidence and invalid
attempts. Runtime metadata remains version
metadata, with no runtime ELF attestation. MongoDB reference performance remains
unattempted.
