# Provisional MongoDB reference lifecycle

The maintained shell still refuses execution selecting `mongodb`, before output
creation or commands. This separate implementation is provisional and has had
no live container, product workload, or performance run. A complete synthetic
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
version and gitVersion, Linux/amd64, actual mongod and Docker executable hashes,
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
getCmdLineOpts observations matching that actual container/process. The same
public identity is checked before every measured/helper operation and before
close. Reopen uses the same image, binary and mounted directory with independent
container, process and server-generated topologyVersion processId.

After all clients settle, a separate helper invokes the real public admin
`fsync:1, lock:false`, retaining canonical Extended JSON requests/replies,
actual monitored connection IDs and checked timed results. It is bound to the
preclose snapshot, active process and public identity. It does not invent a
MongoDB TreeDB-owner checkpoint API or relabel shutdown as this checkpoint.
Only MongoDB 8.2.7 source commit
`9fbea53313e85fbbf864a963ff17b0bd6dafa0be` currently has a reviewed source
contract: fsync calls storage-engine flushAllFiles, which selects the standalone
WiredTiger checkpoint path. Other versions fail closed pending source review.
The commit-bound chain is
[fsync.cpp](https://github.com/mongodb/mongo/blob/9fbea53313e85fbbf864a963ff17b0bd6dafa0be/src/mongo/db/commands/fsync.cpp#L125),
[storage_engine_impl.cpp](https://github.com/mongodb/mongo/blob/9fbea53313e85fbbf864a963ff17b0bd6dafa0be/src/mongo/db/storage/storage_engine_impl.cpp#L506),
and [wiredtiger_kv_engine.cpp](https://github.com/mongodb/mongo/blob/9fbea53313e85fbbf864a963ff17b0bd6dafa0be/src/mongo/db/storage/wiredtiger/wiredtiger_kv_engine.cpp#L961).

Graceful shutdown retains the actual full-ID Docker SIGTERM action, Docker wait
exit status, terminal inspect and source-bound WiredTiger-close (4795901) then
mongod-complete (8423404) structured log records. The reported interval is the
observed process shutdown/wait interval. `close_return_observed` is always false:
this is not a measured public Go Close API return. Checked full-ID removal is
separate. Forced cleanup permanently retains a failure marker and cannot
produce qualifying close evidence; signals/removal require matching complete
ownership/configuration, never an unrelated container name or reusable PID.

The validator independently checks raw CLI output copies and arguments,
nonoverlapping settled-client chronology, public-command results and routing,
canonical typed random snapshots across reopen, exact deterministic expected
values/acknowledged outcomes, same-volume independent reopen, and checked
shutdown/removal. Any retained parent/helper failure marker invalidates the
attempt. Output is labeled `validated-reference-lifecycle-evidence`, separate
from TreeDB baseline evidence, with COW inapplicable, no observed Close return,
no WAL replay proof, no power-loss certification and no complete matrix claim.

Still outstanding: independent source review of this provisional implementation,
actual 8.2.7 buildInfo/ELF/settings/log-shape support controls under separately
authorized host custody, a reviewed preparation route producing the pinned
manifest, and integration enabling execution in the maintained shell. Same-UID
procfs access and the public image's settings/log shape are not established by
the synthetic tests. A separately reviewed runtime identity contract is also
required before activation. MongoDB reference performance remains unattempted.
