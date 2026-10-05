# Same inode checkpoint replay, v2

Conditionally feasible and simpler than rebind: reuse ONE exclusively-owned,
freshly created initial-checkpoint DB pathname and its original resource objects
for all six separate read processes. Seal a separate byte backup after the create
process exits and Close succeeds. Use the retained v1 Go overlay and exact public
durable-primary arguments/environment; there are no v2 Go changes. Public p99
acceptance remains unresolved. This is warm read-path causal evidence.

## Exact exception and source evidence (frozen H and M)

Unix stable identity is dev+ino (`resource_identity_unix.go:114–130`); namespace
parent generation hashes that identity (`resource_token.go:1099–1135`). Keeping
original directories and resource file identities preserves that authority even
when their bytes are reset. Directory mtimes are not identity inputs.

Writable Open always refreshes format metadata (`db.go:2572` ->
`format_config.go:493–513` -> `writeFormatConfig:454–477` ->
`vlog_gc_incremental.go:942` -> `atomicfile.Write:34–77`). A same-directory temp
file is renamed over `format.json`. Thus its inode cannot be required unchanged.

The exact exceptions are **maindb/format.json, dictdb/format.json, and
templatedb/format.json, restricted to regular files actually present at seal**.
All must exist afterward and have exact sealed logical bytes before every read
process. No wildcard exception, directory exception or format-byte exception.
`format.json` is an auxiliary open/format-policy document, not a captured stable
durable-root resource in this fixed generic workload: canonical reachability
inventory (`resource_inventory.go:44–86`) lists no format-document dependency;
index capture forcibly binds pager `index.db` (`stable_resource.go:337–381`);
dict capture binds that index plus the exact pointer's value-log segment
(`dictdb/resource_capture.go:84–138`); template capture similarly binds index and
pointer segment (`templatedb/resource_capture.go:129–180`). No production format
path reference occurs in rootpublication, root publishers/capture, value-log
manager or these side-store capture files. This is a source-bound conclusion for
the exact H/M generic fixture, not a new global format-file policy. Evidence
source hashes are frozen in `source-receipt.json`.

## Other Open/Close effects: conditional feasibility, no exemptions

- Open can repair/swap index artifacts (`db.go:2204`, `index_swap.go:43`), remove
  stale leaf-pack staging (`db.go:2207`, `leaf_generation_pack.go:29`), extend index
  mmap backing in place (`pager.go:196–204`), replay/repair WAL, create a fresh
  active journal and unlink recovery-covered WAL (`db.go:2395–2446`). Start from
  successful initial checkpoint/close, with no stale swap/staging or failed DB.
- Public command-WAL Close invokes a checkpoint even without explicit replay
  writes (`public.go:1808–1818`). Cached Close may flush, sync and delete lane WAL
  (`caching/db.go:26229–26443`); backend Close attempts covered-WAL cleanup
  (`db.go:2959–2964`). Default packing off does not disable journal cleanup.
  Existing retained-prune workers and close drain are preserved.
- LOCK is created/opened in place and only unlocked/closed, not removed
  (`lockfile.go:47,124–141`). Close maintenance can vacuum if explicit environment
  opts enable it (`public.go:1728` onward); preserve sanitized public environment.
  Do not enable maintenance or mutate the suite to avoid a guard failure.

Any original non-format file deletion/replacement or original directory identity
change **rejects v2** and preserves the failed state. In particular, do not waive
an original WAL unlink because it is normally valid retirement. Byte resets may
alter filesystem block allocation; logical index/vlog layout, exact file bytes
and authority identities are controlled, not device extents or cold-device state.

## Root command plan

Root owns native scheduling. Nothing native or Go was run for this packet.
First create the new initial checkpoint with the v1 baseline overlay in its final
working pathname. Do not copy/rename this DB later; do not use a final mutated DB.
After create exits:

```sh
python3 "$V2/inplace.py" seal --owned-root "$GRAPH" --db "$LIVE_DB" --backup "$SEALED_BACKUP" --manifest "$V2_NATIVE/seal.json"
python3 "$V2/inplace.py" check --owned-root "$GRAPH" --manifest "$V2_NATIVE/seal.json"
```

All paths must be graph-owned, clean absolute paths; seal/backup paths are
disjoint and backup is a real copy, never hardlinked. Backup hash+identity are
verified before and after every reset. It is never opened by TreeDB or changed by
the helper. Retain v1 source/overlay and binary provenance. For each read in
**AB / BA / AB**, use the same LIVE_DB with a fresh child process, same existing
environment/public argument vector and unique capture/result paths:

```sh
GOMAP_QS_REPLAY_MODE=read GOMAP_QS_REPLAY_DB="$LIVE_DB" GOMAP_QS_REPLAY_JSONL="$CELL/live.jsonl" \
  python3 "$V2/inplace.py" run --owned-root "$GRAPH" --manifest "$V2_NATIVE/seal.json" --out "$CELL/runner" -- "$DIAGNOSTIC_BINARY" [exact command-plan.json args]
```

The runner requires a graph-owned absolute executable. Parent is responsible for
exclusive ownership and confirming no other DB owner/process/maintenance run;
this helper does not invent a new lock protocol. Existing benchmark guard and
root process timeout policy remain required. Standard stdout/stderr/command/hash
receipts are captured. No concurrent large collector runs during child timing.

Before launch it requires exact file membership/full byte hashes plus every
original directory/non-format file dev+ino. It opens and **keeps original object
FDs across the entire child lifetime**, preventing inode recycling from hiding
unlink/replacement (it also checks retained handles still have links). Ensure
native FD limits cover the sealed census; inability to pin fails before launch.
All helper hashing/copying happens before or after the child, not during timing.

After the child exits, retain full post-run census and byte copies of every new,
changed or replaced file; unchanged bytes are already in the sealed backup. Then
reject resource replacement/removal, new directories and nonzero child exit
before reset. Reset only differing original file chunks by r+b seek/write,
truncate/flush/fsync, preserving the currently observed inode (including permitted
format files). Remove only newly-created owned regular files, after preservation.
Fsync original directories and require exact sealed bytes/membership and original
resource identities again. Each next run also checks this precondition.

Do not retry or reconstruct a failed original resource inode. Stop and preserve
all receipts/live state/backup if any guard fails. A failed guard after partial
reset still leaves the complete changed-file archive and original sealed backup.

Require the same full initial oracle on every replay, literal 50k warmup and three
read-only modes from v1, equal deterministic counters, and inspect per-phase
checkpoint/background counters plus Open/Close physical changes. If mode timing
still regresses on this identical initial checkpoint, direct cache-path effects
become more plausible. An improvement here does not waive the end-to-end p99 gate.

## Validation

`self-check.py` uses tiny graph-owned Python fixtures only: in-place changes and
new files reset successfully; an atomic format replacement is allowed and reset
to exact bytes; original index replacement is rejected **even if identical bytes**;
an original resource deletion and a directory replacement are rejected; sealed
backup stays unchanged. No Go/native benchmark, source edit, rebind or PR action.
