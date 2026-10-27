# Optional storage snapshot (#5266)

`scripts/mvcc_storage_context.py --output FRESH_FILE --path EXISTING_ROOT`
collects a single bounded read-only snapshot. Repeat `--path` up to sixteen times;
records retain positional order rather than serializing potentially sensitive
paths. Invoke separately outside measurement when authorized. It launches no
child, benchmark, writer, drain or flush. The existing gate and its default path
are unchanged. No automatic invocation, CI change or instrumentation acceptance
is supplied. Every record has `qualification=unavailable`, including successful
reads; missing counters never establish quiet or qualification.

A path record describes the current existing root only, not Go's future TempDir
child. Longest-parent mount resolution requires matching stat and mountinfo device
identities. Multiple equally specific mount matches (including stacked mounts)
remain unavailable; device equality cannot disambiguate mount identity. Unknown
mapping remains unavailable. Mount sources/options, arbitrary
counter text, environment and command lines are omitted. Filesystems, device and
mount IDs are context; physical topology/isolation and ownership are explicitly
unavailable. Separate roots/devices do not prove separate physical storage.

Dirty/Writeback are machinewide KiB; device stat fields are cumulative kernel block
statistics in their native Linux field order. Neither establishes task attribution,
request latency, clean page cache or quiet. Snapshot reads can perturb a host.
Outputs are exclusively created with mode0600; existing files/symlinks are refused.
Inputs cap at64KiB, paths16, serialized output256KiB. Use a trusted existing output
parent with trusted ancestors: O_NOFOLLOW protects only the final parent component,
and intermediate directory symlinks are followed during path resolution; no secrets should appear in supplied paths. Collection errors exit2;
a partial write is incomplete evidence, never qualification. No gate checker
consumes these records.

The bounded nofollow-read pattern follows #5282's collector at af8a87f45. This
standalone implementation has no import, cherry-pick or runtime dependency on
that unmerged work. Its narrower schema avoids child execution and raw system
text capture. Consolidation can follow integration of a reviewed common helper.

Before a causal experiment, predeclare full build and lifecycle dirty-page history,
including existing observer/receipt writes. An idle control alone does not reproduce
that history. Preserve exact binaries, fixture storage/durability, all eight pairs
and sixteen samples. Guard remains per-process CPU >=2% over its original measured
observation; aggregate foreign CPU is a different predicate. CPU-only and foreign
writer controls need known process/inode identity; block statistics alone cannot
attribute shared-worker CPU. No control campaign is authorized here.

Task-file fsync can trigger shared journal work and does not isolate ownership.
No fsync/global sync/syncfs or task drain is implemented. Any later preparation
change needs independent contract review and predeclared controls. Telemetry is
not a repair: both original failed qualifications remain failed.
