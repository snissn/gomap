# Optional storage snapshot (#5266)

`scripts/mvcc_storage_context.py --output FRESH_FILE --path EXISTING_ROOT
--stage before-build`
collects a single bounded read-only snapshot. Repeat `--path` up to sixteen times;
records retain positional order rather than serializing potentially sensitive
paths. Invoke separately outside measurement when authorized. It launches no
child, benchmark, writer, drain or flush. The existing gate and its default path
are unchanged. No automatic invocation, CI change or instrumentation acceptance
is supplied. Every record has `qualification=unavailable`, including successful
reads; missing counters never establish quiet or qualification.

A path record describes the current existing root only, not Go's future TempDir
child. Longest-parent mount resolution requires matching stat and mountinfo device
identities. Multiple mounts at any matching ancestor (including stacked ancestors hiding
otherwise plausible child mounts)
remain unavailable; device equality cannot disambiguate mount identity. Unknown
mapping remains unavailable. Mount sources/options, arbitrary
counter text, environment and command lines are omitted. Filesystems, device and
mount IDs are context; physical topology/isolation and ownership are explicitly
unavailable. Separate roots/devices do not prove separate physical storage.

Dirty/Writeback are machinewide KiB; device stat fields are cumulative kernel block
statistics in their native Linux field order. Neither establishes task attribution,
request latency, clean page cache or quiet. Snapshot reads can perturb a host.
Outputs are exclusively created with mode0600; existing files/symlinks are refused.
Counter inputs cap at64KiB, paths16, serialized output256KiB. Use a trusted existing output
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
and sixteen samples. The active guard uses the shared [external-cpu-v2 policy](quiet-host-policy.md):
10% per process and 50% observed aggregate, in units of one logical CPU. Historical
2% failures retain their original disposition. CPU-only and foreign
writer controls need known process/inode identity; block statistics alone cannot
attribute shared-worker CPU. No control campaign is authorized here.

Task-file fsync can trigger shared journal work and does not isolate ownership.
No fsync/global sync/syncfs or task drain is implemented. Any later preparation
change needs independent contract review and predeclared controls. Telemetry is
not a repair: both original failed qualifications remain failed.


Schema2 requires an operator stage: before-build, after-build, after-lifecycle,
quiet-start, quiet-end, collection-start or collection-end. Labels are assertions,
not verified execution phase markers. Optional --source-sha requires a lowercase
40-character SHA; it records an operator assertion, not a verified checkout.
Optional --binary and --harness produce SHA256/size observations without recording
paths or contents. Missing/unreadable/nonregular/symlink/oversize/changed files
are unavailable. Digests stream at64KiB with a512MiB total limit per file and
before/after file-identity metadata checks; they do not establish immutable runtime
binding or execution. Hashing can cause substantial reads: perform it outside
measurement, bind retained snapshots explicitly, and do not imply phase histories
were sampled atomically. A single snapshot does not reconstruct original build or
lifecycle dirty-page history. All statuses remain diagnostic-only.

Binary and harness input ancestors must be trusted: O_NOFOLLOW protects only the
final file component; intermediate directory symlinks are followed. The digest
identifies bytes read through the opened descriptor. The supplied path may be
renamed or replaced during/after hashing and may no longer name that descriptor;
metadata checks do not prove continuing path identity. Hashing both maximum-size
inputs reads up to1GiB in total and must remain outside the quiet acquisition and
measured observation. No automatic phase placement or scheduling is implemented.
