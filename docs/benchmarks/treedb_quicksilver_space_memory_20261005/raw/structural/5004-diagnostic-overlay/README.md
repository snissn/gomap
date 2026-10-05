Diagnostic-only offset metadata and full allocs snapshot packet

Inputs are exact landed H a6b383b6c0d6269a598390032ecce9fd619a4eac
(tree identical to frozen fb42d5ba229d2cbfba8727272af02f9544a33389), and
candidate M 90219d64ced3539accff5d0e50d9b5fa6424ed20. Two Go files per variant
are replaced by Go's overlay feature; no production checkout/PR changes.
receipt.json freezes source and generator SHA256 values. Added instrumentation:
61 lines shared between baseline/candidate. gofmt parsed all four generated Go
files; Python self-check passed. No builds or Go tests were executed here.

Regenerate + runnable mechanical self-check on a Git-enabled host:

  python3 generate.py --repo /private/tmp/gomap-qs-5004-offsets --out "$PACKET"

Copy this entire packet to the native graph directory. For each isolated source
checkout/archive, rebase manifest paths before building (no Git needed):

  python3 "$PACKET/generate.py" --manifest-only --out "$PACKET" --build-root "$SOURCE"
  GOWORK=off go build -overlay "$PACKET/baseline-overlay.json" -o "$BIN" ./cmd/unified_bench

Select candidate-overlay.json for the exact M source. Root owns runner setup,
builds and runs; these commands have NOT been executed by this worker. Rebase
again when using a different source root. Manifest-only mode writes manifests
only and does not alter/hash source files. Keep both input archives and overlay
receipt separate from the unmodified qualified public matrix.

Before the diagnostic run, create a unique run output directory and its snapshot
subdirectory, then set explicit absolute paths:

  export GOMAP_QS_DIAG_CACHE_JSONL="$RUN/grouped-cache.jsonl"
  export GOMAP_QS_DIAG_SNAPSHOT_DIR="$RUN/full-allocs"

Cache stats emits one JSONL record per existing stats() invocation per cache,
using the existing shard and slot lock traversal. Records identify cache pointer,
owner segment path, PID and capture start UnixNano. Stats.AllocatedSlots and
Stats.Entries come from that traversal. LiveK counts valid entries. AllSlotOffsetCapacity
counts every allocated slot, including never-used and empty slots; EmptySlotOffsetCapacity
identifies backing retained after eviction (including zero-cap never-used slots).
Inline distinguishes baseline arrays from candidate slices. OffsetBackingBytes
counts separate slice backing only; InlineOffsetBytes counts bytes embedded in
baseline slots only. SlotStructBytes uses reflect.Type.Size and allocated slots.
StructuralMetadataBytes = SlotStructBytes + OffsetBackingBytes, so baseline
inline arrays are not double counted. All capacities are elements; bytes = cap*4.
Allocator size-class rounding, shard allocations, cache objects and raw payloads
are excluded. These are retained structural bytes, not total Go heap/process RSS.

Use individual records or group records belonging to the same existing manager
stats pass. Never sum repeated captures of a cache across time. Captures use
existing per-shard/slot synchronization and are not an atomic global snapshot.
Diagnostic maps/reflection/JSONL I/O affect allocations and timing; this overlay
must not supply qualified throughput/RSS performance numbers. Public unmodified
matrix remains the acceptance source.

writeAllocsSnapshot now hard-links its successfully written complete profile to
the explicit full-allocs directory, keeping the original basename (base/after and
random suffix). Original snapshot cleanup still happens; the linked inode stays.
Existing two GCs and profiling calls remain unchanged. Empty envs disable both
outputs. Output open/write/close/link errors fail visibly, without stdout output.
The caller must create output parents. Native snapshots must be on the same
filesystem as the temporary original for os.Link; set TMPDIR to a graph-owned
same-filesystem temporary directory before the run. Profile sample inuse_space
from these FULL snapshots can supplement histograms; existing public DELTA
profiles cannot be interpreted as total retained heap.

Self-check fixture proves baseline five slots: 5,120 inline offset bytes and
5,680 total structural slot bytes. Candidate capacities 2/5/0/33/256: 1,184
separate backing bytes plus 680 slot bytes = 1,864 structural bytes, with live K
histogram {1:1,4:1,255:1} and empty capacities {0:1,33:1}. Production reflect
sizes are captured at runtime rather than assuming fixture's amd64/arm64 sizes.

Validate captured records without double-counting repeated snapshots:

  python3 "$PACKET/validate_jsonl.py" "$RUN/grouped-cache.jsonl"

The validator checks live/empty/all slot counts and inline/backing/structural byte
identities for every record; it does not infer global capture boundaries. Its
baseline/candidate synthetic fixtures and rejection of wrong byte totals passed.
