# Query-ready build bounds and prepared-asset handoff

M5 builds QRBG/QRDG images as rebuildable, non-authoritative derived assets and
hands them to TreeDB's existing column asset manager. It does not add another
publisher, manifest, recovery selector, WAL record, sync protocol, GC path, or
deletion owner.

## Pipeline and ownership

`queryReadyBuildCoordinator` owns the bounded work before authoritative
publication:

1. estimate a conservative request reservation from source image bytes, part
   metadata, tombstones, and alignment slack;
2. admit at most `MaxWorkers`, with a per-worker share of
   `MaxInFlightBytes`;
3. build a deterministic base, delta, or consolidated-base image;
4. append the complete image through the existing column asset manager;
5. register the complete ref set through
   `RegisterColumnAssetPreparedAssets`;
6. return the stable asset ref, exact segment byte range, diagnostic path,
   generation/schema identity, sorted source dependencies, and the existing
   lifecycle lease.

`OpenFileDescriptor` translates the asset-manager kind into M3's
`typedcolumn.QueryReadyGenerationFile` at this handoff. The descriptor keeps
the manager's exact nonzero offset and length, so M3 maps and validates the
prepared range directly instead of reading the whole asset file or locating a
lane again by pathname. The manager checksum remains on the returned asset ref
and binds the same byte range.

The admission queue is intentionally zero. Saturated calls fail with
`QueryReadyBuildBackpressureError`, so the build layer cannot accumulate an
unbounded internal request queue. A request larger than its worker share fails
before output allocation with `QueryReadyBuildBoundError`. The coordinator
tracks current/peak workers, reserved in-flight bytes, and both rejection
classes.

Cancellation is cooperative at phase boundaries: before admission, after
image construction, and after the asset append but before registration. QRBG
and QRDG validation is currently a synchronous CPU phase and is not interrupted
mid-part. Cancellation or registration failure after append invokes the
existing safe prepared-tail cleanup; no lifecycle record is installed. A
successful result remains protected by the existing prepared-asset lease until
the caller hands it to publication or calls `Abort`.

## Measurement contract

Build stats distinguish:

- base, delta, consolidation, asset append, existing-manager registration, and
  total handoff duration;
- rows, input/output bytes, assets, parts, copied bytes, compressed bytes,
  SHA-256 dependency bytes, checksum bytes, and base bytes rewritten;
- write amplification;
- one final encoded-buffer size from the format builder; and
- the conservative coordinator reservation used for bounded admission.

The reservation is a safety bound, not a claim about exact Go live heap. The
encoded-buffer counter excludes caller-owned inputs and Go metadata. Focused
`-benchmem` evidence supplies cumulative bytes and allocations, while a
same-host process RSS/live-heap capture remains the peak-memory evidence.

The handoff does not hash the complete QRBG/QRDG output again. The stable asset
ref already carries the manager checksum, the formats carry internal header and
table CRC-32C, and every QRBG dependency carries its image SHA-256. Adding a
second whole-output SHA-256 pass would duplicate current validation work; the
later authoritative token inventory may add a digest only if its shared
publication contract requires one.

The profile-selected M5 primary metric is QRDG build allocation/latency. On the
M2 baseline host, the derived envelope took about `46.5 us`, `74.4 KB/op`, and
`114 allocs/op`, versus `28.3 us`, `40.0 KB/op`, and `105 allocs/op` for the
validated inner container. The implementation encodes QRBG directly into the
final QRDG buffer while preserving all QRBG/QRDG validation, SHA-256, CRC-32C,
identity, deterministic-byte, and reopen checks.

Numeric guardrails for this change are:

- output bytes and write amplification unchanged;
- QRDG `B/op` and encoded-buffer bytes no greater than the former
  inner-plus-outer construction;
- no material regression in base build or consolidation latency/allocations;
- bounded worker count and conservative in-flight reservation under concurrent
  saturation; and
- no registered partial generation after cancellation/error.

The canonical 1M load target remains TreeDB no slower than `1.5x` ClickHouse.
This M2-based branch does not yet route the production JSONBench load through
query-ready asset construction, so a canonical load run cannot honestly
attribute a load change to this pipeline. M6 owns that integrated matrix after
M4 consumes the prepared assets. Production-shaped focused QRBG/QRDG and
consolidation evidence is still required here.
