# GetMany callback pointer values

Grouped `GetManyView` pointer callbacks prefer the existing key-aware append
reader, then the key-aware view reader, then the generic append reader, then
the generic view reader. This matches iterator value routing and preserves
key-aware lookup when only a generic appender is available. The append route
can reuse the value log's existing compressed grouped-frame cache; CRC checks,
cache identity, admission limits and manager-wide retention budget remain in
force. Cache hits copy selected bytes into the callback destination, so this
route borrows neither cache slots nor mapped value storage.

One page-sized destination is acquired lazily per grouped tree batch when an
appender first reads a pointer. Inline-only batches and reader-only routes do
not acquire it. It is separate from the active leaf-page scratch and is reused
only after each callback returns. Returned backing is never saved in scratch;
values larger than a page use temporary output, leaving the pooled destination
at one page. Repeated oversized values can therefore allocate repeatedly.
Callbacks must still copy values they retain. Owned `GetMany` results and
`GetAppend` path counters keep their existing behavior.

The per-key leaf planning, small-batch fallback, callback indices, duplicate
and missing-key behavior, root pins and leaf leases retain their current
contracts. This change does not introduce shared internal-node traversal.

Focused route, lifetime, bounded scratch and actual CRC-verified compressed
frame cache checks use the existing tree fixtures:

```sh
GOWORK=off go test ./TreeDB/tree -run '^TestTreeGetManyPointerView' -count=1
```

## Measured selected-source qualification

Five matched Linux fresh-process pairs compare selected candidate
`75fe577ecc6ab81237df2cff58454bbeb205cc71` with control
`bc9764ab92a9ceeeb4ae15706c920653e5dd8bbc`. Every process uses the unchanged
full 250,000-key fixture, 64-key input, 128 natural batches, 1,000 measured
calls, verified CRC and the same complete value/miss/duplicate consumer.
All twelve modes and original plus extension runs remain retained. Reported
latency includes the public API and consumer; it is per batch of 64 keys.

| Pointer callback shape | Control → candidate median us/batch | Change | Control → candidate B/batch | Allocations/batch |
| --- | --- | --- | --- | --- |
| sorted | 363.113 → 143.948 | -60.36% | 271574 → 776 | 175 → 7 |
| clustered | 346.298 → 148.301 | -57.18% | 263490 → 776 | 175 → 7 |
| uniform | 404.067 → 218.507 | -45.92% | 270630 → 912 | 175 → 7 |

Sorted and clustered callbacks improve by at least 25% in every matched
pair; their bytes and allocation counts fall by at least 50% in every pair.
Actual internal traversal counts are unchanged. The source routes pointer
values through the existing appender/frame cache; no shared internal
traversal was introduced. These observations do not isolate a causal
fraction of the measured latency gain.
The deterministic compressed-frame test verifies cache admission/hits and
CRC rejection; callback destinations remain bounded as described above.

Shared-host timing spread remains 10–21% in several cells after all five
pairs. No observation was removed and no quiet/cold-cache claim is made.
Root accepts the consistent callback objective and explicitly accepts the
small observed owned-read tradeoff: pointer/uniform owned median +2.37%
(worst pair +7.93%), +100 B/batch (+0.27%) and unchanged 11 allocations;
inline/uniform owned median +1.29% (worst pair +2.71%). Other inline/owned
median changes range from -5.83% to +1.93% with mixed directions. These
observations do not establish precise inline/owned gains or no regressions.

A separate six-process forced-GC diagnostic passed all 72 full cells and all
72 paired retained-HeapAlloc/absolute phase-end VmHWM guards: zero material
increases above max(4 MiB, 15% of matched control). All 288 decoded-cache
observations stay within 64 MiB. Pointer callback phase-end VmHWM medians
were sorted 499.348→353.457 MiB, clustered 538.520→353.871 MiB and uniform
538.758→355.691 MiB. Linux reset-phase HWM is approximate; this diagnostic
is separate from latency and does not establish an exact peak, general RSS
ceiling, architectural cause for the heap differences or capacity result.

Retained evidence is in `tmp/treedb-quicksilver-graph-20261001` and the
corresponding `/mnt/fast4tb/gomap-quicksilver-full-20261002/A` packets.
Five-pair analysis SHA256:
`33329d7db4892f758d8092a0f3aa4444623d1f3811f0a88322dd24427baceba9`.
Residency analysis SHA256:
`7908f7ecf6c2cab50ed93985c07c6a32bebe2308c7c4778a0bf6b19e144ff99d`.
Normal executable hashes remain control
`f4559a0125776a47df3ceebac35eeacbd605b3a399d9fca09be3b28baa1f5c99`
and candidate
`519cec8a30c6cd365ef2e64426faf8a7bf6bf55f8e743e540b08b9f3a94e211a`.
Full source/environment/input freezes and distinct counter/diagnostic
executables are retained with their original identities. Earlier shared-
planner pilots and rejected experiments are historical evidence only.
