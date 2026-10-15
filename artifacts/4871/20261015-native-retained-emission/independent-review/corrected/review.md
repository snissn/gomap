ACCEPT — corrected three-file emission escape delta; no actionable findings. Native ACK=0.

Exact packet: `native-retained-emission-escape-fix-20261015`; source receipt SHA256 `2788a76738b2ae72733440170ea22a08cd16f403cf4bcfe4c95a3c20fdeccc2f`; manifest SHA256 `fa2426bb4d4c50156e6e1e493c164fca6fb7d3ac2a3038e04a3885e5c07c2845`. All 27 members independently hash/size checked. Only txn, emission, and emission test differ from the original accepted five; format and encoder are byte-identical.

`emitStatePages` (txn:636) now initializes stack-owned fixed-depth control and drains the same `advance` loop directly. Retained `step` (emit:185) still binds exact root/generation/next-pointer/sink identities before advancing and preserves first-error stickiness. The encoding, admission, page IDs, sink operations, and output ownership stages are unchanged.

Embedded controls avoid temporary allocation. Fresh 4KiB output retains candidate/writer lifetime (emit:262); resettable batch/memtable/vlog arenas cannot supply that lifetime without changing ownership. Existing arena inventory and detached-output exception apply; no new pool or allocator.

The actual allocation regression is RED at 3 with old txn/emission and GREEN at exactly 1 with corrected bytes. GREEN also passes canonical/decoder/retained alias and opaque refusal/exact-yield binding/sticky-error selectors. Both runs have unchanged 4427-input censuses. Compiler evidence reports `next` and the original recordingSink do not escape. Reviewer ran no Go or benchmarks.

Source-bound six-sample public ordinary guard passes all preset gates: medians baseline 25,576 ns / 46,330 B / 148 objects; corrected 25,902 ns / 46,298 B / 147 objects. This is a short guard, not final qualification. Original failed guard remains preserved.

Scope stops at this repair. Native ACK remains 0; retained full MaterializeCandidate constructor/reservation/header/closure work remains pending. No broader architecture review or new semantic gate imposed. Full hashes, evidence bindings, and limitations are in review.json.
