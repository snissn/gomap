# Zipper clone scratch reuse: issue 5133

Product cb5960399df0c23e320193b64d8b6a5db5844c3e; base 3f87d74b98e8b1e809d151611be03785f85e2283. Exact-byte independent review ACCEPT in review.json. Final-main report: packet/final-main/report.md. Full zipper, pointer Checkpoint/WriteSync reopen/GC and focused race passed.

Three alternating fixed1000x public Batch.Write pairs: median 152734 -> 141606 B/op (-7.286%), 281 -> 269 objects; 45653 -> 44701 ns/op (21905 -> 22371 ops/s). Short latency is directional, not a qualified speedup. Same seeded10k/stopped-timer Checkpoint fixture. Source census, baseline single-file overlay, commands, raw output and binary identities retained.

Earlier ordinary177 v1 profile proves recurring newMergeScratch 33323104 -> 0 bytes in exact public Write frame. Its pre-final-trim source is separately pinned; it is not relabeled final-main profiling evidence. Product scratch-reuse.md documents retained-capacity caveats. No live retained-heap claim from post-Close inuse data.

Required latest-head CI/GitHub gates remain root-owned. Original eight-repeat micro, M0/Dgraph, bounded native ACK/Q32 and final graph acceptance remain open.
