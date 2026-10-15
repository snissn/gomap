# Ordinary allocation integration checkpoint

WIP recovery checkpoint only, parent 177fb0c8db1d19c79d3d975126b4b0b64019568e. Integrates merged allocator/zipper source 5bca87ffe6e1bfba2d693264d2dc82936adc234f through actual common base 3f87d74b98e8b1e809d151611be03785f85e2283, preserving ordinary#5111 contracts. See source-manifest.json, integration-decisions.json and review-packet.json. Independent focused review ACCEPT is in checkpoint-addenda.

Full freelist, full zipper at normal GOMAXPROCS=4, and narrow DB reopen/format/readonly/root publication checks pass. Original seven zipper worker-precondition failures at GOMAXPROCS=1 are retained with the corrected scheduler run; no source, tests or thresholds were changed to fix them.

Exactly six alternating fixed1000x public Batch.Write samples at CPU0/GOMAXPROCS=1: medians 47139 to 21894 ns/op, 154839 to 34050 B/op, 286 to 132 allocs/op. The unchanged fixture seeds10000 keys and drains Checkpoint every8 outside measured foreground work; setup/checkpoint/final close are excluded. Source and binary input identities stayed fixed. This short component diagnostic is not full#5111, native, M0, Dgraph or final8x2s acceptance.

Existing batch/memtable/vlog arenas remain the default for their eligible storage. Maintained zipper clone-family scratch is reused with existing retention bounds. Immutable COW generations and encoded outputs can outlive batch reset through snapshots/rollback/candidate publication, so they retain owning detached storage and explicit private grants rather than borrowing resettable batch arenas. No new arena/pool/allocator/cap increase. Persistent value-log reachability and durability remain unchanged.

The original ordinary worktree, native foreign source, and common Git/index custody are preserved. This commit uses a private index; the active isolated integration worktree keeps its measured parent/index/diff. published-members.json binds every published member except itself; its own blob is bound by the checkpoint tree and remote readback. Two ELF contents are excluded; hashes and reproducible build commands are retained. Root owns graph state and final qualification.
