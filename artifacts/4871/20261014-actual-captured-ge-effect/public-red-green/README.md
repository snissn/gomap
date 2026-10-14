# Provisional captured-GE production consolidation

Scope: exactly nine paths in each actual ordinary-5111 and native-4878-traversal lane, using the same six frozen production files and original tree unit. The standalone regression lives in the maintained mvcc_test package; its DB.Close is checked even when the allocation assertion fails. No benchmark harness or complete projection was imported.

The initial public RED is source-faithful in both actual lanes: drained cache, captured nonempty backend, timestamp-15 historical selection returns timestamp 10, and the large fixture proves depth 3 / 2728 leaves with an actual between-version leaf boundary. Both allocation assertions fail before GE, and checked DB.Close succeeds. Fixed 16x allocation guards discriminate the complete iterator fallback; these are not throughput or latency measurements.

Selected normal/race controls cover PRIMARY operand validation, bounded external-leaf readers, source precedence/pressure, pointer ownership/refusal, physical tombstone fallback, queued DB Close, owned outputs and original public PRIMARY activation/empty-checkpoint semantics. One pre-existing retained-heap diagnostic explicitly requires opt-in and remains SKIP. All original all19, M5, performance, platform, full native and PR readiness gates remain open.

The narrow documentation addition changes no public semantics, format, WAL ACK or publication authority. Existing empty-root, RELAXED affinity, checkpoint repair, lifecycle/cap/ABI source and the two foreign native tests are preserved. Full tracked and nonignored untracked file inventories bind content/bytes or symlink identity, HEAD, actual index SHA and unrelated status. Baseline HEAD Git blobs are not claimed to represent dirty tested source; tested SHA256 rows are authoritative.

Before/after patches compare against the copied actual BEFORE bytes, not merely HEAD. The retained prototype is immutable. Candidate copies and final-source.json bind exact final tested bytes. Command receipts retain actual argv/env/cwd/exit and raw SHA256. No source commit, GitHub mutation, standalone performance collection or cleanup was performed by this packet.
