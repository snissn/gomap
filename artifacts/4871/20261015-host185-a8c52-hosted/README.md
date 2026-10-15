# Exact a8c52 hosted evidence and native phase-25 checkpoint

Ordinary candidate: `a8c52ed66fc587d4823f020cf15ea1c3ee8c1ebc`.
Control: `5bca87ffe6e1bfba2d693264d2dc82936adc234f`.

Hosted raw-path gate passes all five original benchmark groups, eight paired alternating AB/BA samples per revision. The coordinator downloaded artifact 11994602458 from run 38777236329, verified all six executable sizes/hashes and bindings, and recomputed the summary with the checker from the exact candidate. Recomputed JSON equals hosted JSON. Executables remain in the hosted artifact and task-local download; this packet retains their hashes and verification receipt, not executable copies.

Paired deltas: Get -1.72%, DATA batch -3.83%, snapshot seek +1.84%, repeated iterator -0.39%, tiny durable public batch +0.28%. All unchanged timing and allocation gates pass. This evidence applies to this exact rejected candidate; its successor requires fresh qualification. Matched PRIMARY cost remains a separate gate.

M0 remains RED: foreground p99 CV 0.245 exceeds the unchanged 0.10 limit. All ten legacy calls completed without abort; public classification passed. Its raw packet is preserved in predecessor commit 7ec95bf7e1df50ff6ca957cc84c0e7db81d00c55. This packet includes the earlier f041 M0 run for provenance, not causal attribution: the two runs used different hosted CPU models. No retry or variance waiver.

CI snapshot 06 is PARTIAL: 42 successful statuses (including CodeRabbit), eight failures, five in progress. Failures include M0, six Windows core shards, and Linux race shard 3. Full available failed-job logs and exact failed-test output are retained. Ordinary source repair is active for review R4/R5, platform handling and observer lifetime. No mergeability or full-suite pass is claimed.

Native checkpoint: actual original selected/anchor request under Q32/1MiB reaches phase 25 after 139 calls, including original retained directory seal/graft and private allocator candidate/certificate. Deferred DATA claims permit a newer foreground durable writer. Exact source-bound controls verify stale refusal, cancellation and reopen preserve the newer value, and late foreign DATA claims cause no partial slot/stat/claim effects. Actual member binding, Publish/Consume/Finish and paused-Finish/newer-writer acceptance remain outstanding. Native Q32 remains RED, ACK=0. This is construction evidence, not native acceptance.

Text/log gzip payloads decompress byte-for-byte to their original files. `manifest.json` binds both compressed and source bytes. Prior failures and all earlier packets are preserved.
