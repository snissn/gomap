# User-Command WAL PR1 Test Migration Inventory

This table is the PR1 repository-local inventory required by tracker #1529. It
maps legacy commit-log tests by invariant, not by old payload compatibility.
Legacy raw payload tests stay active until PR3 converts production raw KV writes,
but PR1 adds typed-frame equivalents for the same durable-byte invariants.

| Existing test | Package | Invariant | Migration bucket | Command-WAL replacement | Disposition |
|---|---|---|---|---|---|
| TestCommitLogWriteReadBatch | TreeDB/internal/commitlog | round-trip multiple raw mutations in one WAL segment | Legacy raw frame encoding and decoding | TestCommandWALFormatGoldenV1RawKVBatch | migrated; legacy test retained until PR3 |
| TestCommitLogWriteReadBatchCompressed | TreeDB/internal/commitlog | physical segment compression preserves payload identity | Legacy raw frame encoding and decoding | TestCommandWALFormatGoldenV1RawKVBatch plus existing compressed segment tests | retained; command frames reuse same segment framing |
| TestCommitLogWriter_LazyCompressionEncoder | TreeDB/internal/commitlog | compression encoder is not initialized on small writes | Legacy raw frame encoding and decoding | existing segment writer behavior covers command frames through AppendCommand | retained |
| TestCommitLogCorruptCRC | TreeDB/internal/commitlog | corrupt complete segment fails closed before decode | Truncated tail and corruption handling | TestCommandWALFormatRejectsHeaderPayloadDigestAndTrailerMismatch | migrated; legacy test retained |
| TestCommitLogAppendBatchRejectsMixedSequence | TreeDB/internal/commitlog | one logical batch cannot carry mixed commit sequence values | typed command frame LSN tests | TestCommandWALDuplicateLSNFailsClosed and TestCommandWALFormatGoldenV1RawKVBatch | migrated to one-frame/one-LSN command identity |
| TestCommitLogTruncatedPayload | TreeDB/internal/commitlog | incomplete terminal payload is not treated as a valid record | Truncated tail and corruption handling | TestCommandWALTerminalShortHeaderIgnored | migrated; command reader returns typed terminal-tail classification |
| FuzzCommitLogReader | TreeDB/internal/commitlog | arbitrary bytes must not panic or allocate past configured limits | typed command frame decoder hardening tests | FuzzCommandWALDecodeFrame | migrated for command frames; legacy fuzz retained until PR3 |
| TestCrashRecovery_WALReplayIsCoherent | TreeDB | raw KV set/delete WAL replay preserves user-visible state after reopen | Raw KV WAL replay | TestCommandWALRawSetDeleteBatchReplaysThroughNormalExecutor | planned PR3 replacement; legacy test retained for non-command-WAL directories |
| TestRecovery_RIDJoinReplaysValueLog | TreeDB | value-log RID fence is satisfied before recovered keys can reference value-log bytes | RID/value-log fence behavior | TestCommandWALRIDFencePreservedForRawKVBatch | planned PR3 replacement; legacy test retained for non-command-WAL directories |
| TestRecovery_PartialCommitBatchIgnored | TreeDB | incomplete terminal commit-log tail cannot publish a partial logical batch | Truncated tail and corruption handling | TestCommandWALOpenAllowsActivePartialFirstFrameTail | planned PR3 replacement; command frames use terminal-tail classification |
| TestReadOnlyDoesNotReplayOrRemoveCommitLog | TreeDB/db | read-only open must not perform mutating replay or remove WAL debt | Read-only open and recovery-required behavior | TestCommandWALReadOnlyOpenWithUnappliedFrameFailsRecoveryRequired | planned PR2 replacement; legacy test retained for non-command-WAL directories |
| TestCachingDB_Checkpoint_TrimsWAL | TreeDB/caching | checkpoint cleanup removes only WAL segments made redundant by durable roots | Checkpoint cleanup | TestCommandWALCheckpointCleansOnlyCoveredSegments | planned PR2 replacement; legacy cached WAL cleanup test retained until cached writes switch to command WAL |
