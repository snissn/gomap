# User-Command WAL Test Migration Inventory

This table is the repository-local inventory required by tracker #1529. It
maps legacy commit-log tests by invariant, not by old payload compatibility.
Legacy raw payload tests stay active for non-command-WAL directories. Command
WAL mode maps the same invariants to typed `RawKVBatch` frames.

| Existing test | Package | Invariant | Migration bucket | Command-WAL replacement | Disposition |
|---|---|---|---|---|---|
| TestCommitLogWriteReadBatch | TreeDB/internal/commitlog | round-trip multiple raw mutations in one WAL segment | Legacy raw frame encoding and decoding | TestCommandWALFormatGoldenV1RawKVBatch | migrated; legacy test retained until PR3 |
| TestCommitLogWriteReadBatchCompressed | TreeDB/internal/commitlog | physical segment compression preserves payload identity | Legacy raw frame encoding and decoding | TestCommandWALFormatGoldenV1RawKVBatch plus existing compressed segment tests | retained; command frames reuse same segment framing |
| TestCommitLogWriter_LazyCompressionEncoder | TreeDB/internal/commitlog | compression encoder is not initialized on small writes | Legacy raw frame encoding and decoding | existing segment writer behavior covers command frames through AppendCommand | retained |
| TestCommitLogCorruptCRC | TreeDB/internal/commitlog | corrupt complete segment fails closed before decode | Truncated tail and corruption handling | TestCommandWALFormatRejectsFrameCRCMismatch | migrated; legacy test retained |
| TestCommitLogAppendBatchRejectsMixedSequence | TreeDB/internal/commitlog | one logical batch cannot carry mixed commit sequence values | typed command frame LSN tests | TestCommandWALDuplicateLSNFailsClosed and TestCommandWALFormatGoldenV1RawKVBatch | migrated to one-frame/one-LSN command identity |
| TestCommitLogTruncatedPayload | TreeDB/internal/commitlog | incomplete terminal payload is not treated as a valid record | Truncated tail and corruption handling | TestCommandWALTerminalShortHeaderIgnored | migrated; command reader returns typed terminal-tail classification |
| FuzzCommitLogReader | TreeDB/internal/commitlog | arbitrary bytes must not panic or allocate past configured limits | typed command frame decoder hardening tests | FuzzCommandWALDecodeFrame | migrated for command frames; legacy fuzz retained for non-command-WAL directories |
| TestCrashRecovery_WALReplayIsCoherent | TreeDB | raw KV set/delete replay through backend executor | Raw KV WAL replay | TestCommandWALRawSetDeleteBatchReplaysThroughNormalExecutor, TestCommandWALCrashAfterFrameBeforeRootPublishRecovers | migrated for direct backend command-WAL mode; legacy cached WAL test retained for non-command-WAL directories |
| TestRecovery_RIDJoinReplaysValueLog | TreeDB | value-log RID fence must be satisfied before publishing recovered key pointers | RID/value-log fence behavior | TestCommandWALRIDFencePreservedForRawKVBatch, TestCommandWALMissingRIDFenceFailsRecovery, TestCommandWALExistingRIDFenceTestsMappedToExternalRefFence | migrated for command-WAL RawKVBatch SetRID payloads |
| TestRecovery_PartialCommitBatchIgnored | TreeDB | incomplete terminal commit-log tail must not publish a partial logical batch | Truncated tail and corruption handling | TestCommandWALOpenAllowsActivePartialFirstFrameTail, TestCommandWALOpenFailsClosedOnNonActivePartialFirstFrameTail | migrated for typed command frames |
| TestReadOnlyDoesNotReplayOrRemoveCommitLog | TreeDB | read-only open must not perform mutating replay or cleanup | Read-only open/recovery-required behavior | TestCommandWALReadOnlyOpenWithUnappliedFrameFailsRecoveryRequired, TestCommandWALReadOnlyOpenAllowsFramesCoveredByAppliedLSN | migrated for command-WAL dirty/covered detection |
| TestCachingDB_Checkpoint_TrimsWAL | TreeDB/caching | checkpoint cleanup removes only WAL segments made redundant by durable roots | Checkpoint cleanup | TestCommandWALCheckpointCleanupDeletesOnlyCoveredSegments, TestCommandWALCheckpointCleanupRetainsActiveCoveredSegment | migrated for command-WAL covered-segment cleanup; legacy cached WAL cleanup test retained until cached writes switch to command WAL |
