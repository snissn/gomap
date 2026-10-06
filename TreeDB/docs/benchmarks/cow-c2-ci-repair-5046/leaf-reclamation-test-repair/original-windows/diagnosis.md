# Exact440 Windows-core-3 failure: read-only diagnosis

The only failed leaf is `TestCOWPublicEmptyCheckpointLeafRegistrationProgress/command_wal_relaxed` at line319 (3.47s). The parent and TreeDB package also emit failure events. The assertion required an unused rotated gen2 leaf segment to be absent immediately after ordinary `CompactStorage`; `os.Stat` succeeded. Durable and `no_wal_fast` leaves passed.

Both repaired synchronous Close-notification profile leaves passed (durable7.86s, relaxed7.73s; parent15.59s). The separate asynchronous Close-hook witness has no event in this job artifact; no result is claimed for it. This is a distinct failure.

The captured compaction report marks generation2 `deleted`, with `PinnedCount:1`, while GC reports one eligible generation and zero physically deleted files/generations. Source at the exact hosted head permits marked-zombie segments to remain while Set/identity pins exist and retries some blocked deletions. The assertion still fails before the later-write/reopen portion of this leaf; those operations were not executed in the failed leaf. The log does not identify the remaining pin owner, a Windows sharing violation, or eventual deletion. No test-flake/runtime-defect classification is established.

Provenance: run37441320552, job112196498661, head4403776d10b6ac82eb9e1839f437f59a78058816; artifact11402094450 `treedb-test-json-windows-core-3` created2026-10-06T09:45:00Z, job completed09:45:05Z. Raw log/zip/JSON and API metadata are retained with SHA256 bindings. Only artifact files were written; no source edits, reruns, CI changes or Linux jobs. Full raw logs remain private.
