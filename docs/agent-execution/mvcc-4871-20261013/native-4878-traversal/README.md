# Native #4878 lower memtable construction checkpoint

This packet binds the single production immutable memtable Map implementation and its charged resumable traversal to exact Git objects. `manifest.json` records the source-only staged tree, the complete TreeDB runtime subtree, package trees, each changed runtime/harness blob and SHA256, upstream provenance, focused commands and raw diagnostics. The final commit containing this packet is the external publication identity; its TreeDB subtree must match the manifest. This avoids a circular self-hash.

The cursor preserves paused binary search and partial long-key comparisons on the same immutable root. The actual COW adapter uses existing OrdinalScanWork and existing COW owner/admission state. Constructor height comes from maintained cardinality geometry; no opaque Height/Seek runs before admission. All production memtable Map imports share the unchanged upstream mutation/layout implementation.

Focused normal, treedb_safe and race evidence covers ordered/differential behavior, long-key yields, one-record progress, old-root stability under replacement, allocation admission, cancellation and once-owned retirement. The first failed differential diagnostic is preserved with its oracle correction described in the manifest and package README. Caller checks are compilation only.

This is a provisional construction boundary, not issue completion or native qualification. Public pre-effects refusal remains. Existing composite capture, PRIMARY/page/frame, payload work, merge/anchor/ACK and bounded Finish obligations remain open. Cursor close transfers retirement custody; it does not complete physical cleanup or refund history before resource callbacks finish. No new publisher, index, registry, scheduler, storage format or cap reduction is introduced.

All generated test/build consumers have completed. The exact worktree and small scratch diagnostic files remain retained, with identities in the manifest. No donor, fixture, shared cache or other worker source was altered or removed.
