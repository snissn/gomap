Adds a safe `vlog-gc` command to delete fully-unreferenced value-log segments.

What changed
- Backend `ValueLogGC` implementation scans user + system trees for value-log pointers and marks unreferenced segments as zombie.
- Public `(*DB).ValueLogGC` wrapper (checkpoints cached mode first).
- Treemap CLI adds `vlog-gc -rw [-dry-run]` with summary output.
- New integration test verifies an unreferenced segment is removed.

How to test
- `GOWORK=off go test ./TreeDB/... -count=1`
- `treemap vlog-gc <db-dir> -rw -dry-run`

Notes
- GC is conservative: it never removes the newest segment per lane, and pinned segments are retained until snapshots drain.
