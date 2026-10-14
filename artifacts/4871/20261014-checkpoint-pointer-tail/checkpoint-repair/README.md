Bounded #5111 public empty-checkpoint basis repair.

Baseline is exact effective frozen BEFORE production census, not HEAD alone. No GE prototype production change is credited. The new public test fails on all four durable/relaxed and inline/pointer rows with stale fresh StateToken plus actual checked Close basis refusal. Retained old cuts remain valid. Candidate normal and race pass; race also confirms original strict backend rejection and existing PRIMARY fallback convergence unchanged.

The only production change is COW-only runCachedBackendMaintenance around existing public fallback refresh. Adaptive route and journal cleanup stage stay as before. No WAL ACK, publication authority, format, cap, or old-cut lifetime contract changes. The spec paragraph documents required existing successor-basis ordering. Final public Close invokes this before cached.Close sets closing.

Lock order: cache maintenance denies admission and drains writeMu before the existing fallback OperationGate.Lock. Public COW Set/SetSync use command-WAL batch; caching Batch.writeRegular obtains beginDirectWrite/writeMu before its append callback can take OperationGate.RLock. writeCOWPublication releases writeMu after cut publication; outer finishPublicCommandWALGroupPublication releases the gate without rejoining cache admission. A fast relaxed gate lease never waits for maintenance admission. Existing maintenance-writer and forming-group/cutover controls cover these boundaries.

Scope remains focused normal/race evidence; no full all19, performance, hosted CI/review, GH mutation, merge, cleanup, or source edits to protected ordinary/native/prototype worktrees occurred. Candidate is frozen for root read-only transfer/integration. No owned test subprocess remains.
