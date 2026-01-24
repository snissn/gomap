PR13: Mode2/3 performance investigation + read-path optimizations

Goal
- Make Mode2 (DisableJournal=1 + value_log pointers) and Mode3 (journal on + value_log pointers) competitive with or faster than Mode1 on the IAVL benchmark without changing thresholds/config defaults.
- Use the Celestia Linux server (192.168.0.185) for profiling and validation.

Scope
- Focus on read-path and read-after-write performance when MemtableValueLogPointers is enabled.
- No changes to ValueLogPointerThreshold, inline thresholds, or benchmark “cheats”.

Validation
- Add a regression test that would have caught the Mode2 forced-sync regression (already fixed in PR12) and add new tests as needed to ensure Mode2 does not regress in read-path behavior.

Perf methodology
- iavl-bench mode scripts: mode1/2/3.
- Collect CPU + mutex/block + trace for Mode1 vs Mode2 vs Mode3 on the Linux server and iterate.

Notes
- PR will be based on PR12 branch.
