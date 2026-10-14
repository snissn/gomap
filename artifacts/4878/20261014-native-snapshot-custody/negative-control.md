# Causal negative control

The initial inverse-old-order control using only repeated Manager.Close after initial Close completion was GREEN (`ok github.com/snissn/gomap/TreeDB/internal/valuelog 0.002s`, session 74265). This is a negative result: closeDone returns without re-entering original terminal metadata refund. It does not demonstrate the defect. That first log was superseded during causal fixture refinement; the retained command outcome is recorded here explicitly.

The separate retained direct under-mu refund RED establishes only the narrow containing invariant. red-initial-close-manager.log establishes the reachable original initial Close completion sequence: a real worker holds retryWorkers.Wait while native final File alias clears, then its completion lets the original Close body reach its original terminal refund. The inverse-old-order version FAILS; the candidate focused normal.log passes. No whole public prune or general concurrency qualification is claimed.
