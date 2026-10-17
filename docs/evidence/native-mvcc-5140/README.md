# Native MVCC P1 repair19 evidence

PR: https://github.com/snissn/gomap/pull/5146 . This evidence-only branch is not an implementation PR and uses the repository-supported `[skip ci]` mechanism; required implementation CI remains unchanged.

Published runtime head `d64fba3d88fe15bd89bf086face1981a7d3f6b97`, exact source tree `9a56421e3aca8265a3eb4cd591eb6187f536eb55`; local frozen commit `1fedf4de8f2b8945089dfc814242ca4cd28696f0` has identical source. Original baseline `0149d78dba2574b09d7119d2631ec78a144e088d`.

Read [the repair packet and scoped disposition](repair19.md). [The archive](repair19-evidence.tar.gz) contains 121 raw/provenance files plus its internal manifest. Archive SHA256 `7c5052b8ab22c76d882569f8065b263fa5d77925588c5cb4c33a5ecb4295833f`; the adjacent [manifest](repair19-manifest.json) binds every member.

Normal, race, safe-build and recovery checks pass. The local raw gate passes; enabled and ordinary bounded captures complete. All local PRIMARY attempts stop before samples because of namespace/host-quiet failures, and the unchanged hosted gate remains required. Ordinary timing uncertainty is explicitly accepted only for intermediate P1; final integrated Q remains open. No final-graph qualification or merge readiness is implied.

The immutable [frozen18 archive](https://github.com/snissn/gomap/tree/b726a1c871b1e83dfbad6381df317600658c46ec/docs/evidence/native-mvcc-5140) retains earlier successes, failures, profiles and decisions. Retired executable/derived copies were released after consumers finished; manifests preserve their exact build/source/hash provenance. This archive records actual cleanup, including old entries from the graph-local build cache. Source worktrees, raw evidence and current19 measured executables were preserved.
