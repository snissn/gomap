Final PR5135 source a22078f59bfe9023787d5147c51c989a4010644d differs from measured/reviewed 7702c2a28f9c45266977ec708688717f4e19fd6a only in TreeDB/db/physical_snapshot_cut_v1_test.go.

Both captured parents must be protected at the captured cut. Inactive parent reuse is tested at the actual successor CommitSeq and strictly after it. All32 concurrent writes, delayed copy, bothslot restore/fallback, locks and extent checks remain. Full focused PhysicalSnapshotCutV1 normal suite and exact regression under race PASS; independent Sol6.1 delta ACCEPT. Exact production/module/public benchmark equivalence permits reuse of the repaired public3pair result in parent packet fad5aad. No new timing is claimed.

Current-head required CI/hosted review/merge and complete sprint qualification remain separate live gates. Earlier M0 variance failure is retained and not waived.
