from pathlib import Path
W=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015/native-integrated')
def sub(n,old,new,count=1):
 p=W/n;s=p.read_text()
 if old not in s and new in s:return
 assert s.count(old)==count,(n,old,s.count(old));p.write_text(s.replace(old,new))
for n,recv in [('TreeDB/snapshot_fastpath_test.go','d'),('TreeDB/caching/snapshot_pool_test.go','cached')]:
 sub(n,f'\t\t\t\tsnap := {recv}.AcquireSnapshot()',f'\t\t\t\tsnap, snapshotErr := {recv}.AcquireSnapshot()\n\t\t\t\tif snapshotErr != nil {{\n\t\t\t\t\tpublishErr(snapshotErr)\n\t\t\t\t\treturn\n\t\t\t\t}}')
sub('TreeDB/caching/cow_cut_test.go','\n\t\ts := db.AcquireSnapshot()','\n\t\ts, snapshotErr := db.AcquireSnapshot()\n\t\tif snapshotErr != nil {\n\t\t\tdone <- snapshotErr\n\t\t\treturn\n\t\t}')
sub('TreeDB/caching/cow_cut_test.go','\t\t\ts := db.AcquireSnapshot()','\t\t\ts, snapshotErr := db.AcquireSnapshot()\n\t\t\tif snapshotErr != nil {\n\t\t\t\tt.Errorf("AcquireSnapshot: %v", snapshotErr)\n\t\t\t\treturn\n\t\t\t}')
sub('TreeDB/caching/cow_cut_test.go','\tif db.AcquireSnapshot() != nil {\n\t\tt.Fatal("errorless capture admitted despite pressure")\n\t}', '\tif snapshot, err := db.AcquireSnapshot(); snapshot != nil || !errors.Is(err, memtable.ErrCOWCapacity) {\n\t\tif snapshot != nil {\n\t\t\tsnapshot.Close()\n\t\t}\n\t\tt.Fatalf("capture pressure=(%v, %v) want nil, ErrCOWCapacity", snapshot, err)\n\t}')
sub('TreeDB/cow_public_close_race_test.go','\t\t\t\t\tif snap := database.AcquireSnapshot(); snap != nil {','\t\t\t\t\tsnap, err := database.AcquireSnapshot()\n\t\t\t\t\tcheck(err)\n\t\t\t\t\tif snap != nil {')
sub('TreeDB/cow_public_close_race_test.go','\t\t\tif snap := database.AcquireSnapshot(); snap != nil {\n\t\t\t\t_ = snap.Close()\n\t\t\t\tt.Fatal("closed public snapshot acquired")\n\t\t\t}', '\t\t\tif snap, err := database.AcquireSnapshot(); snap != nil || !errors.Is(err, ErrClosed) {\n\t\t\t\tif snap != nil {\n\t\t\t\t\t_ = snap.Close()\n\t\t\t\t}\n\t\t\t\tt.Fatalf("closed public snapshot=(%v, %v) want nil, ErrClosed", snap, err)\n\t\t\t}')
sub('TreeDB/algorithm_work_bench_test.go','\t\t\t\tif err := algorithmSnapshotCheckAndClose(d.AcquireSnapshot(), algorithmKey(id*2), id, 1); err != nil {','\t\t\t\tsnapshot, snapshotErr := d.AcquireSnapshot()\n\t\t\t\tif snapshotErr != nil {\n\t\t\t\t\tt.Fatal(snapshotErr)\n\t\t\t\t}\n\t\t\t\tif err := algorithmSnapshotCheckAndClose(snapshot, algorithmKey(id*2), id, 1); err != nil {')
sub('TreeDB/mvcc/native_prune_foreground_test.go','\t\tif snap := db.AcquireSnapshot(); snap != nil {','\t\tsnap, snapshotErr := db.AcquireSnapshot()\n\t\tif snapshotErr != nil {\n\t\t\tfail(snapshotErr)\n\t\t}\n\t\tif snap != nil {')
for typ in ['pruneSnapshotPauseDB','snapshotValueErrorDB']:
 sub('TreeDB/mvcc/versions_test.go',f'func (db *{typ}) AcquireSnapshot() treedb.Snapshot {{\n\tsnapshot := db.DB.AcquireSnapshot()\n\tif snapshot == nil {{\n\t\treturn nil\n\t}}',f'func (db *{typ}) AcquireSnapshot() (treedb.Snapshot, error) {{\n\tsnapshot, err := db.DB.AcquireSnapshot()\n\tif err != nil {{\n\t\treturn nil, err\n\t}}\n\tif snapshot == nil {{\n\t\treturn nil, nil\n\t}}')
sub('TreeDB/mvcc/versions_test.go','return &pruneSnapshotPause{Snapshot: snapshot, owner: db}','return &pruneSnapshotPause{Snapshot: snapshot, owner: db}, nil')
sub('TreeDB/mvcc/versions_test.go','return &snapshotValueErrorSnapshot{Snapshot: snapshot, err: db.err}','return &snapshotValueErrorSnapshot{Snapshot: snapshot, err: db.err}, nil')
sub('TreeDB/caching/snapshot_test.go','func (b *dirtyDuringAcquireBackend) AcquireSnapshot() *db.Snapshot {','func (b *dirtyDuringAcquireBackend) AcquireSnapshot() (*db.Snapshot, error) {')
sub('TreeDB/caching/point_successor_mvcc_test.go','func (b *successorAcquireBarrier) AcquireSnapshot() *backenddb.Snapshot {','func (b *successorAcquireBarrier) AcquireSnapshot() (*backenddb.Snapshot, error) {')
sub('TreeDB/caching/vlog_generation_protected_paths_test.go','interface{ AcquireSnapshot() *backenddb.Snapshot }','interface{ AcquireSnapshot() (*backenddb.Snapshot, error) }',2)
sub('TreeDB/caching/root_domain_probe_stats_test.go','\t\tsnap := &Snapshot{','\t\tbackendSnapshot, err := backend.AcquireSnapshot()\n\t\tif err != nil {\n\t\t\tt.Fatal(err)\n\t\t}\n\t\tsnap := &Snapshot{',3)
sub('TreeDB/caching/root_domain_probe_stats_test.go','backend: backend.AcquireSnapshot(),','backend: backendSnapshot,',3)
sub('TreeDB/caching/snapshot_test.go','\tif snap := cached.AcquireSnapshot(); snap != nil {\n\t\t_ = snap.Close()\n\t\tt.Fatalf("AcquireSnapshot()=%v want nil on rotate failure", snap)\n\t}', '\tif snap, err := cached.AcquireSnapshot(); snap != nil || err == nil {\n\t\tif snap != nil {\n\t\t\t_ = snap.Close()\n\t\t}\n\t\tt.Fatalf("AcquireSnapshot()=(%v, %v) want nil and rotate failure", snap, err)\n\t}')
sub('TreeDB/caching/snapshot_test.go','\tif snap := cached.AcquireBackendSnapshotFastPath(); snap != nil {','\tif snap, err := cached.AcquireBackendSnapshotFastPath(); err != nil {\n\t\tt.Fatalf("AcquireBackendSnapshotFastPath: %v", err)\n\t} else if snap != nil {',4)
# Reassignments already have snapshotErr from their initial checked acquisition.
for n in ['TreeDB/cmd/treemap/compact_endpoint_test.go','TreeDB/collections/api_test.go','TreeDB/collections/vector_partition_split_insert_recovery_v1_test.go','TreeDB/collections/typed_graph_publication_test.go','TreeDB/collections/typed_graph_base_alias_test.go']:
 p=W/n;s=p.read_text();import re
 s=re.sub(r'(?m)^(\s*)(\w+) = (\S+\.AcquireSnapshot\(\))$',lambda m:f'{m[1]}{m[2]}, snapshotErr = {m[3]}\n{m[1]}if snapshotErr != nil {{\n{m[1]}\tt.Fatal(snapshotErr)\n{m[1]}}}',s);p.write_text(s)
sub('TreeDB/collections/vector_index_insert_cache_test.go','currentRoot := currentDBSystemRootForTest4243(d)','currentRoot := currentDBSystemRootForTest4243(t, d)')
sub('TreeDB/collections/vector_index_insert_cache_test.go','func currentDBSystemRootForTest4243(d *backenddb.DB) uint64 {\n\tsnap := d.AcquireSnapshot()', 'func currentDBSystemRootForTest4243(t testing.TB, d *backenddb.DB) uint64 {\n\tt.Helper()\n\tsnap, err := d.AcquireSnapshot()\n\tif err != nil {\n\t\tt.Fatal(err)\n\t}')
sub('TreeDB/collections/vector_partition_paged_source_session_v2_test.go','\tsession := &VectorPartitionPagedSourceSessionV2{collection: c, manifest: m, ownership: ownership, snapshot: d.AcquireSnapshot()}', '\tsnapshot, err := d.AcquireSnapshot()\n\tif err != nil {\n\t\tt.Fatal(err)\n\t}\n\tsession := &VectorPartitionPagedSourceSessionV2{collection: c, manifest: m, ownership: ownership, snapshot: snapshot}')
# Additional checked stable/root capture APIs.
sub('TreeDB/internal/templatedb/stable_resource_capture_test_adapter_test.go','\tsnapshot := kv.db.AcquireStableSnapshot()', '\tsnapshot, err := kv.db.AcquireStableSnapshot()\n\tif err != nil {\n\t\treturn nil, err\n\t}')
sub('TreeDB/production_authority_matrix_test.go','\tsnapshot := database.AcquireStableSnapshot()', '\tsnapshot, err := database.AcquireStableSnapshot()\n\tif err != nil {\n\t\treturn nil, err\n\t}')
sub('TreeDB/cmd/treemap/compact_endpoint_test.go','\t\t\tsnapshot := roots.AcquireSnapshotForRoot(root)', '\t\t\tsnapshot, snapshotErr := roots.AcquireSnapshotForRoot(root)\n\t\t\tif snapshotErr != nil {\n\t\t\t\treturn snapshotErr\n\t\t\t}')
sub('TreeDB/collections/typed_graph_work_epoch_test.go','\t\t\tsnapshot := roots.AcquireSnapshotForRoot(root)', '\t\t\tsnapshot, snapshotErr := roots.AcquireSnapshotForRoot(root)\n\t\t\tif snapshotErr != nil {\n\t\t\t\tt.Fatal(snapshotErr)\n\t\t\t}')
print('manual contextual migrations applied')
