from pathlib import Path
p=Path('TreeDB/db/db.go');s=p.read_text()
s=s.replace('func (db *DB) AcquireSnapshot() *Snapshot {','func (db *DB) AcquireSnapshot() (*Snapshot,error) {').replace('func (db *DB) acquireSnapshotWithValueLogPublicationLockHeld() *Snapshot {','func (db *DB) acquireSnapshotWithValueLogPublicationLockHeld() (*Snapshot,error) {')
a=s.index('func (db *DB) AcquireSnapshot()');b=s.index('// SnapshotAllocationSizes',a);r=s[a:b].replace('return nil','return nil, ErrClosed').replace('if !db.captureSnapshotWithValueLogPublicationLockHeld(snap) {','if err := db.captureSnapshotWithValueLogPublicationLockHeld(snap); err != nil {').replace('return nil, ErrClosed\n\t}\n\treturn snap','return nil, err\n\t}\n\treturn snap, nil');s=s[:a]+r+s[b:]
s=s.replace('captureSnapshotWithValueLogPublicationLockHeld(snap *Snapshot) bool','captureSnapshotWithValueLogPublicationLockHeld(snap *Snapshot) error').replace('captureSnapshotForReadWithValueLogPublicationLockHeld(snap *Snapshot, oneShot bool) bool','captureSnapshotForReadWithValueLogPublicationLockHeld(snap *Snapshot, oneShot bool) error')
a=s.index('func (db *DB) captureSnapshotForRead');b=s.index('func (db *DB) captureSnapshotBounded',a);r=s[a:b].replace('return false','return ErrClosed').replace('return true','return nil')
# Registry admission precedes all Manager/Set/root borrows, retaining original
# rootReuse capture fence and the epoch/in-flight exit order.
x=r.index('\tvar registryID int64');y=r.index('\tif state.primaryRoot != nil',x);block=r[x:y]
block=block.replace('registryID, snap.registryShardHint = idx.registry.RegisterWithHint(state.primaryRetentionCommitV5(), snap.registryShardHint)','var registerErr error\nregistryID, snap.registryShardHint, registerErr = idx.registry.RegisterWithHint(state.primaryRetentionCommitV5(), snap.registryShardHint)\nif registerErr != nil { return registerErr }')
# No vlog acquisitions precede the moved block.
block=block.replace('''\t\t\tif vlogNeedsPin && vm != nil {
\t\t\t\t_ = vm.Release(vlogSet)
\t\t\t}
''','')
r=r[:x]+r[y:];x=r.index('\tif vm != nil {');r=r[:x]+block+r[x:]
# Acquisition failures after membership preserve unregister cleanup cause.
r=r.replace('''\t\t\tif registryID != 0 {
\t\t\t\tidx.registry.Unregister(registryID)
\t\t\t}''','''\t\t\tif registryID != 0 { e = errors.Join(e, idx.registry.Unregister(registryID)) }''').replace('''\t\t\treturn ErrClosed
\t\t}
\t}
\tif state.LeafGenerations''','''\t\t\treturn e
\t\t}
\t}
\tif state.LeafGenerations''')
# Manager failure also must drop prior registration.
r=r.replace('if !vm.RetainSnapshotWrapper() {\n\t\t\treturn ErrClosed','if !vm.RetainSnapshotWrapper() {\n\t\t\treturn errors.Join(ErrClosed,idx.registry.Unregister(registryID))').replace('if vm == nil {\n\t\t\treturn ErrClosed','if vm == nil {\n\t\t\treturn errors.Join(ErrClosed,idx.registry.Unregister(registryID))')
s=s[:a]+r+s[b:]
# Existing optional ordinary branch remains growable and propagates cause.
s=s.replace('registryID, snap.registryShardHint = idx.registry.RegisterWithHint(state.primaryRetentionCommitV5(), snap.registryShardHint)','var registerErr error\nregistryID, snap.registryShardHint, registerErr = idx.registry.RegisterWithHint(state.primaryRetentionCommitV5(), snap.registryShardHint)\nif registerErr != nil {db.mu.RUnlock();return nil,registerErr}')
# Stable snapshots follow the checked core API.
s=s.replace('func (db *DB) AcquireStableSnapshot() *Snapshot {','func (db *DB) AcquireStableSnapshot() (*Snapshot,error) {').replace('func (db *DB) acquireStableSnapshotWithMaintenanceLockHeld() *Snapshot {','func (db *DB) acquireStableSnapshotWithMaintenanceLockHeld() (*Snapshot,error) {')
a=s.index('func (db *DB) AcquireStableSnapshot()');b=s.index('func (s *Snapshot) releaseLeafGenerationPins',a);r=s[a:b].replace('return nil','return nil, ErrClosed').replace('snapshot := db.AcquireSnapshot()','snapshot,err := db.AcquireSnapshot()').replace('if snapshot == nil {\n\t\treturn nil, ErrClosed','if err != nil {\n\t\treturn nil, err').replace('return snapshot','return snapshot,nil');s=s[:a]+r+s[b:]
# Ordinary finalizer preserves real registry cleanup error.
s=s.replace('s.idx.registry.Unregister(s.registryID)','err = errors.Join(err,s.idx.registry.Unregister(s.registryID))\ns.registryID = 0')
p.write_text(s)
p=Path('TreeDB/db/pools.go');s=p.read_text().replace('ok := db.captureSnapshotForReadWithValueLogPublicationLockHeld','captureErr := db.captureSnapshotForReadWithValueLogPublicationLockHeld').replace('if !ok {','if captureErr != nil {',1).replace('return nil, ErrClosed\n\t}\n\treturn r, nil','return nil, captureErr\n\t}\n\treturn r, nil',1);p.write_text(s)
p=Path('TreeDB/db/api.go');s=p.read_text();a=s.index('func (db *DB) acquireSnapshotOrErr()');b=s.index('\nfunc ',a+1);r=s[a:b];r=r[:r.index('\tsnap :=')]+ '\treturn db.AcquireSnapshot()\n}\n';s=s[:a]+r+s[b:];p.write_text(s)
