from pathlib import Path
# Each route has its original result shape. Capture errors are distinct from
# genuinely missing/invalid state; original guards below remain intact.
routes={
'read_only_prepare.go':['return out, captureErr'],
'flush_span_run_plan.go':['return out, captureErr'],
'fragmentation.go':['return IndexVacuumFreelistDebtSnapshot{}, false','return IndexVacuumTriggerReport{}, captureErr','return nil, captureErr'],
'leaf_generation_maintenance_limits.go':['return captureErr'],
'leaf_generation_pack_rewrite.go':['return 0,0,captureErr'],
'value_log_appender.go':['return nil,captureErr'],
'vlog_gc_incremental.go':['return false,captureErr','return nil,0,valueLogRefIncarnation{},captureErr','return valueLogRefCounts{},0,valueLogRefIncarnation{},captureErr'],
'leaf_generation_plan.go':['return plan,captureErr'],
'conditional_txn.go':['db.conditionalUnregisterTxn(id); return nil,captureErr','return page.LegacyEntryRevision,false,captureErr'],
'negative_filter.go':['return'],
'api.go':['stats["treedb.snapshot.error"] = captureErr.Error(); return stats'],
'vlog_rewrite.go':['return 0,nil,false,captureErr','return nil,captureErr','return stats,captureErr'],
'vlog_rewrite_chunk_plan.go':['return nil,captureErr'],
'compact_storage.go':['return LeafGenerationPlan{},captureErr','state.invalidate(); return captureErr'],
'compact_storage_audit.go':['return nil,captureErr'],
}
import re
for name,returns in routes.items():
 p=Path('TreeDB/db')/name;s=p.read_text();matches=list(re.finditer(r'(\t+)(\w+) := ((?:db|tx\.db)\.AcquireSnapshot\(\))',s))
 if len(matches)!=len(returns):raise Exception((name,len(matches),len(returns)))
 for m,ret in reversed(list(zip(matches,returns))):
  indent,var,call=m.groups();end=m.end();suffix=s[end:]
  # Let original locks go before exposing failure, exactly as before.
  unlock=re.match(r'\n(\t+db\.writeMu\.RUnlock\(\)|\t+db\.runCompactStorageAuditProtectedBasisHook[^\n]*\n)*',suffix)
  if name in ('leaf_generation_plan.go','compact_storage_audit.go'):
   pos=s.index('\n\tif snap ==',end);s=s[:pos]+f'\n{indent}if captureErr != nil {{ {ret} }}'+s[pos:]
   s=s[:m.start()]+f'{indent}{var},captureErr := {call}'+s[end:]
  else:s=s[:m.start()]+f'{indent}{var},captureErr := {call}\n{indent}if captureErr != nil {{ {ret} }}'+s[end:]
 p.write_text(s)
p=Path('TreeDB/db/compact_storage.go');s=p.read_text().replace('indexVacuumLeafGuard = db.AcquireSnapshot()','var captureErr error\nindexVacuumLeafGuard,captureErr = db.AcquireSnapshot()\nif captureErr != nil {return stats,captureErr}');p.write_text(s)
p=Path('TreeDB/db/durable_root_runtime.go');s=p.read_text().replace('var snapshot *Snapshot\n\tif valueLogPublicationLocked','var snapshot *Snapshot\nvar captureErr error\n\tif valueLogPublicationLocked',1).replace('snapshot = db.acquireSnapshotWithValueLogPublicationLockHeld()','snapshot,captureErr = db.acquireSnapshotWithValueLogPublicationLockHeld()',1).replace('snapshot = db.AcquireSnapshot()','snapshot,captureErr = db.AcquireSnapshot()',1).replace('if snapshot == nil {\n\t\t// Command-WAL','if captureErr != nil && (!errors.Is(captureErr,ErrClosed) || db.snapshotViewRO.Load()!=nil) {return nil,captureErr}\nif snapshot == nil {\n\t\t// Command-WAL',1);p.write_text(s)
p=Path('TreeDB/db/vacuum_online.go');s=p.read_text().replace('func (db *DB) startVacuumRecorderWithBaseSnapshot() *Snapshot {','func (db *DB) startVacuumRecorderWithBaseSnapshot() (*Snapshot,error) {').replace('baseSnap := db.AcquireSnapshot()','baseSnap,captureErr := db.AcquireSnapshot()',1).replace('return baseSnap\n','return baseSnap,captureErr\n',1).replace('successor := db.AcquireSnapshot()','successor,captureErr := db.AcquireSnapshot()\nif captureErr!=nil{unlockCutover(false);cleanupNewPager();return captureErr}',1);p.write_text(s)
