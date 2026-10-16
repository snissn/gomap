from pathlib import Path
W=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015/native-integrated')
p=W/'TreeDB/db/index_gen.go';s=p.read_text();needle='''		if g.primaryConstructionAllocator != nil {''';s=s.replace(needle,'''        // Failed registration need not have produced any Snapshot borrow.
        // Join the original header before dropping even the physical edge.
        g.wrapperMu.Lock()
        registryErr:=g.registryCleanupFailureLockedV6()
        g.wrapperMu.Unlock()
        if registryErr!=nil { return }
'''+needle,1)
s=s.replace('''	metadata, charge = g.detachGenerationMetadataLockedV6()
	g.wrapperMu.Unlock()
	return metadata, charge, nil''','''    metadata,charge,err=g.detachGenerationMetadataLockedV6()
    g.wrapperMu.Unlock()
    return metadata,charge,err''')
s=s.replace('metadata, charge = g.detachGenerationMetadataLockedV6()','metadata, charge, err = g.detachGenerationMetadataLockedV6()')
s=s.replace('func (g *indexGen) detachGenerationMetadataLockedV6() (*retainedalloc.Owner, uint64) {','''// Caller holds wrapperMu. The same closeFailure retains the original payload;
// a registry error alone is never permission to refund its containing header.
func (g *indexGen) registryCleanupFailureLockedV6() error {
    if g.registry==nil { return nil }
    cause:=g.registry.OriginalCleanupError()
    if cause==nil { return nil }
    if g.closeErr==nil {
        g.closeErr=g.originalCloseFailure(cause)
        if !g.physicalCompleted && g.primaryOwner!=nil {
            g.primaryOwner.retainFailedDataGeneration(g)
        } else if g.offlineScopePhysicalOwner!=nil && !g.offlineScopeRetained {
            g.offlineScopePhysicalOwner.retainFailedDataGeneration(g)
            g.offlineScopeRetained=true
        }
    } else { g.closeFailure.retainCause(1,cause); g.closeErr=&g.closeFailure }
    if g.metadata!=nil { g.metadata.CleanupFailed() }
    if g.stableNamespaceMetadata!=nil { g.stableNamespaceMetadata.CleanupFailed() }
    return g.closeErr
}

func (g *indexGen) detachGenerationMetadataLockedV6() (*retainedalloc.Owner, uint64,error) {
    if err:=g.registryCleanupFailureLockedV6(); err!=nil { return nil,0,err }''')
s=s.replace('''		return nil, 0
	}
	metadata, charge := g.metadata, g.metadataCharge''','''        return nil,0,nil
    }
    metadata,charge:=g.metadata,g.metadataCharge''')
s=s.replace('''	return metadata, charge
}''','''    return metadata,charge,nil
}''')
p.write_text(s)
p=W/'TreeDB/db/primary_offline_pair_v6.go';s=p.read_text().replace('''	physicalErr := idx.close()
	idx.stableNamespaceMu.Lock()''','''    physicalErr:=idx.close()
    // A late header failure must not become a successful aggregate scope
    // completion merely because the earlier physical call already returned.
    idx.wrapperMu.Lock()
    registryErr:=idx.registryCleanupFailureLockedV6()
    idx.wrapperMu.Unlock()
    if registryErr!=nil { physicalErr=registryErr }
    idx.stableNamespaceMu.Lock()''',1);p.write_text(s)
p=W/'TreeDB/db/original_startup_generation.go';s=p.read_text();a=s.index('\tcustody := g.startupCustody\n',s.index('func (g *indexGen) finishStartupFailure'));b=s.index('\n}\n',a)
s=s[:a]+'''    metadata,charge,err:=g.completeStartupGenerationMetadataV6(operation)
    return metadata,charge,err''' +s[b:]
s=s.replace('''	g.finishStartupSuccess()
	metadata, charge, err := g.detachGenerationMetadataCompletionV6()
	g = nil
	return metadata, charge, errors.Join(operation, err)''','''    return g.completeStartupGenerationMetadataV6(operation)''')
# It is necessary to temporarily withdraw the startup gate, but refusal restores
# every exact original alias before completing the ORIGINAL custody as failed.
s+='''
func (g *indexGen) completeStartupGenerationMetadataV6(operation error) (*retainedalloc.Owner,uint64,error) {
    source,manager,lock,custody:=g.startupDB,g.startupManager,g.startupLock,g.startupCustody
    constructed:=g.startupConstructed
    g.startupDB,g.startupManager,g.startupLock,g.startupCustody=nil,nil,nil,nil
    g.startupConstructed=false
    metadata,charge,err:=g.detachGenerationMetadataCompletionV6()
    if err!=nil {
        g.startupDB,g.startupManager,g.startupLock,g.startupCustody=source,manager,lock,custody
        g.startupConstructed=constructed
        g.startupReceipt.cause=errors.Join(operation,err,ErrRecoveryRequired)
        g.startupReceipt.generation=g
        if custody!=nil { custody.Complete(g,&g.startupReceipt) }
        if g.metadata!=nil { g.metadata.CleanupFailed() }
        return nil,0,&g.startupReceipt
    }
    g=nil
    if custody!=nil { custody.Complete(nil,nil) }
    return metadata,charge,operation
}
''';p.write_text(s)
