from pathlib import Path
w=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015/native-integrated')
p=w/'TreeDB/db/index_gen.go';s=p.read_text()
s=s.replace('failedSnapshots   *Snapshot','''failedSnapshots   *Snapshot
    // Only wrapperMu accesses these fixed terminal/registry handoff flags.
    registryFailureLatched bool
    primaryEdgeReleased bool
    failedPhysicalEdgeRetained bool''')
a=s.index('func (g *indexGen) close() error {');b=s.index('// The terminal bit',a)
part=s[a:b].replace('g.closeOnce.Do(func() {','g.closeOnce.Do(func() {\n        var closeErr error').replace('g.closeErr','closeErr')
part=part.replace('''			g.wrapperMu.Lock()
			if closeErr != nil {''','''			g.wrapperMu.Lock()
            // The physical body owns closeErr locally. A concurrent failed
            // unregister only latches the independent original registry cause.
            if registryErr:=g.registryCleanupFailureLockedV6(); registryErr!=nil {
                g.closeFailure.retainCause(0,closeErr)
                closeErr=registryErr
            }
            g.closeErr=closeErr
			if closeErr != nil {''',1)
part=part.replace('''				closeErr = g.originalCloseFailure(closeErr)''','''				g.closeErr = g.originalCloseFailure(closeErr)''',1)
part=part.replace('''					g.primaryOwner.retainFailedDataGeneration(g)''','''                    g.retainFailedPhysicalEdgeV6()''')
part=part.replace('''				g.primaryOwner.retainFailedDataGeneration(g)''','''                g.retainFailedPhysicalEdgeV6()''')
old='''			if closeErr != nil {
				// Preserve this generation's EXISTING owner edge.'''
new='''            // Linearize the original physical-owner handoff against late
            // registry failure. No header callback can latch between this
            // check and the existing release; later Snapshot failure owns its
            // separate, already retained physical edge.
            g.wrapperMu.Lock()
            if registryErr:=g.registryCleanupFailureLockedV6(); registryErr!=nil {
                g.closeFailure.retainCause(0,closeErr)
                closeErr=registryErr
            }
			if closeErr != nil {
				// Preserve this generation's EXISTING owner edge.'''
assert old in part;part=part.replace(old,new)
# Last block's helper already holds wrapperMu.
q=part.index('// Linearize the original physical-owner handoff')
head,tail=part[:q],part[q:]
tail=tail.replace('g.retainFailedPhysicalEdgeV6()','g.retainFailedPhysicalEdgeLockedV6()')
tail=tail.replace('''				_, debt, err := g.primaryOwner.releaseWithOutcome(nil)''','''				_, debt, err := g.primaryOwner.releaseWithOutcome(nil)
                g.primaryEdgeReleased=true''')
tail=tail.replace('''			}
		}
	})
	return closeErr''','''			}
            g.wrapperMu.Unlock()
		}
	})
    g.wrapperMu.Lock()
    defer g.wrapperMu.Unlock()
	return g.closeErr''')
part=head+tail
s=s[:a]+part+s[b:]
a=s.index('func (g *indexGen) registryCleanupFailureLockedV6() error {');b=s.index('func (g *indexGen) detachGenerationMetadataLockedV6()',a)
s=s[:a]+'''func (g *indexGen) registryCleanupFailureLockedV6() error {
    if g.registry==nil { return nil }
    cause:=g.registry.OriginalCleanupError()
    if cause==nil { return nil }
    // closeErr belongs to the closeOnce body until its terminal locked merge.
    // Never assign it from a concurrently completing Snapshot.
    g.closeFailure.retainCause(1,cause)
    if !g.registryFailureLatched {
        g.registryFailureLatched=true
        if !g.physicalCompleted && g.primaryOwner!=nil {
            g.retainFailedPhysicalEdgeLockedV6()
        } else if g.offlineScopePhysicalOwner!=nil && !g.offlineScopeRetained {
            g.offlineScopePhysicalOwner.retainFailedDataGeneration(g)
            g.offlineScopeRetained=true
        }
    }
    if g.metadata!=nil { g.metadata.CleanupFailed() }
    if g.stableNamespaceMetadata!=nil { g.stableNamespaceMetadata.CleanupFailed() }
    return &g.closeFailure
}

func (g *indexGen) retainFailedPhysicalEdgeV6() {
    g.wrapperMu.Lock()
    g.retainFailedPhysicalEdgeLockedV6()
    g.wrapperMu.Unlock()
}

// wrapperMu protects the SAME preexisting physical edge, including deduplication
// with a concurrent registry failure. No edge is recreated after its release.
func (g *indexGen) retainFailedPhysicalEdgeLockedV6() {
    if g.primaryOwner!=nil && !g.primaryEdgeReleased && !g.failedPhysicalEdgeRetained {
        g.failedPhysicalEdgeRetained=true
        g.primaryOwner.retainFailedDataGeneration(g)
    }
}

'''+s[b:]
p.write_text(s)
p=w/'TreeDB/lifecycle/registry.go';s=p.read_text().replace('''// All deltas were separately admitted to the same owner before allocation;
		// their sum cannot exceed its original uint64 pending domain. Never wrap.''','''// Each deferred delta is still pending in this SAME original owner:
        // AddPending admitted it before allocation, and no other consumer can
        // refund a detached node. Thus deferred+amount <= owner.pending <=
        // MaxUint64, even if the FAILED active delta was already debited.
        // The guard is defensive; no amount is discarded on a reachable path.''');p.write_text(s)
