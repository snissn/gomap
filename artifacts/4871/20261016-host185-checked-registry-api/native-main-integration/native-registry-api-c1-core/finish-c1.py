from pathlib import Path
w=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015/native-integrated')
p=w/'TreeDB/lifecycle/registry.go';s=p.read_text()
s=s.replace('''            leaf.owner=nil
            leaf=nil
            *branch=readerBranch{}
            branch=nil
            return 0,hint,r.refund(charge,err)''','''            return 0,hint,r.refundUnusedNodes(&leaf,&branch,charge,err)''')
s=s.replace('''    if used!=charge { *branch=readerBranch{} }
    branch=nil
    if owner==nil || used==charge { return id,h,nil }
    // The unused leaf must have no last alias while the header refunds it.
    // A live root leaf remains charged during the branch-only completion.
    if used==0 { leaf.owner=nil; leaf=nil }
    if err:=r.refund(charge-used,nil); err!=nil {''','''    if used==charge { branch=nil; return id,h,nil }
    // An unused leaf is disposed together with its speculative branch. A root
    // leaf remains charged and reachable during a branch-only completion.
    var refundErr error
    if used==0 { refundErr=r.refundUnusedNodes(&leaf,&branch,charge,nil) } else {
        *branch=readerBranch{}; branch=nil
        refundErr=r.refund(charge-used,nil)
    }
    if err:=refundErr; err!=nil {''')
needle='// refund consumes accounting only; no disposed leaf/branch reaches this'
s=s.replace(needle,'''// refundUnusedNodes is shared by a failed first-hole search and a locked
// fast-path recheck. The caller's actual speculative aliases are cleared before
// entering the surviving header receipt; no leaf-owned completion survives.
func (r *ReaderRegistry) refundUnusedNodes(leaf **readerLeaf, branch **readerBranch, charge uint64, first error) error {
    if *leaf!=nil { **leaf=readerLeaf{}; *leaf=nil }
    if *branch!=nil { **branch=readerBranch{}; *branch=nil }
    return r.refund(charge,first)
}

'''+needle)
p.write_text(s)
p=w/'TreeDB/db/index_gen.go';s=p.read_text();s=s.replace('''	err = cleanupErr
	if err == nil && g.closeCompleted {''','''	err = cleanupErr
    if registryErr:=g.registryCleanupFailureLockedV6(); err==nil { err=registryErr }
	if err == nil && g.closeCompleted {''');p.write_text(s)
p=w/'TreeDB/db/db.go';s=p.read_text().replace('// Failed registry shrink retains its SAME original speculative/disposal leaf\n// through the DB\'s existing cleanup graph. Ordinary capacity refusal has no debt.','// Failed registry shrink retains its SAME original header receipt through the\n// DB cleanup graph. Its generation gate retains the containing header charge.\n// Ordinary capacity refusal without cleanup debt has no retained payload.');p.write_text(s)
p=w/'TreeDB/lifecycle/registry_patricia_v6_test.go';s=p.read_text().replace('''		var failed *readerLeaf
		if h != 0 || !errors.As(err, &failed) || !errors.Is(err, retainedalloc.ErrCapacity) || !r.root.empty() || failed.owner != o || failed.debt != readerLeafCharge()+readerBranchCharge() || o.Bytes() != before+readerLeafCharge() || b.bytes != before+failed.debt {''','''		var failed *ReaderRegistry
        debt:=readerLeafCharge()+readerBranchCharge()
		if h != 0 || !errors.As(err, &failed) || failed!=r || !errors.Is(err, retainedalloc.ErrCapacity) || !r.root.empty() || failed.metadata != o || failed.refundAmount!=readerBranchCharge() || failed.refundDeferred!=readerLeafCharge() || !failed.refundFailed || o.Bytes() != before+readerLeafCharge() || b.bytes != before+debt {''');p.write_text(s)
