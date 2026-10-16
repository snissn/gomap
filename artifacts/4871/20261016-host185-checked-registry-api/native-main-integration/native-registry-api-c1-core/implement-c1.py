from pathlib import Path
W=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015/native-integrated')
p=W/'TreeDB/lifecycle/registry.go';s=p.read_text();a=s.index('// The actual speculative leaf');b=s.index('func (n readerNode) empty()',a)
s=s[:a]+'''// Leaves contain only canonical membership; their disposal runs in the
// surviving original registry header, never in the allocation being refunded.
type readerLeaf struct {
 parent *readerBranch
 handle, sequence uint64
 owner *retainedalloc.Owner
}

// The header is already admitted by its original generation/scope. Its fixed
// receipt owns accounting only after all disposed node aliases have ended.
func (r *ReaderRegistry) Error() string { return "reader registration accounting cleanup incomplete" }
func (r *ReaderRegistry) Is(target error) bool {
 r.failureMu.Lock(); causes := r.refundCauses; r.failureMu.Unlock()
 for _, cause := range causes { if cause != nil && errors.Is(cause,target) { return true } }; return false
}
func (r *ReaderRegistry) As(target any) bool {
 r.failureMu.Lock(); causes := r.refundCauses; r.failureMu.Unlock()
 for _, cause := range causes { if cause != nil && errors.As(cause,target) { return true } }; return false
}
func (r *ReaderRegistry) OriginalCleanupPriorAvailable() bool { r.failureMu.Lock(); defer r.failureMu.Unlock(); return r.refundCauses[2]==nil }
func (r *ReaderRegistry) TryLinkPriorCleanupError(prior error) bool {
 r.failureMu.Lock(); defer r.failureMu.Unlock()
 if prior==nil || prior==r || r.refundCauses[2]!=nil { return false }; r.refundCauses[2]=prior; return true
}
func (r *ReaderRegistry) LinkPriorCleanupError(prior error) { r.TryLinkPriorCleanupError(prior) }
func ReaderRegistryCleanupFailed(err error) bool { var r *ReaderRegistry; return errors.As(err,&r) }

// OriginalCleanupError joins an in-flight ordinary refund. No containing
// header completion may pass an unfinished receipt or a later detached debt.
func (r *ReaderRegistry) OriginalCleanupError() error {
 if r==nil { return nil }; r.refundMu.Lock(); defer r.refundMu.Unlock()
 if r.refundAmount!=0 || r.refundDeferred!=0 || r.refundFailed { return r }; return nil
}
''' +s[b:]
s=s.replace('type ReaderRegistry struct {\n','''type ReaderRegistry struct {
 // Never hold mu while entering Owner. This independent serialization leaves
 // minimum queries and fast unregister available to governing callbacks.
 refundMu sync.Mutex
 refundWork retainedalloc.RemovalWork
 refundAmount, refundDeferred uint64
 refundFailed bool
 failureMu sync.Mutex
 refundCauses [3]error
''')
s=s.replace('''			r.mu.Unlock()
			branch = nil
			return 0, hint, leaf.refund(charge, err, true)''','''			r.mu.Unlock()
            leaf.owner=nil
            leaf=nil
            *branch=readerBranch{}
            branch=nil
            return 0,hint,r.refund(charge,err)''')
s=s.replace('''	r.mu.Unlock()
	branch = nil
	if owner == nil || used == charge {
		return id, h, nil
	}
	if err := leaf.refund(charge-used, nil, true); err != nil {''','''	r.mu.Unlock()
    if used!=charge { *branch=readerBranch{} }
    branch=nil
    if owner==nil || used==charge { return id,h,nil }
    // The unused leaf must have no last alias while the header refunds it.
    // A live root leaf remains charged during the branch-only completion.
    if used==0 { leaf.owner=nil; leaf=nil }
    if err:=r.refund(charge-used,nil); err!=nil {''')
s=s.replace('''			leaf.debt += extra''','''            leaf.owner=nil
            leaf=nil
            // The first failed header receipt is not overwritten. This exact
            // subsequent detached charge stays in its fixed deferred scalar.
            r.refund(extra,nil)''')
a=s.index('func (l *readerLeaf) refund(');b=s.index('func (r *ReaderRegistry) firstHoleLocked()',a)
s=s[:a]+'''// refund consumes accounting only; no disposed leaf/branch reaches this
// method. The original header survives every Owner callback and receipt access.
func (r *ReaderRegistry) refund(amount uint64, first error) error {
 if r.metadata==nil || amount==0 { return first }
 r.refundMu.Lock(); defer r.refundMu.Unlock()
 if r.refundFailed {
  // All deltas were separately admitted to the same owner before allocation;
  // their sum cannot exceed its original uint64 pending domain. Never wrap.
  if amount > math.MaxUint64-r.refundDeferred { return r }
  r.refundDeferred+=amount
  return r
 }
 r.refundAmount=amount
 fail:=func(cause error) error {
  r.refundFailed=true
  r.failureMu.Lock(); r.refundCauses[0],r.refundCauses[1]=first,cause; r.failureMu.Unlock()
  r.metadata.CleanupFailed()
  return r
 }
 if _,err:=r.refundWork.BindPendingOriginal(r.metadata,amount); err!=nil { return fail(err) }
 for { done,err:=r.refundWork.ContinueOriginal(); if err!=nil { return fail(err) }; if done { break } }
 if _,err:=r.refundWork.ResetCompletedOriginal(); err!=nil { return fail(err) }
 r.refundAmount=0
 return first
}
''' +s[b:]
s=s.replace('''	l := n.leaf
	charge := r.detachLeafLocked(l)
	r.mu.Unlock()
	return l.refund(charge, nil, false)''','''    l:=n.leaf
    charge:=r.detachLeafLocked(l)
    l.owner=nil
    l=nil
    n=readerNode{}
    r.mu.Unlock()
    return r.refund(charge,nil)''')
p.write_text(s)
p=W/'TreeDB/lifecycle/registry_unregister_work_v6.go';s=p.read_text().replace('\tl.debt = 0\n','').replace('\tl.causes = [3]error{}\n','');p.write_text(s)
