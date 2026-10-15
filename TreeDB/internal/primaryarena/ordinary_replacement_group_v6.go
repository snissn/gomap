package primaryarena

import (
 "bytes"
 "crypto/sha256"
 "math/bits"
 "unsafe"

 "github.com/snissn/gomap/TreeDB/internal/iterator"
 "github.com/snissn/gomap/TreeDB/node"
 "github.com/snissn/gomap/TreeDB/page"
)

// NewReplacementGroupV6 is the ordinary directory producer's checked custody
// construction. It preserves the existing single flat baseline while at most
// two logical classes differ. Every comparison is made on the real old/new
// directory entries, including inline absences and revisions; equal component
// refs alone do not establish equal logical contents. A third distinct change
// uses the original full flat construction, so no unbounded ancestry is kept.
func (a *Arena) NewReplacementGroupV6(previous uint64, base node.PrimaryOperand, sequence uint64, entries []node.PrimaryDirectoryEntry, refs [groupSize]Ref, w *iterator.OrdinalScanWork) (*Group, bool, error) {
 cost := uint64(2*page.PageSize)+4*uint64(unsafe.Sizeof(Group{}))+2*uint64(unsafe.Sizeof(bank{}))+uint64(groupSize)*(2*uint64(unsafe.Sizeof(node.PrimaryDirectoryEntry{}))+2*uint64(unsafe.Sizeof(Ref{})))
 for _, e := range entries { cost += 2*uint64(len(e.Key)) }
 if !reserve(w, 3+groupSize, cost) { return nil, false, nil }
 a.mu.Lock()
 defer a.mu.Unlock()
 a.counters.Bytes += cost
 src := a.slot(previous)
 if src == nil || src.class != Directory || !src.sealed || src.refs == 0 || len(src.image)!=page.PageSize {
  return nil,false,ErrStale
 }
 if !page.VerifyChecksumNonMutating(src.image) || sha256.Sum256(src.image)!=src.digest { return nil,false,ErrFormat }
 // Changed DATA or compaction deliberately establishes a fresh baseline.
 priorBase,priorSequence := src.directory.Base()
 old := src.groups[0]
 if old == nil || priorBase!=base || priorSequence!=sequence {
  return a.newFlatReplacementGroupLockedV6(refs,w)
 }
 if old.owner!=a || old.refs==0 { return nil,false,ErrStale }
 var positions [groupSize]uint8
 for i,e := range entries {
  if int(e.ClassSlot)>=groupSize || positions[e.ClassSlot]!=0 { return nil,false,ErrFormat }
  positions[e.ClassSlot]=uint8(i+1)
  if e.InlineAbsence() {
   if refs[e.ClassSlot]!=(Ref{}) { return nil,false,ErrFormat }
  } else if refs[e.ClassSlot].PageID!=e.Operand.Ref.Page {
   return nil,false,ErrFormat
  }
 }
 mask:=old.overrideMask
 for slot:=0;slot<groupSize;slot++ {
  oldEntry,oldErr:=src.directory.ClassEntry(uint16(slot))
  position:=positions[slot]
  same:=oldErr!=nil && position==0
  if oldErr==nil && position!=0 {
   same=samePrimaryClassEntryV6(oldEntry,entries[position-1])
  }
  if !same { mask|=uint64(1)<<uint(slot) }
 }
 if bits.OnesCount64(mask)>2 { return a.newFlatReplacementGroupLockedV6(refs,w) }
 baseline:=old
 if old.base!=nil { baseline=old.base }
 if baseline.owner!=a || baseline.refs==0 || baseline.base!=nil || baseline.refs==^uint64(0) {
  return nil,false,ErrStale
 }
 count:=uint64(bits.OnesCount64(mask))
 edgeBytes:=2*uint64(unsafe.Sizeof(Group{}))+count*2*uint64(unsafe.Sizeof(bank{}))
 if !reserve(w,2+count,edgeBytes) { return nil,false,nil }
 value:=Group{owner:a,refs:1,base:baseline,overrideMask:mask}
 for slot:=0;slot<groupSize;slot++ {
  if mask&(uint64(1)<<uint(slot))==0 { continue }
  r:=refs[slot]
  if r!=(Ref{}) {
   b:=a.valid(r)
   if b==nil || !b.sealed || b.class!=Component || b.refs==^uint64(0) { return nil,false,ErrStale }
  }
  value.components[slot]=r
 }
 group,err:=a.newGroup(value)
 if err!=nil { return nil,false,err }
 baseline.refs++
 for _,r:=range group.components { if r!=(Ref{}) { a.valid(r).refs++ } }
 a.counters.GroupsCreated++
 a.counters.EdgeReads+=1+count
 a.counters.EdgeWrites+=1+count
 a.counters.Bytes+=edgeBytes
 return group,true,nil
}

func samePrimaryClassEntryV6(a,b node.PrimaryDirectoryEntry) bool {
 return a.Kind==b.Kind && a.Revision==b.Revision && a.ClassSlot==b.ClassSlot && a.Operand==b.Operand && bytes.Equal(a.Key,b.Key)
}

func (a *Arena) newFlatReplacementGroupLockedV6(refs [groupSize]Ref,w *iterator.OrdinalScanWork) (*Group,bool,error) {
 count:=uint64(0)
 for _,r:=range refs { if r!=(Ref{}) { count++ } }
 cost:=2*uint64(unsafe.Sizeof(Group{}))+count*2*uint64(unsafe.Sizeof(bank{}))
 if !reserve(w,1+count,cost) { return nil,false,nil }
 return a.newGroupFromRefsLockedV6(refs,count,cost)
}
