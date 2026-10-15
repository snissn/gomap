package primaryarena

import (
 "bytes"
 "crypto/sha256"
 "math/bits"
 "unsafe"

 "github.com/snissn/gomap/TreeDB/internal/iterator"
 "github.com/snissn/gomap/TreeDB/internal/retainedalloc"
 "github.com/snissn/gomap/TreeDB/node"
 "github.com/snissn/gomap/TreeDB/page"
 "github.com/snissn/gomap/TreeDB/pager"
)

// SealConstructedMaterializedGraftV6 replaces the captured logical contents
// with their caller-validated materialized DATA output, retaining only actual
// logical changes since capture. The ordinary producer's genuine shared Group
// baseline proves which classes can have changed. Neither an equal physical
// file nor an old directory position establishes that provenance.
//
// The caller holds the fresh publication cut, proves the selected deletion and
// the DATA output, and must consume this SAME constructor in that cut. A new
// flat baseline (materialization, flattening, or a third distinct override)
// refuses before effects; it cannot authorize dropping an unknown component.
func (a *Arena) SealConstructedMaterializedGraftV6(destination Ref, captured, current *ReadRootCertificateV6, base node.PrimaryOperand, sequence uint64, scratch []byte, owner ReadRootConstructionV6, w *iterator.OrdinalScanWork) (*ReadRootCertificateV6, bool, error) {
 if a==nil || w==nil || owner==nil || captured==nil || current==nil { return nil,false,ErrStale }
 cost:=uint64(10*page.PageSize)+6*uint64(unsafe.Sizeof(bank{}))+4*uint64(unsafe.Sizeof(Group{}))+2*uint64(unsafe.Sizeof(ReadRootCertificateV6{}))
 // Three actual bank/certificate operands, two source image CRC/SHA checks,
 // and one canonical output-image encode/digest operand.
 if !w.Reserve(6,cost) { return nil,false,nil }
 a.mu.Lock()
 defer a.mu.Unlock()
 dst,old,now:=a.valid(destination),a.valid(captured.ref),a.valid(current.ref)
 if !a.capsuleFormat || dst==nil || dst.class!=Directory || dst.sealed || dst.construction!=owner || Local(destination.PageID)<pager.PrimaryReadRootLocalBaseV6 || len(scratch)!=page.PageSize || sequence==0 {
  return nil,false,ErrStale
 }
 for _,pair:=range [...]struct{bank *bank; cert *ReadRootCertificateV6}{{old,captured},{now,current}} {
  b,c:=pair.bank,pair.cert
  if c.arena!=a || b==nil || !b.sealed || b.class!=Directory || b.refs==0 || b.readCertificate!=c || len(b.image)!=page.PageSize { return nil,false,ErrStale }
  if !page.VerifyChecksumNonMutating(b.image) || sha256.Sum256(b.image)!=c.digest { return nil,false,ErrFormat }
 }
 oldBase,oldSeq:=old.directory.Base()
 nowBase,nowSeq:=now.directory.Base()
 if oldBase!=nowBase || oldSeq!=nowSeq { return nil,false,ErrStale }
 oldGroup,nowGroup:=old.groups[0],now.groups[0]
 baseline:=func(g *Group)*Group { if g!=nil && g.base!=nil { return g.base }; return g }
 if baseline(oldGroup)!=baseline(nowGroup) { return nil,false,ErrNeedsConsolidation }
 var mask uint64
 for _,g:=range [...]*Group{oldGroup,nowGroup} {
  if g==nil { continue }
  if g.owner!=a || g.refs==0 || bits.OnesCount64(g.overrideMask)>2 || (g.base!=nil && (g.base.owner!=a || g.base.refs==0 || g.base.base!=nil)) { return nil,false,ErrStale }
  mask|=g.overrideMask
 }
 // An already overlaid capture is compared to CURRENT logical entries, not
 // blindly treated as the flat baseline. At most four actual class operands.
 var entries [4]node.PrimaryDirectoryEntry
 var refs [groupSize]Ref
 count:=0
 for mask!=0 {
  slot:=bits.TrailingZeros64(mask)
  mask &^=uint64(1)<<uint(slot)
  if !w.Reserve(1,4*uint64(unsafe.Sizeof(node.PrimaryDirectoryEntry{}))+4*page.PageSize) { return nil,false,nil }
  before,be:=old.directory.ClassEntry(uint16(slot))
  after,ae:=now.directory.ClassEntry(uint16(slot))
  if be!=nil && ae!=nil { continue }
  if be==nil && ae==nil && samePrimaryClassEntryV6(before,after) { continue }
  // The maintained same-base ordinary producer preserves class/key identity.
  // Losing/reassigning a captured key requires a rebase, never resurrection.
  if ae!=nil || (be==nil && !bytes.Equal(before.Key,after.Key)) { return nil,false,ErrNeedsConsolidation }
  var ref Ref
  if !after.InlineAbsence() {
   if nowGroup==nil { return nil,false,ErrStale }
   ref=nowGroup.componentAt(slot)
   b:=a.valid(ref)
   if !w.Reserve(1,4*page.PageSize+2*uint64(unsafe.Sizeof(bank{}))) { return nil,false,nil }
   if b==nil || b.refs==0 || b.refs==^uint64(0) || !b.sealed || b.class!=Component || ref.PageID!=after.Operand.Ref.Page || sha256.Sum256(b.image)!=after.Operand.Digest || node.ValidatePrimaryComponent(after,b.image)!=nil { return nil,false,ErrFormat }
  }
  // Small caller-owned sorted storage; every key comparison is admitted.
  at:=count
  for at>0 {
   if !w.Reserve(1,2*uint64(len(entries[at-1].Key)+len(after.Key))) { return nil,false,nil }
   if bytes.Compare(entries[at-1].Key,after.Key)<0 { break }
   entries[at]=entries[at-1]
   refs[at]=refs[at-1]
   at--
  }
  entries[at],refs[at]=after,ref
  count++
 }
 for i:=0;i<count;i++ { entries[i].ClassSlot=uint16(i) }
 if !w.Reserve(uint64(count)+1,uint64(count)*2*uint64(unsafe.Sizeof(bank{}))+2*uint64(unsafe.Sizeof(Group{}))) { return nil,false,nil }
 if err:=node.EncodePrimaryDirectory(scratch,destination.PageID,sequence,base,entries[:count]);err!=nil { return nil,false,err }
 view,err:=node.DecodePrimaryDirectory(scratch)
 if err!=nil { return nil,false,err }
 certCharge:=retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(ReadRootCertificateV6{})))
 if err=a.metadata.Add(certCharge);err!=nil { return nil,false,err }
 var group *Group
 if count>0 {
  group,err=a.newGroup(Group{owner:a,refs:1,components:refs})
  if err!=nil { a.metadata.Remove(certCharge);return nil,false,err }
 }
 physical,ready,err:=a.writeBundleLocked(destination,scratch,nil,w)
 if !ready || err!=nil {
  a.metadata.Remove(certCharge)
  if group!=nil { a.metadata.Remove(retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(Group{})))) }
  return nil,ready,err
 }
 // Only fixed transfers remain after actual immutable image registration.
 if group!=nil {
  for _,r:=range group.components { if r!=(Ref{}) { a.valid(r).refs++;a.counters.EdgeReads++;a.counters.EdgeWrites++ } }
  a.counters.GroupsCreated++
 }
 dst.groups=[groupCount]*Group{group}
 dst.image=physical
 dst.directory=view.WithPhysicalImage(physical)
 dst.digest=sha256.Sum256(physical)
 dst.sealed,dst.privatePublication=true,false
 dst.metadataCharge+=certCharge
 dst.readCertificate=&ReadRootCertificateV6{arena:a,ref:destination,directory:dst.directory,image:physical,digest:dst.digest}
 a.counters.PagesWritten++
 a.counters.Bytes+=cost
 return dst.readCertificate,true,nil
}
