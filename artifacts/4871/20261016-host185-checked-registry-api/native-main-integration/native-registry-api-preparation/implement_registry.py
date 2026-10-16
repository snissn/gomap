from pathlib import Path
p=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015/native-integrated/TreeDB/lifecycle/registry.go')
s=p.read_text();tail=s[s.index('func (r *ReaderRegistry) loadFastMin'):]
head='''package lifecycle
import (
 "math"
 "math/bits"
 "sync"
 "sync/atomic"
 "unsafe"
 "github.com/snissn/gomap/TreeDB/internal/iterator"
 "github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)
const (
 fastReaderShardCount=16
 fastReaderShardMask=fastReaderShardCount-1
 FastReaderShardMask=fastReaderShardMask
 FastReaderShardCount=fastReaderShardCount
 registerHintUnset=-1
)
type readerRegistryShard struct {seq atomic.Uint64;count atomic.Int32;_ [52]byte}
type readerNode struct {leaf *readerLeaf;branch *readerBranch}
type readerSummary struct {minimum,lo,hi,count uint64}
// Branch allocation/lifetime is independent of the first leaf inserting it.
type readerBranch struct {parent *readerBranch;child [2]readerNode;summary readerSummary;bit uint8}
// The actual speculative leaf contains its original fixed failure receipt.
// Failure does not allocate another error, registry, owner, or cleanup keeper.
type readerLeaf struct {
 parent *readerBranch
 handle,sequence uint64
 owner *retainedalloc.Owner
 debt uint64
 removal retainedalloc.RemovalWork
 causes [2]error
}
func (l *readerLeaf) Error() string {return "reader registration accounting cleanup incomplete"}
func (l *readerLeaf) Unwrap() []error {return l.causes[:]}
func (n readerNode) empty() bool {return n.leaf==nil && n.branch==nil}
func (n readerNode) summary() readerSummary {if n.leaf!=nil{return readerSummary{n.leaf.sequence,n.leaf.handle,n.leaf.handle,1}};return n.branch.summary}
func (n readerNode) setParent(p *readerBranch) {if n.leaf!=nil{n.leaf.parent=p}else if n.branch!=nil{n.branch.parent=p}}
func readerSide(key uint64,bit uint8) int{return int((key>>bit)&1)}
func repairReaderBranch(b *readerBranch){a,c:=b.child[0].summary(),b.child[1].summary();m:=a.minimum;if c.minimum<m{m=c.minimum};b.summary=readerSummary{m,a.lo,c.hi,a.count+c.count}}
func repairReaderAncestors(b *readerBranch){for ;b!=nil;b=b.parent{repairReaderBranch(b)}}
func readerLeafCharge() uint64{return retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(readerLeaf{})))}
func readerBranchCharge() uint64{return retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(readerBranch{})))}
func ReaderRegistryHeaderCharge() uint64{return retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(ReaderRegistry{})))}
type ReaderRegistry struct {
 mu sync.Mutex
 root readerNode
 metadata *retainedalloc.Owner
 fastShards [fastReaderShardCount]readerRegistryShard
 nextFastShard atomic.Uint64
}
// NewReaderRegistry is explicitly standalone, GC-owned construction.
func NewReaderRegistry() *ReaderRegistry{return &ReaderRegistry{}}
// NewOwnedReaderRegistry admits the separate header before its actual birth.
// Its charge transfers only through the original generation terminal receipt.
func NewOwnedReaderRegistry(owner *retainedalloc.Owner)(*ReaderRegistry,error){if owner==nil{return nil,retainedalloc.ErrCapacity};if err:=owner.AddPending(ReaderRegistryHeaderCharge());err!=nil{return nil,err};return &ReaderRegistry{metadata:owner},nil}
func (r *ReaderRegistry) Register(seq uint64)(int64,error){id,_,err:=r.RegisterWithHint(seq,registerHintUnset);return id,err}
func (r *ReaderRegistry) hint(h int)int{if h<0{return int(r.nextFastShard.Add(1)-1)&fastReaderShardMask};return h&fastReaderShardMask}
func (r *ReaderRegistry) fastLocked(seq uint64,h int)(int64,int){for i:=0;i<fastReaderShardCount;i++{j:=(h+i)&fastReaderShardMask;if id:=r.tryJoinFast(seq,j);id!=0{return id,j}};for i:=0;i<fastReaderShardCount;i++{j:=(h+i)&fastReaderShardMask;if id:=r.tryClaimFast(seq,j);id!=0{return id,j}};return 0,registerHintUnset}
func (r *ReaderRegistry) RegisterWithHint(seq uint64,hint int)(int64,int,error){
 if r==nil{return 0,registerHintUnset,retainedalloc.ErrClosed};hint=r.hint(hint)
 if id:=r.tryJoinFast(seq,hint);id!=0{return id,hint,nil}
 r.mu.Lock();id,h:=r.fastLocked(seq,hint);owner:=r.metadata;r.mu.Unlock();if id!=0{return id,h,nil}
 // Governing lease callbacks run OUTSIDE registry.mu. Recheck after admission.
 charge:=readerLeafCharge()+readerBranchCharge()
 if owner!=nil{if err:=owner.AddPending(charge);err!=nil{return 0,hint,err}}
 leaf,branch:=&readerLeaf{owner:owner},&readerBranch{}
 r.mu.Lock();id,h=r.fastLocked(seq,hint);used:=uint64(0)
 if id==0{
  key,err:=r.firstHoleLocked();if err!=nil{r.mu.Unlock();branch=nil;return 0,hint,leaf.refund(charge,err)}
  leaf.handle,leaf.sequence=key,seq
  if r.root.empty(){r.root=readerNode{leaf:leaf};used=readerLeafCharge()}else{r.insertLocked(leaf,branch);used=charge}
  id,h=int64(key),registerHintUnset
 }
 r.mu.Unlock();branch=nil
 if owner==nil || used==charge{return id,h,nil}
 if err:=leaf.refund(charge-used,nil);err!=nil{
  // A failed attempt cannot expose a registration with unsettled speculation.
  if used!=0{r.mu.Lock();extra:=r.detachLeafLocked(leaf);r.mu.Unlock();leaf.debt+=extra}
  return 0,hint,err
 }
 return id,h,nil
}
func (l *readerLeaf) refund(amount uint64,first error)error{
 if l.owner==nil || amount==0{return first};l.debt=amount;l.causes[0]=first
 if _,err:=l.removal.BindPendingOriginal(l.owner,amount);err!=nil{l.causes[1]=err;l.owner.CleanupFailed();return l}
 for{done,err:=l.removal.ContinueOriginal();if err!=nil{l.causes[1]=err;l.owner.CleanupFailed();return l};if done{break}}
 l.debt=0;return first
}
func (r *ReaderRegistry) firstHoleLocked()(uint64,error){
 if r.root.empty(){return 1,nil};n:=r.root;s:=n.summary();if s.lo>1{return 1,nil}
 if s.count==s.hi-s.lo+1{if s.hi==math.MaxInt64{return 0,retainedalloc.ErrCapacity};return s.hi+1,nil}
 for n.branch!=nil{a,b:=n.branch.child[0].summary(),n.branch.child[1].summary();if a.count!=a.hi-a.lo+1{n=n.branch.child[0];continue};if a.hi+1<b.lo{return a.hi+1,nil};n=n.branch.child[1]}
 return 0,retainedalloc.ErrCapacity
}
func (r *ReaderRegistry) insertLocked(l *readerLeaf,b *readerBranch){
 n:=r.root;for n.branch!=nil{n=n.branch.child[readerSide(l.handle,n.branch.bit)]};bit:=uint8(bits.Len64(l.handle^n.leaf.handle)-1)
 n=r.root;var parent *readerBranch;for n.branch!=nil && n.branch.bit>bit{parent=n.branch;n=n.branch.child[readerSide(l.handle,n.branch.bit)]}
 b.parent,b.bit=parent,bit;side:=readerSide(l.handle,bit);b.child[side]=readerNode{leaf:l};b.child[1-side]=n;l.parent=b;n.setParent(b)
 if parent==nil{r.root=readerNode{branch:b}}else{parent.child[readerSide(l.handle,parent.bit)]=readerNode{branch:b}};repairReaderAncestors(b)
}
func (r *ReaderRegistry) detachLeafLocked(l *readerLeaf)uint64{
 charge:=readerLeafCharge();p:=l.parent
 if p==nil{r.root=readerNode{}}else{side:=readerSide(l.handle,p.bit);sibling:=p.child[1-side];gp:=p.parent;sibling.setParent(gp);if gp==nil{r.root=sibling}else{gp.child[readerSide(l.handle,gp.bit)]=sibling};*p=readerBranch{};charge+=readerBranchCharge();repairReaderAncestors(gp)}
 l.parent=nil;l.handle,l.sequence=0,0;return charge
}
func (r *ReaderRegistry) RegisterFastWithHint(seq uint64,hint int)(int64,int){hint=r.hint(hint);if id:=r.tryJoinFast(seq,hint);id!=0{return id,hint};r.mu.Lock();defer r.mu.Unlock();return r.fastLocked(seq,hint)}
func (r *ReaderRegistry) Unregister(id int64)error{
 if r==nil || id==0{return nil};if id<0{r.unregisterFast(id);return nil}
 r.mu.Lock();n:=r.root;for n.branch!=nil{n=n.branch.child[readerSide(uint64(id),n.branch.bit)]};if n.leaf==nil || n.leaf.handle!=uint64(id){r.mu.Unlock();return nil};l:=n.leaf;charge:=r.detachLeafLocked(l);r.mu.Unlock();return l.refund(charge,nil)
}
func (r *ReaderRegistry) unregisterFast(id int64){shard:=int(-id-1);if shard<0 || shard>=fastReaderShardCount{return};if c:=r.fastShards[shard].count.Add(-1);c<0{r.fastShards[shard].count.Store(0)}}
func (r *ReaderRegistry) minLocked()uint64{m:=r.loadFastMin();if !r.root.empty(){if s:=r.root.summary().minimum;s<m{m=s}};return m}
func (r *ReaderRegistry) MinPinnedSeq()uint64{r.mu.Lock();defer r.mu.Unlock();return r.minLocked()}
func (r *ReaderRegistry) MinPinnedSeqWithWorkV6(w *iterator.OrdinalScanWork)(uint64,bool,error){if r==nil || w==nil{return 0,false,nil};records,bytes,_:=NativeMinimumPriceForTestV6(0);if !w.Reserve(records,bytes){if records>w.RecordLimit || bytes>w.ByteLimit{return 0,false,&iterator.OrdinalUnitTooLarge{Records:records,Bytes:bytes}};return 0,false,nil};r.mu.Lock();defer r.mu.Unlock();return r.minLocked(),true,nil}
'''
p.write_text(head+tail)
