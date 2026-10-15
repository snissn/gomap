package rootpublication

import (
 "github.com/snissn/gomap/TreeDB/internal/iterator"
 "github.com/snissn/gomap/TreeDB/internal/primaryarena"
 "github.com/snissn/gomap/TreeDB/node"
)

// SealPrimaryMaterializedGraftV6 continues the SAME native constructor. Its DB
// caller owns selected-record validation and the current publication cut.
func (t *DurableRootTransaction) SealPrimaryMaterializedGraftV6(captured,current *primaryarena.ReadRootCertificateV6,base node.PrimaryOperand,sequence uint64,scratch []byte,w *iterator.OrdinalScanWork)(bool,error) {
 if t==nil || w==nil { return false,ErrDurableRootOwnership }
 t.mu.Lock()
 defer t.mu.Unlock()
 c:=t.constructionV6
 if c==nil || c.cleanupPhase!=0 || t.phase!=durableRootConstructingV6 || c.certificate!=nil || c.componentCount!=0 { return false,ErrDurableRootOwnership }
 cert,ready,err:=c.arena.SealConstructedMaterializedGraftV6(c.root,captured,current,base,sequence,scratch,t,w)
 if ready && err==nil { c.certificate=cert }
 return ready,err
}
