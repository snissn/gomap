package collections

import (
	"sync/atomic"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// nativeCollectionState owns one logical incarnation. Physical root replacement
// preserves incarnation and domain interpretation. Names are schema-owned;
// rename replaces schema/name-directory entries while retaining incarnation.
// pending contains only domains requiring physical materialization, so the
// physical builder need not scan an untouched collection's entire domain set.
type nativeCollectionState struct {
	refs        atomic.Int64
	incarnation uint64
	schema      *nativeSchemaState
	domains     *cowbtree.OwnedMap[string, *nativeDomainState]
	pending     *cowbtree.OwnedMap[string, *nativeDomainState]
	refund      *cowbtree.OwnedMapRefund
	attempted   bool
	err         error
}

// newNativeCollectionState adopts the supplied original schema/directory
// references only on success. Ordinary replacement retains the schema once;
// each structurally shared directory separately owns its actual root edge.
func newNativeCollectionState(incarnation uint64, schema *nativeSchemaState, domains, pending *cowbtree.OwnedMap[string, *nativeDomainState], admit cowbtree.OwnedMapAdmit) (*nativeCollectionState, error) {
	if incarnation == 0 || schema == nil || domains == nil || pending == nil || admit == nil {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	charge := nativeCollectionStateCharge()
	lease, err := admit(charge)
	if err != nil {
		return nil, err
	}
	refund := new(cowbtree.OwnedMapRefund)
	refund.Init(lease)
	c := &nativeCollectionState{incarnation: incarnation, schema: schema, domains: domains, pending: pending, refund: refund}
	c.refs.Store(1)
	return c, nil
}

type nativeCollectionLifetime struct{}

func (nativeCollectionLifetime) Retain(c *nativeCollectionState) {
	if c == nil || c.refs.Add(1) <= 1 {
		panic("collections: retain released native collection")
	}
}
func (nativeCollectionLifetime) Release(c *nativeCollectionState) (completion cowbtree.OwnedMapCompletion, err error) {
	if c == nil {
		return completion, nil
	}
	refs := c.refs.Add(-1)
	if refs < 0 {
		panic("collections: native collection released twice")
	}
	if refs != 0 {
		return completion, nil
	}
	if c.attempted {
		panic("collections: native collection cleanup replayed")
	}
	c.attempted = true
	child, childErr := finishNativeDomainDirectory(&c.pending)
	completion.Append(child)
	c.err = childErr
	child, childErr = finishNativeDomainDirectory(&c.domains)
	completion.Append(child)
	if c.err == nil {
		c.err = childErr
	}
	if c.err != nil {
		return completion, c.err
	}
	completion.Append(c.schema.release())
	c.schema = nil
	refund := c.refund
	c.refund = nil
	completion.Append(refund.Completion())
	return completion, nil
}

func finishNativeDomainDirectory(slot **cowbtree.OwnedMap[string, *nativeDomainState]) (completion cowbtree.OwnedMapCompletion, err error) {
	m := *slot
	if m == nil {
		return completion, nil
	}
	if m.Release() {
		completion, err = m.Finish()
		if err != nil {
			return completion, err
		}
	}
	*slot = nil
	m = nil
	return completion, nil
}

func nativeCollectionStateCharge() uint64 {
	return memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeCollectionState{}))) +
		memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowbtree.OwnedMapRefund{})))
}
