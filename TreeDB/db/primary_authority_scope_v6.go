package db

import (
	"unsafe"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// nativeAuthorityScopeV6 is call-local custody over the original authority
// mutation. It never escapes the original producer
// call to publishNativeAuthorityWithWorkV6. Dependencies, namespaces,
// materialization and index sync run before this scope. Only the fixed actual
// authority write and original post-fence install run under these locks.
type nativeAuthorityScopeV6 struct {
	runtime   *rootPublicationRuntimeV1
	seal      *rootPublicationSealV1
	allocator freelist.COWPublicationScopeV6
	active    bool
}

func (s *nativeAuthorityScopeV6) begin(runtime *rootPublicationRuntimeV1, seal *rootPublicationSealV1, w *iterator.OrdinalScanWork) (bool, error) {
	if s == nil || s.active || runtime == nil || seal == nil || w == nil {
		return false, rootpublication.ErrNativeMVCCRootWorkRequiredV6
	}
	db := runtime.db
	db.durablePublishMu.Lock()
	db.rootReuseMu.Lock()
	runtime.mu.Lock()
	s.runtime, s.seal, s.active = runtime, seal, true
	ready := false
	defer func() {
		if !ready {
			s.Close()
		}
	}()
	// All current validation and compaction shapes are admitted before a
	// variable traversal. These bounds cover prefix validation, debt copy and
	// clear, seal partition and clear, and fixed two-slot ownership transfers.
	records := uint64(11) + 2*uint64(len(seal.prefix)) + 4*uint64(len(runtime.debt)) + 3*uint64(len(runtime.seals)) + 2*uint64(len(runtime.visibleMembers)-runtime.visibleHead)
	bytes := 2*uint64(unsafe.Sizeof(*s)) + 4*uint64(unsafe.Sizeof(primaryDurableRuntimeV5{})) +
		uint64(len(seal.prefix))*16 + uint64(len(runtime.debt))*32 + uint64(len(runtime.seals))*32 + 2*uint64(len(runtime.visibleMembers)-runtime.visibleHead)*16
	if !w.Reserve(records, bytes) {
		if records > w.RecordLimit || bytes > w.ByteLimit {
			return false, &iterator.OrdinalUnitTooLarge{Records: records, Bytes: bytes}
		}
		return false, nil
	}
	member := runtime.visibleMember(seal.latestSequence)
	// Canonical capsules embed their parent by value and zero ParentCommitSeq.
	// Bind authority to the complete captured original slot and generation ref.
	if seal.target > 1 || seal.primary == nil || seal.primary.promotion == nil {
		return false, rootpublication.ErrDurableRootOwnership
	}
	baseSlot := uint64(1) - seal.target
	if runtime.activeSeal != seal || runtime.poison != nil || member == nil || member.transaction == nil ||
		!member.transaction.RequiresNativeCallbackWorkV6() || seal.primary == nil ||
		member.primary != seal.primary.prepared || member.primaryRecordTransferred ||
		db.durableRoot.primary == nil || db.durableRoot.slot != baseSlot ||
		db.durableRoot.meta != seal.base.slotMeta[baseSlot] || db.durableRoot.record != seal.base.slotRecord[baseSlot] ||
		db.durableRoot.primary.records[baseSlot] != seal.primary.promotion.Parent ||
		seal.primary.promotion.View.Parent().Record != db.durableRoot.record ||
		!seal.commitStorageReady || seal.commitRuntime == nil || cap(seal.commitRetired) < len(runtime.seals) ||
		len(runtime.debt) < len(seal.prefix) {
		return false, rootpublication.ErrDurableRootOwnership
	}
	for i, prepared := range seal.prefix {
		if runtime.debt[i] != prepared {
			return false, rootpublication.ErrDurableRootOwnership
		}
	}
	if len(seal.prefix) != 0 {
		ok, err := seal.idx.allocator.BeginCOWPublicationScopeV6(&s.allocator, seal.prefix, w)
		if !ok || err != nil {
			return false, err
		}
	}
	ready = true
	return true, nil
}

func (s *nativeAuthorityScopeV6) Close() {
	if s == nil || !s.active {
		return
	}
	runtime := s.runtime
	s.allocator.Close()
	s.runtime, s.seal, s.active = nil, nil, false
	runtime.mu.Unlock()
	runtime.db.rootReuseMu.Unlock()
	runtime.db.durablePublishMu.Unlock()
}
