package db

import (
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/pager"
)

// This is borrowed evidence on an already completed durable descriptor, not a
// new physical owner or a certificate for every byte currently in the file.
// Open/rebuild leaves it empty. Only successful capsule publication mints it.
type primaryDataCompletionV6 struct {
	pager       *pager.Pager
	incarnation uint64
	identity    rootpublication.StableIdentity
}

// The original parent record/meta are immutable value copies in seal.base.
// Keeping its selected slot avoids consulting a newer visible or durable root
// during I/O. The original DATA token retains physical custody throughout.
type primaryDataParentCoverageV6 struct {
	completed  primaryDataCompletionV6
	parentSlot uint64
}

func (proof primaryDataParentCoverageV6) valid() bool { return proof.completed.pager != nil }

// Called only by Prepare, under its activation gate and the existing
// durablePublishMu -> rootReuseMu -> runtime.mu lock order.
func (runtime *rootPublicationRuntimeV1) primaryDataParentCoverageV6(seal *rootPublicationSealV1, base durableRootRuntimeV1) primaryDataParentCoverageV6 {
	var none primaryDataParentCoverageV6
	if seal == nil || seal.primary == nil || seal.primary.promotion == nil || base.primary == nil ||
		base.slot > 1 || base.pending != nil || len(base.ambiguous) != 0 || len(seal.prefix) != 0 || seal.token == nil || seal.idx != runtime.idx {
		return none
	}
	completed := base.primary.dataCompletion
	if completed.pager == nil || completed.pager != seal.idx.pager || completed.incarnation != seal.idx.id ||
		completed.incarnation != seal.token.Generation() || completed.identity != seal.token.Identity() {
		return none
	}
	parent := base.primary.capsuleViews[base.slot]
	// The constructor copied this exact validated parent directory into the
	// child's embedded-parent page. Its address and resulting directory digest
	// necessarily change; no DATA operand or other parent metadata may change.
	embedded := seal.primary.promotion.View.Parent()
	embedded.Record.UserRootPageID = parent.Current().Record.UserRootPageID
	embedded.Primary.DirectoryDigest = parent.Current().Primary.DirectoryDigest
	if parent.Slot() != base.slot || parent.Current().Record != base.record ||
		parent.Current().Primary != base.primary.projection || embedded != parent.Current() ||
		seal.base.slotMeta[base.slot] != base.meta || seal.base.slotRecord[base.slot] != base.record {
		return none
	}
	a, b := seal.record, base.record
	x, y := seal.primary.projection, base.primary.projection
	if a.Freelist != b.Freelist || a.TotalPages != b.TotalPages || a.FreelistFreeCount != b.FreelistFreeCount ||
		a.FreelistRetiredCount != b.FreelistRetiredCount || a.SystemRootPageID != b.SystemRootPageID ||
		x.BaseRootPageID != y.BaseRootPageID || x.BaseSequence != y.BaseSequence || x.DataCommitSeq != y.DataCommitSeq ||
		x.BaseDigest != y.BaseDigest || x.SystemDigest != y.SystemDigest ||
		seal.idx.pager.PageCount() < seal.primary.prepared.Generation().HighWater() {
		return none
	}
	return primaryDataParentCoverageV6{completed: completed, parentSlot: base.slot}
}

func (proof primaryDataParentCoverageV6) matchesParent(seal *rootPublicationSealV1, base durableRootRuntimeV1) bool {
	return proof.valid() && base.slot == proof.parentSlot && base.pending == nil && len(base.ambiguous) == 0 && base.primary != nil &&
		base.primary.dataCompletion == proof.completed && base.meta == seal.base.slotMeta[proof.parentSlot] &&
		base.record == seal.base.slotRecord[proof.parentSlot]
}
