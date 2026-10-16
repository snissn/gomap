package db

import (
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// nativeCapsuleParentRecordWorkV6 prices target validation/address arithmetic,
// the complete record copy, four codec-defined writes, and full comparison.
// Copy and comparison each touch two complete records. Scalar traffic is the
// two operand pointers, target read, UserRootPageID and ancestry writes.
// Both callers reserve this fixed traffic before inspecting the operands.
func nativeCapsuleParentRecordWorkV6() (uint64, uint64) {
	return 4, 4*uint64(unsafe.Sizeof(rootpublication.DurableRootRecordV1{})) + 2*uint64(unsafe.Sizeof((*rootpublication.DurableRootRecordV1)(nil))) + 4*uint64(unsafe.Sizeof(uint64(0))) + uint64(unsafe.Sizeof([32]byte{}))
}

// Capsules carry the parent by value at their own directory page. Project only
// the fields canonicalized by encodeCapsuleOperandV6; every other field remains
// part of the full equality. This stack copy owns no retained edge or authority.
func nativeCapsuleParentRecordMatchesV6(base, embedded *rootpublication.DurableRootRecordV1, target uint64) bool {
	if target > 1 || base == nil || embedded == nil {
		return false
	}
	expected := *base
	expected.UserRootPageID = rootpublication.PrimaryCapsulePageV6(target) + 2
	expected.ParentRecordPageID, expected.ParentCommitSeq = 0, 0
	expected.ParentRecordDigest = [32]byte{}
	return expected == *embedded
}
