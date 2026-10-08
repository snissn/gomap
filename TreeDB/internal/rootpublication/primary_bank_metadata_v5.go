package rootpublication

import (
	"bytes"
	"encoding/binary"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// PrimaryBankMetadataEdgesV5 is the actual format decoder used for publication
// custody and restart recovery. A durable record owns its complete primary and
// dependency roots. Parent proof record images are independent handles supplied
// by the selector/capture owner; parent fields never create a retention chain.
func PrimaryBankMetadataEdgesV5(class primaryarena.Class, image []byte) ([]primaryarena.MetadataEdge, error) {
	return primaryBankMetadataEdgesV5(class, image, nil, nil)
}

// Fixed page-local validation scratch belongs to the same arena metadata owner.
// Borrowed codec fields never escape this synchronous scope.
type primaryBankMetadataScratchV5 struct {
	owner      *retainedalloc.Owner
	charge     uint64
	edges      [page.PageSize / 8]primaryarena.MetadataEdge
	count      int
	dependency dependencyBorrowScratch
}

func (s *primaryBankMetadataScratchV5) Edges() []primaryarena.MetadataEdge {
	if s.count == 0 {
		return nil
	}
	return s.edges[:s.count]
}
func (s *primaryBankMetadataScratchV5) Close() {
	if s.owner == nil {
		return
	}
	owner, charge := s.owner, s.charge
	*s = primaryBankMetadataScratchV5{}
	owner.RemovePending(charge)
}
func PrimaryBankMetadataEdgesOwnedV5(class primaryarena.Class, image []byte, owner *retainedalloc.Owner) (primaryarena.MetadataEdgeScratch, error) {
	if owner == nil {
		return nil, ErrResourceOwnership
	}
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(primaryBankMetadataScratchV5{})))
	if err := owner.AddPending(charge); err != nil {
		return nil, err
	}
	s := &primaryBankMetadataScratchV5{owner: owner, charge: charge}
	edges, err := primaryBankMetadataEdgesV5(class, image, s.edges[:0], &s.dependency)
	if err != nil {
		s.Close()
		return nil, err
	}
	s.count = len(edges)
	return s, nil
}
func primaryBankMetadataEdgesV5(class primaryarena.Class, image []byte, dst []primaryarena.MetadataEdge, scratch *dependencyBorrowScratch) ([]primaryarena.MetadataEdge, error) {
	if len(image) != page.PageSize || !page.VerifyChecksumNonMutating(image) {
		return nil, ErrDurableRootRecordChecksum
	}
	h := page.DecodeHeader(image)
	switch class {
	case primaryarena.Record:
		var expected [32]byte
		copy(expected[:], image[312:344])
		v, e := DecodeDurablePrimaryRootRecordV5(image, h.PageID, expected)
		if e != nil {
			return nil, e
		}
		edges := append(dst, primaryarena.MetadataEdge{PageID: v.Record.UserRootPageID, Class: primaryarena.Directory, Digest: v.Primary.DirectoryDigest})
		if v.Record.Directory != (DependencyDirectoryRefV2{}) {
			edges = append(edges, primaryarena.MetadataEdge{PageID: v.Record.Directory.RootPageID, Class: primaryarena.Dependency})
		} else {
			edges = append(edges, primaryarena.MetadataEdge{PageID: v.Record.Manifest.FirstPageID, Class: primaryarena.Manifest})
		}
		return edges, nil
	case primaryarena.Manifest:
		if page.PageType(h.Flags) != page.PageTypeDependencyManifest || h.Count != 0 || !bytes.Equal(image[16:24], dependencyManifestPageMagicV1[:]) || binary.LittleEndian.Uint16(image[24:26]) != 1 || binary.LittleEndian.Uint16(image[26:28]) != dependencyManifestPageHeaderV1 || !allZeroV1(image[92:96]) {
			return nil, ErrDependencyManifestFormat
		}
		index, count := binary.LittleEndian.Uint32(image[28:32]), binary.LittleEndian.Uint32(image[32:36])
		total := binary.LittleEndian.Uint64(image[40:48])
		next := binary.LittleEndian.Uint64(image[48:56])
		length := binary.LittleEndian.Uint32(image[88:92])
		var digest [32]byte
		copy(digest[:], image[56:88])
		if total < 16 || total > maxDependencyManifestBytesV1 || count == 0 || index >= count || uint64(count) != (total+dependencyManifestPayloadV1-1)/dependencyManifestPayloadV1 || digest == ([32]byte{}) {
			return nil, ErrDependencyManifestFormat
		}
		wantLength := min(uint64(dependencyManifestPayloadV1), total-uint64(index)*dependencyManifestPayloadV1)
		if uint64(length) != wantLength || !allZeroV1(image[dependencyManifestPageHeaderV1+int(length):]) {
			return nil, ErrDependencyManifestFormat
		}
		if index+1 == count {
			if next != 0 {
				return nil, ErrDependencyManifestFormat
			}
			return nil, nil
		}
		if next != h.PageID+1 || !primaryarena.IsPage(next) {
			return nil, ErrDependencyManifestFormat
		}
		return append(dst, primaryarena.MetadataEdge{PageID: next, Class: primaryarena.Manifest}), nil
	case primaryarena.Dependency:
		n := node.NewNode(image)
		if n.Count() > page.PageSize/2 || (scratch != nil && n.Type() == page.PageTypeInternal && int(n.Count()) > cap(dst)) || (n.Type() != page.PageTypeLeaf && n.Type() != page.PageTypeInternal) {
			return nil, ErrDependencyManifestFormat
		}
		var previous []byte
		edges := dst
		for i := uint16(0); i < n.Count(); i++ {
			var key []byte
			if n.Type() == page.PageTypeInternal {
				k, child, e := n.GetInternalEntryRefView(i)
				if e != nil || child.Kind != page.ChildRefPage || !primaryarena.IsPage(child.Page) {
					return nil, ErrDependencyManifestFormat
				}
				key = k
				edges = append(edges, primaryarena.MetadataEdge{PageID: child.Page, Class: primaryarena.Dependency})
			} else {
				k, value, ptr, flags, e := n.GetLeafEntryView(i)
				if e != nil || ptr != (page.ValuePtr{}) || flags != 0 || len(k) == 0 {
					return nil, ErrDependencyManifestFormat
				}
				key = k
				switch k[0] {
				case dependencyPhysicalKeyV2:
					if scratch == nil {
						_, e = DecodeDependencyPhysicalV2(k, value)
					} else {
						_, e = scratch.physical(k, value)
					}
					if e != nil {
						return nil, e
					}
				case dependencyLogicalKeyV2:
					if scratch == nil {
						_, _, e = DecodeDependencyLogicalV2(k, value)
					} else {
						_, _, e = decodeBorrowedLogicalV2(k, value)
					}
					if e != nil {
						return nil, e
					}
				default:
					return nil, ErrDependencyManifestFormat
				}
			}
			if i > 0 && bytes.Compare(previous, key) >= 0 {
				return nil, ErrDependencyManifestFormat
			}
			previous = key
		}
		return edges, nil
	}
	return nil, ErrDurableRootRecordFormat
}
