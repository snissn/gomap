package rootpublication

import (
	"crypto/sha256"
	"errors"
	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"unsafe"
)

// PreparedPrimaryRootV5 is exact immutable authority for a complete directory
// and its private record bank over one actual DATA generation. A bank-only
// publication owns no invented COW generation. When DATA really changes, cow
// identifies the genuine candidate that owns that generation separately.
// Lifecycle ownership is moved by DurableRootTransaction, not by these getters.
type PreparedPrimaryRootV5 struct {
	arena           *primaryarena.Arena
	bundle          primaryarena.PublicationBundle
	generation      *freelist.FreelistGenerationV1
	cow             *freelist.PreparedCOWCandidateV1
	frontier        Frontier
	projection      PrimaryProjectionV5
	dataCertificate *PrimaryDataCertificateV6
	readCertificate *primaryarena.ReadRootCertificateV6
}

func (c *PreparedPrimaryRootV5) Bundle() primaryarena.PublicationBundle {
	if c == nil {
		return primaryarena.PublicationBundle{}
	}
	return c.bundle
}
func (c *PreparedPrimaryRootV5) Generation() *freelist.FreelistGenerationV1 {
	if c == nil {
		return nil
	}
	return c.generation
}
func (c *PreparedPrimaryRootV5) COW() *freelist.PreparedCOWCandidateV1 {
	if c == nil {
		return nil
	}
	return c.cow
}
func (c *PreparedPrimaryRootV5) Projection() PrimaryProjectionV5 {
	if c == nil {
		return PrimaryProjectionV5{}
	}
	return c.projection
}
func (c *PreparedPrimaryRootV5) Frontier() Frontier {
	if c == nil {
		return Frontier{}
	}
	return c.frontier
}
func (c *PreparedPrimaryRootV5) Arena() *primaryarena.Arena {
	if c == nil {
		return nil
	}
	return c.arena
}

// NewPreparedPrimaryRootV5 validates ordinary producer output against actual
// physical operands. Its work is preparation work; callers must charge it in
// their return, or admit preparation separately. It never samples a current DB
// head and cannot mint custody for an unowned directory.
func NewPreparedPrimaryRootV5(a *primaryarena.Arena, bundle primaryarena.PublicationBundle, generation *freelist.FreelistGenerationV1, cow *freelist.PreparedCOWCandidateV1, frontier Frontier, data freelist.PageSource, w *iterator.OrdinalScanWork) (*PreparedPrimaryRootV5, bool, error) {
	return newPreparedPrimaryRoot(a, bundle, generation, cow, frontier, data, w, false)
}

// NewPreparedPrimaryReadRootV6 binds the copied immutable read directory to its
// actual DATA generation. It grants no fixed-slot seal, parent or eligibility.
func NewPreparedPrimaryReadRootV6(a *primaryarena.Arena, bundle primaryarena.PublicationBundle, generation *freelist.FreelistGenerationV1, cow *freelist.PreparedCOWCandidateV1, frontier Frontier, data freelist.PageSource, w *iterator.OrdinalScanWork) (*PreparedPrimaryRootV5, bool, error) {
	cert, ready, err := a.BorrowReadRootCertificateV6(bundle.Directory, w)
	if !ready || err != nil {
		return nil, ready, err
	}
	return NewPreparedPrimaryReadRootCertificateV6(cert, generation, cow, frontier, data, nil, w)
}
func newPreparedPrimaryRoot(a *primaryarena.Arena, bundle primaryarena.PublicationBundle, generation *freelist.FreelistGenerationV1, cow *freelist.PreparedCOWCandidateV1, frontier Frontier, data freelist.PageSource, w *iterator.OrdinalScanWork, capsule bool) (*PreparedPrimaryRootV5, bool, error) {
	bytes := uint64(24*page.PageSize) + 2*uint64(unsafe.Sizeof(PreparedPrimaryRootV5{}))
	if w != nil && !w.Reserve(3, bytes) {
		return nil, false, nil
	}
	if a == nil || data == nil || generation == nil || frontier.commitSeq == 0 || generation.CommitSeq() == 0 || generation.CommitSeq() == ^uint64(0) || generation.CommitSeq() > frontier.commitSeq || frontier.userRootPageID != bundle.Directory.PageID || !primaryarena.IsPage(bundle.Directory.PageID) || a.CapsuleFormatV6() != capsule || (!capsule && bundle.Record.PageID != bundle.Directory.PageID+1) || (capsule && bundle.Record != (primaryarena.Ref{})) {
		return nil, false, ErrInvalidCandidate
	}
	if cow != nil && (cow.Candidate() == nil || cow.Candidate().Generation() != generation || generation.CommitSeq() != frontier.commitSeq) {
		return nil, false, ErrInvalidCandidate
	}
	var owned bool
	var ownershipError error
	if capsule {
		owned, ownershipError = a.ValidateOwnedReadRootV6(bundle, w)
	} else {
		owned, ownershipError = a.ValidateUnsealedPublicationBundle(bundle, w)
	}
	if !owned || ownershipError != nil {
		return nil, owned, ownershipError
	}
	class, digest, ok, e := a.Identity(bundle.Directory, w)
	if !ok || e != nil {
		return nil, ok, e
	}
	if class != primaryarena.Directory {
		return nil, false, ErrInvalidCandidate
	}
	image, ok, e := a.Pager().GetWithWork(primaryarena.Local(bundle.Directory.PageID), w)
	if !ok || e != nil {
		return nil, ok, e
	}
	if sha256.Sum256(image) != digest {
		return nil, false, ErrDurableRootRecordDigest
	}
	d, e := node.DecodePrimaryDirectory(image)
	if e != nil {
		return nil, false, e
	}
	base, seq := d.Base()
	if base.Ref.Kind != page.ChildRefPage {
		return nil, false, ErrInvalidCandidate
	}
	projection := PrimaryProjectionV5{ArenaUUID: a.UUID(), ArenaHighWater: a.Pager().PageCount(), DirectoryDigest: digest, BaseRootPageID: base.Ref.Page, BaseSequence: seq, DataCommitSeq: generation.CommitSeq(), BaseDigest: base.Digest}
	for _, root := range []struct {
		id     uint64
		digest [32]byte
	}{{base.Ref.Page, base.Digest}, {frontier.systemRootPageID, [32]byte{}}} {
		unused, ready, e := generation.SnapshotPageUnusedWithWorkV1(root.id, generation.CommitSeq()+1, w)
		if !ready && e == nil {
			return nil, false, nil
		}
		if e != nil || unused {
			return nil, false, ErrInvalidCandidate
		}
		var image []byte
		if physical, ok := data.(interface {
			GetWithWork(uint64, *iterator.OrdinalScanWork) ([]byte, bool, error)
		}); ok {
			var done bool
			image, done, e = physical.GetWithWork(root.id, w)
			if !done && e == nil {
				return nil, false, nil
			}
		} else {
			if w != nil && !w.Reserve(1, uint64(page.PageSize)) {
				return nil, false, nil
			}
			image, e = data.ReadPage(root.id)
		}
		if e != nil {
			return nil, false, e
		}
		if len(image) != page.PageSize || !page.VerifyChecksumNonMutating(image) || page.DecodeHeader(image).PageID != root.id {
			return nil, false, ErrDurableRootRecordDigest
		}
		n := node.NewNode(image)
		if n.Type() != page.PageTypeLeaf && n.Type() != page.PageTypeInternal {
			return nil, false, ErrInvalidCandidate
		}
		actual := sha256.Sum256(image)
		if root.digest != ([32]byte{}) && actual != root.digest {
			return nil, false, ErrDurableRootRecordDigest
		}
		if root.id == frontier.systemRootPageID {
			projection.SystemDigest = actual
		}
	}
	return &PreparedPrimaryRootV5{arena: a, bundle: bundle, generation: generation, cow: cow, frontier: frontier, projection: projection}, true, nil
}
func (c *PreparedPrimaryRootV5) validateSequence(sequence uint64) error {
	if c == nil || c.arena == nil || c.generation == nil || c.frontier.commitSeq != sequence || c.projection.ArenaUUID != c.arena.UUID() || c.projection.DataCommitSeq != c.generation.CommitSeq() || c.projection.DataCommitSeq > sequence || c.projection.DirectoryDigest == ([32]byte{}) || c.projection.BaseDigest == ([32]byte{}) || c.projection.SystemDigest == ([32]byte{}) {
		return errors.New("primary transaction has no exact DATA and bank authority")
	}
	return nil
}
