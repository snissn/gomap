package rootpublication

import (
	"crypto/sha256"
	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"unsafe"
)

// PrimaryDataCertificateV6 certifies exactly one immutable DATA generation and
// two immutable root operands. It grants neither durability nor physical
// retention. The caller's existing index/generation/slot owners must retain
// these operands; a certificate cannot replace those owners or a completed cut.
type PrimaryDataCertificateV6 struct {
	generation   *freelist.FreelistGenerationV1
	source       *pager.Pager
	base         node.PrimaryOperand
	baseSequence uint64
	systemID     uint64
	systemDigest [32]byte
}

func (c *PreparedPrimaryRootV5) DataCertificateV6() *PrimaryDataCertificateV6 {
	if c == nil {
		return nil
	}
	return c.dataCertificate
}
func (c *PreparedPrimaryRootV5) ReadCertificateV6() *primaryarena.ReadRootCertificateV6 {
	if c == nil {
		return nil
	}
	return c.readCertificate
}

// NewPreparedPrimaryReadRootCertificateV6 is the shared ordinary/native root
// constructor. The arena certificate already checked the owned physical image;
// DATA validation is reused only for the identical retained generation, source,
// base operand/sequence and system root. Changed DATA always rebuilds the proof.
func NewPreparedPrimaryReadRootCertificateV6(cert *primaryarena.ReadRootCertificateV6, generation *freelist.FreelistGenerationV1, cow *freelist.PreparedCOWCandidateV1, frontier Frontier, data freelist.PageSource, previous *PrimaryDataCertificateV6, w *iterator.OrdinalScanWork) (*PreparedPrimaryRootV5, bool, error) {
	if w != nil && !w.Reserve(1, 2*uint64(unsafe.Sizeof(PreparedPrimaryRootV5{}))+2*uint64(unsafe.Sizeof(primaryarena.ReadRootCertificateV6{}))) {
		return nil, false, nil
	}
	if cert == nil || cert.Arena() == nil || data == nil || generation == nil || frontier.commitSeq == 0 || generation.CommitSeq() == 0 || generation.CommitSeq() == ^uint64(0) || generation.CommitSeq() > frontier.commitSeq || frontier.userRootPageID != cert.Reference().PageID {
		return nil, false, ErrInvalidCandidate
	}
	if cow != nil && (cow.Candidate() == nil || cow.Candidate().Generation() != generation || generation.CommitSeq() != frontier.commitSeq) {
		return nil, false, ErrInvalidCandidate
	}
	base, seq := cert.Directory().Base()
	if base.Ref.Kind != page.ChildRefPage {
		return nil, false, ErrInvalidCandidate
	}
	if w != nil && !w.Reserve(1, 2*uint64(unsafe.Sizeof(PrimaryDataCertificateV6{}))+128) {
		return nil, false, nil
	}
	a := cert.Arena()
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(PreparedPrimaryRootV5{}))) + retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(PrimaryDataCertificateV6{})))
	if err := a.AdmitRootMetadata(cert.Reference(), charge); err != nil {
		return nil, false, err
	}
	adopted := false
	defer func() {
		if !adopted {
			a.CancelRootMetadata(cert.Reference(), charge)
		}
	}()
	physical, _ := data.(*pager.Pager)
	proof := previous
	if proof == nil || physical == nil || proof.source != physical || proof.generation != generation || proof.base != base || proof.baseSequence != seq || proof.systemID != frontier.systemRootPageID {
		var ready bool
		var err error
		proof, ready, err = preparePrimaryDataCertificateV6(data, physical, generation, base, seq, frontier.systemRootPageID, w)
		if !ready || err != nil {
			return nil, ready, err
		}
	}
	projection := PrimaryProjectionV5{ArenaUUID: a.UUID(), ArenaHighWater: a.Pager().PageCount(), DirectoryDigest: cert.Digest(), BaseRootPageID: base.Ref.Page, BaseSequence: seq, DataCommitSeq: generation.CommitSeq(), BaseDigest: base.Digest, SystemDigest: proof.systemDigest}
	adopted = true
	return &PreparedPrimaryRootV5{arena: a, bundle: primaryarena.PublicationBundle{Directory: cert.Reference()}, generation: generation, cow: cow, frontier: frontier, projection: projection, dataCertificate: proof, readCertificate: cert}, true, nil
}
func preparePrimaryDataCertificateV6(data freelist.PageSource, physical *pager.Pager, generation *freelist.FreelistGenerationV1, base node.PrimaryOperand, sequence, systemID uint64, w *iterator.OrdinalScanWork) (*PrimaryDataCertificateV6, bool, error) {
	proof := &PrimaryDataCertificateV6{generation: generation, source: physical, base: base, baseSequence: sequence, systemID: systemID}
	for _, root := range []struct {
		id     uint64
		digest [32]byte
	}{{base.Ref.Page, base.Digest}, {systemID, [32]byte{}}} {
		unused, ready, err := generation.SnapshotPageUnusedWithWorkV1(root.id, generation.CommitSeq()+1, w)
		if !ready && err == nil {
			return nil, false, nil
		}
		if err != nil || unused {
			return nil, false, ErrInvalidCandidate
		}
		var image []byte
		if charged, ok := data.(interface {
			GetWithWork(uint64, *iterator.OrdinalScanWork) ([]byte, bool, error)
		}); ok {
			image, ready, err = charged.GetWithWork(root.id, w)
			if !ready && err == nil {
				return nil, false, nil
			}
		} else {
			if w != nil && !w.Reserve(1, page.PageSize) {
				return nil, false, nil
			}
			image, err = data.ReadPage(root.id)
		}
		if err != nil {
			return nil, false, err
		}
		// The actual DATA bytes are separate from pager mapping/header operands:
		// admit their CRC, type/id and full-page digest reads explicitly.
		if w != nil && !w.Reserve(1, 2*page.PageSize+128) {
			return nil, false, nil
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
		if root.id == systemID {
			proof.systemDigest = actual
		}
	}
	return proof, true, nil
}
