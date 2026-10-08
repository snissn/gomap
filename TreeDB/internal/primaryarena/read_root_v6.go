package primaryarena

import (
	"crypto/sha256"
	"encoding/binary"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"os"
	"unsafe"
)

// OpenCapsule reserves the two fixed complete three-page root capsules before
// any reusable component/dependency allocation. No legacy format is relabeled.
func OpenCapsule(path string) (*Arena, error)         { return open(path, false, true) }
func OpenReadOnlyCapsule(path string) (*Arena, error) { return open(path, true, true) }

// OpenExisting admits the actual header format and never creates or relabels
// a file. The full header checksum/version/UUID is checked by the shared Open.
func OpenExisting(path string, readOnly bool) (*Arena, error) {
	f, e := os.Open(path)
	if e != nil {
		return nil, e
	}
	var header [26]byte
	_, e = f.ReadAt(header[:], 0)
	f.Close()
	if e != nil {
		return nil, e
	}
	switch string(header[16:24]) {
	case "TDPRCAPB":
		return open(path, readOnly, true)
	case "TDPRBANK":
		return open(path, readOnly, false)
	default:
		return nil, ErrFormat
	}
}

func (a *Arena) CapsuleFormatV6() bool { return a != nil && a.capsuleFormat }

// PrepareReadRootV6 creates one private immutable read-root owner. Its RAM-only
// address is distinct from every physical bank; it can never enter the physical
// allocator, durable extent or fixed-slot eligibility. SealDirectory transfers
// the owned canonical copy and component group edges into this same owner.
func (a *Arena) PrepareReadRootV6(w *iterator.OrdinalScanWork) (Ref, bool, error) {
	return a.PrepareConstructedReadRootV6(nil, w)
}
func (a *Arena) PrepareConstructedReadRootV6(owner ReadRootConstructionV6, w *iterator.OrdinalScanWork) (Ref, bool, error) {
	bytes := 2*uint64(unsafe.Sizeof(bank{})) + 128
	if !reserve(w, 1, bytes) {
		return Ref{}, false, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.capsuleFormat || !a.recovered {
		return Ref{}, false, ErrUnrecovered
	}
	if a.readSerial >= Namespace-pager.PrimaryReadRootLocalBaseV6-1 {
		return Ref{}, false, ErrFull
	}
	local := pager.PrimaryReadRootLocalBaseV6 + a.readSerial + 1
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(bank{})))
	if err := a.metadata.Add(charge); err != nil {
		return Ref{}, false, err
	}
	b := &bank{incarnation: 1, refs: 1, class: Directory, privatePublication: owner == nil, construction: owner, metadataCharge: charge}
	if err := a.pager.PreparePrimaryReadRootOwnerV6(local, b); err != nil {
		a.metadata.Remove(charge)
		return Ref{}, false, err
	}
	a.readSerial++
	a.counters.Claims++
	a.counters.Bytes += bytes
	return Ref{Namespace + local, 1}, true, nil
}

// ValidateOwnedReadRootV6 checks the actual sealed immutable owner, not a fixed
// slot address or a caller's directory digest. There is no private record bank.
func (a *Arena) ValidateOwnedReadRootV6(bundle PublicationBundle, w *iterator.OrdinalScanWork) (bool, error) {
	if !reserve(w, 1, 2*uint64(unsafe.Sizeof(bank{}))+64) {
		return false, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	d := a.valid(bundle.Directory)
	if !a.capsuleFormat || bundle.Record != (Ref{}) || Local(bundle.Directory.PageID) < pager.PrimaryReadRootLocalBaseV6 || d == nil || d.class != Directory || !d.sealed {
		return false, ErrStale
	}
	a.counters.EdgeReads++
	return true, nil
}

// BindReadRootDependencyV6 attaches genuine immutable dependency-bank custody
// to an unpublished promotion/recovery root. Published logical readers never
// receive the later coalesced publication's dependency closure.
func (a *Arena) BindReadRootDependencyV6(root Ref, edge MetadataEdge, w *iterator.OrdinalScanWork) (bool, error) {
	bytes := 4*uint64(unsafe.Sizeof(bank{})) + 2*uint64(unsafe.Sizeof(Ref{}))
	if !reserve(w, 2, bytes) {
		return false, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	b, child := a.valid(root), a.slot(edge.PageID)
	if !a.capsuleFormat || Local(root.PageID) < pager.PrimaryReadRootLocalBaseV6 || b == nil || !b.sealed || b.class != Directory || len(b.edges) != 0 || child == nil || !child.sealed || child.refs == 0 || child.refs == ^uint64(0) || (edge.Class != Dependency && edge.Class != Manifest) || child.class != edge.Class || (edge.Digest != ([32]byte{}) && child.digest != edge.Digest) {
		return false, ErrStale
	}
	charge := a.edgeCharge(1)
	if err := a.metadata.Add(charge); err != nil {
		return false, err
	}
	child.refs++
	b.edges = []Ref{{edge.PageID, child.incarnation}}
	b.metadataCharge += charge
	a.counters.EdgeReads += 2
	a.counters.EdgeWrites++
	a.counters.Bytes += bytes
	return true, nil
}

// ReadRootCertificateV6 borrows the directory's actual constructor-owned
// immutable image. It owns no extra reference: the transferred publication
// claim (or caller's retained Ref) must remain alive through its consumption.
type ReadRootCertificateV6 struct {
	arena     *Arena
	ref       Ref
	directory node.PrimaryDirectoryView
	image     []byte
	digest    [32]byte
}

func (c *ReadRootCertificateV6) Reference() Ref {
	if c == nil {
		return Ref{}
	}
	return c.ref
}
func (c *ReadRootCertificateV6) Arena() *Arena {
	if c == nil {
		return nil
	}
	return c.arena
}
func (c *ReadRootCertificateV6) Directory() node.PrimaryDirectoryView {
	if c == nil {
		return node.PrimaryDirectoryView{}
	}
	return c.directory
}
func (c *ReadRootCertificateV6) Image() []byte {
	if c == nil {
		return nil
	}
	return c.image
}
func (c *ReadRootCertificateV6) Digest() [32]byte {
	if c == nil {
		return [32]byte{}
	}
	return c.digest
}

// TakeReadRootCertificateV6 combines private-claim transfer with the fresh
// actual-bank check. Refusal leaves ownership unchanged, including corrupted
// images and under-admitted work. Hash/CRC read owned bytes, never caller scratch.
func (a *Arena) TakeReadRootCertificateV6(id uint64, w *iterator.OrdinalScanWork) (*ReadRootCertificateV6, bool, error) {
	return a.readRootCertificateV6(Ref{PageID: id}, true, w)
}
func (a *Arena) BorrowReadRootCertificateV6(ref Ref, w *iterator.OrdinalScanWork) (*ReadRootCertificateV6, bool, error) {
	return a.readRootCertificateV6(ref, false, w)
}
func (a *Arena) readRootCertificateV6(ref Ref, take bool, w *iterator.OrdinalScanWork) (*ReadRootCertificateV6, bool, error) {
	// One bank/ownership operand and one complete physical-image integrity operand.
	bytes := 2*uint64(unsafe.Sizeof(bank{})) + 2*page.PageSize + 2*uint64(unsafe.Sizeof(ReadRootCertificateV6{}))
	if !reserve(w, 2, bytes) {
		return nil, false, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	b := a.slot(ref.PageID)
	if !a.capsuleFormat || Local(ref.PageID) < pager.PrimaryReadRootLocalBaseV6 || b == nil || !b.sealed || b.class != Directory || b.refs == 0 ||
		(take && (b.refs != 1 || !b.privatePublication)) || (!take && b.incarnation != ref.Incarnation) {
		return nil, false, ErrStale
	}
	if len(b.image) != page.PageSize || !page.VerifyChecksumNonMutating(b.image) || sha256.Sum256(b.image) != b.digest || page.DecodeHeader(b.image).PageID != ref.PageID {
		return nil, false, ErrFormat
	}
	// SealDirectory already performed canonical decode and retained its view on
	// this owned image. Fresh integrity above validates precisely those bytes.
	cert := b.readCertificate
	if cert == nil {
		charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(ReadRootCertificateV6{})))
		if err := a.metadata.Add(charge); err != nil {
			return nil, false, err
		}
		cert = &ReadRootCertificateV6{arena: a, ref: Ref{ref.PageID, b.incarnation}, directory: b.directory, image: b.image, digest: b.digest}
		b.readCertificate = cert
		b.metadataCharge += charge
	}
	if take {
		b.privatePublication = false
	}
	a.counters.EdgeReads++
	a.counters.Bytes += bytes
	return cert, true, nil
}

// ReadRootConstructionV6 is implemented by the actual publication transaction.
// The existing bank retains this owner; the interface introduces no ref or queue.
type ReadRootConstructionV6 interface {
	PrimaryConstructionReferenceV6() Ref
	OwnPrimaryComponentV6(Ref, *iterator.OrdinalScanWork) (bool, error)
	SealPrimaryConstructionV6([]byte, [1]*Group, *iterator.OrdinalScanWork) (bool, error)
	Abort() error
}

func (a *Arena) SealConstructedReadRootV6(ref Ref, image []byte, groups [1]*Group, owner ReadRootConstructionV6, w *iterator.OrdinalScanWork) (*ReadRootCertificateV6, bool, error) {
	var cert *ReadRootCertificateV6
	ready, err := a.sealDirectoryConstructedV6(ref, image, groups, nil, owner, &cert, w)
	return cert, ready, err
}

// This is a charged bank-owner lookup, not a transfer or an image certificate.
func (a *Arena) ReadRootConstructionOwnerV6(id uint64, w *iterator.OrdinalScanWork) (ReadRootConstructionV6, bool, error) {
	if !reserve(w, 1, 2*uint64(unsafe.Sizeof(bank{}))) {
		return nil, false, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	b := a.slot(id)
	if b == nil || b.refs == 0 || !b.sealed || b.construction == nil {
		return nil, false, ErrStale
	}
	return b.construction, true, nil
}

// Detach is the once-only end of constructor discoverability. Old readers still
// own the immutable bank and its closure; they no longer retain DB callbacks.
func (a *Arena) DetachReadRootConstructionV6(ref Ref, owner ReadRootConstructionV6, w *iterator.OrdinalScanWork) (bool, error) {
	if !reserve(w, 1, 2*uint64(unsafe.Sizeof(bank{}))) {
		return false, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	b := a.valid(ref)
	if b == nil || b.construction != owner {
		return false, ErrStale
	}
	b.construction = nil
	return true, nil
}

// CloneReadRootForPromotionV6 owns a separate immutable RAM image and genuine
// group edges. The visible logical root and its admitted retention never grow.
// Capsule encoding still consumes the original constructor certificate; this
// clone is slot custody, not a second publication/transaction authority.
func (a *Arena) CloneReadRootForPromotionV6(source Ref, w *iterator.OrdinalScanWork) (Ref, bool, error) {
	bytes := 4*uint64(unsafe.Sizeof(bank{})) + 5*page.PageSize + 2*uint64(unsafe.Sizeof(Group{})) + 128
	if !reserve(w, 3+groupCount, bytes) {
		return Ref{}, false, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	src := a.valid(source)
	if !a.capsuleFormat || !a.recovered || src == nil || src.class != Directory || !src.sealed ||
		Local(source.PageID) < pager.PrimaryReadRootLocalBaseV6 || len(src.edges) != 0 {
		return Ref{}, false, ErrStale
	}
	if len(src.image) != page.PageSize || !page.VerifyChecksumNonMutating(src.image) ||
		sha256.Sum256(src.image) != src.digest || page.DecodeHeader(src.image).PageID != source.PageID {
		return Ref{}, false, ErrFormat
	}
	for _, g := range src.groups {
		if g != nil && (g.owner != a || g.refs == 0 || g.refs == ^uint64(0)) {
			return Ref{}, false, ErrStale
		}
	}
	if a.readSerial >= Namespace-pager.PrimaryReadRootLocalBaseV6-1 {
		return Ref{}, false, ErrFull
	}
	local := pager.PrimaryReadRootLocalBaseV6 + a.readSerial + 1
	ref := Ref{Namespace + local, 1}
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(bank{}))) + retainedalloc.AllocationCharge(uint64(len(src.image)))
	if err := a.metadata.Add(charge); err != nil {
		return Ref{}, false, err
	}
	image := make([]byte, len(src.image))
	copy(image, src.image)
	binary.LittleEndian.PutUint64(image[:8], ref.PageID)
	page.UpdateChecksum(image)
	directory, err := node.DecodePrimaryDirectory(image)
	if err != nil {
		a.metadata.Remove(charge)
		return Ref{}, false, err
	}
	b := &bank{incarnation: 1, refs: 1, class: Directory, sealed: true, digest: sha256.Sum256(image), directory: directory, image: image, groups: src.groups, metadataCharge: charge}
	if err := a.pager.PreparePrimaryReadRootOwnerV6(local, b); err != nil {
		a.metadata.Remove(charge)
		return Ref{}, false, err
	}
	ok, err := a.pager.RegisterPrimaryReadRootV6(local, image, w)
	if !ok || err != nil {
		_, _ = a.pager.RemovePrimaryReadRootV6(local, nil)
		a.metadata.Remove(charge)
		return Ref{}, ok, err
	}
	for _, g := range src.groups {
		if g != nil {
			g.refs++
			a.counters.EdgeWrites++
		}
	}
	a.readSerial++

	a.counters.Claims++
	a.counters.EdgeReads += 1 + groupCount
	a.counters.Bytes += bytes
	return ref, true, nil
}
