package primaryarena

import (
	"crypto/sha256"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/page"
)

// MetadataDecoder validates the canonical native format and returns only its
// actual owning children. It is installed once by the format owner before any
// metadata claim or recovery. It must not follow parent durable-proof fields.
// Those finite proof records have independent incoming handles, never ancestry.
type MetadataDecoder func(Class, []byte) ([]MetadataEdge, error)

// MetadataEdgeScratch owns admitted temporary decoder backing. Edges may only
// be borrowed until Close; immutable bank refs are independently constructed.
type MetadataEdgeScratch interface {
	Edges() []MetadataEdge
	Close()
}
type OwnedMetadataDecoder func(Class, []byte, *retainedalloc.Owner) (MetadataEdgeScratch, error)

func (a *Arena) SetOwnedMetadataDecoder(legacy MetadataDecoder, owned OwnedMetadataDecoder) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if legacy == nil || owned == nil || a.metadataDecoder != nil {
		return ErrFormat
	}
	a.metadataDecoder, a.ownedMetadataDecoder = legacy, owned
	return nil
}

func (a *Arena) decodeMetadata(class Class, image []byte) ([]MetadataEdge, MetadataEdgeScratch, error) {
	if a.ownedMetadataDecoder == nil {
		edges, err := a.metadataDecoder(class, image)
		return edges, nil, err
	}
	scratch, err := a.ownedMetadataDecoder(class, image, &a.metadata)
	if err != nil {
		return nil, nil, err
	}
	return scratch.Edges(), scratch, nil
}

type MetadataEdge struct {
	PageID uint64
	Class  Class
	Digest [32]byte
}

func validMetadataEdge(parent, child Class) bool {
	switch parent {
	case Record:
		return child == Directory || child == Dependency || child == Manifest
	case Dependency:
		return child == Dependency
	case Manifest:
		return child == Manifest
	}
	return false
}
func (a *Arena) SetMetadataDecoder(decoder MetadataDecoder) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if decoder == nil || a.metadataDecoder != nil {
		return ErrFormat
	}
	a.metadataDecoder = decoder
	return nil
}

// SealMetadata constructs genuine immutable record/dependency banks. The codec
// derives custody from the actual page, rather than a caller supplied shadow
// list. Every referenced bank is already sealed; this gives the physical DAG
// a construction order and excludes cycles, self edges and unowned references.
func (a *Arena) SealMetadata(r Ref, image []byte, w *iterator.OrdinalScanWork) (bool, error) {
	bytes := uint64(16*page.PageSize) + 2*uint64(unsafe.Sizeof(bank{}))
	if !reserve(w, 1, bytes) {
		return false, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	b := a.valid(r)
	if b == nil || b.class < Dependency || b.class > Record || b.sealed || a.metadataDecoder == nil || len(image) != page.PageSize || page.DecodeHeader(image).PageID != r.PageID || !page.VerifyChecksumNonMutating(image) {
		return false, ErrFormat
	}
	edges, scratch, e := a.decodeMetadata(b.class, image)
	if e != nil {
		return false, e
	}
	if scratch != nil {
		defer scratch.Close()
	}
	// A page cannot encode more independent children than its bytes. The format
	// decoder may impose a smaller canonical bound for each page type.
	if len(edges) > page.PageSize/8 {
		return false, ErrFormat
	}
	edgeBytes := uint64(len(edges)) * (2*uint64(unsafe.Sizeof(bank{})) + 2*uint64(unsafe.Sizeof(Ref{})))
	if !reserve(w, uint64(len(edges)), edgeBytes) {
		return false, nil
	}
	var publicationDirectory *bank
	for _, edge := range edges {
		child := a.slot(edge.PageID)
		if !validMetadataEdge(b.class, edge.Class) || child == nil || child.refs == 0 || !child.sealed || child.class != edge.Class || child.refs == ^uint64(0) || (edge.Digest != ([32]byte{}) && child.digest != edge.Digest) {
			return false, ErrStale
		}
		if b.class == Record && b.pair != 0 && edge.Class == Directory && Local(edge.PageID) == b.pair && child.pair == b.pair {
			publicationDirectory = child
		}
	}
	if b.class == Record && b.pair != 0 && publicationDirectory == nil {
		return false, ErrFormat
	}
	charge := a.edgeCharge(len(edges))
	if err := a.metadata.Add(charge); err != nil {
		return false, err
	}
	adopted := false
	defer func() {
		if !adopted {
			a.metadata.Remove(charge)
		}
	}()
	refs := make([]Ref, len(edges))
	var physical []byte
	var ok bool
	if b.class == Record {
		physical, ok, e = a.pager.WritePublicationRecordViewWithWork(Local(r.PageID), image, w)
	} else {
		physical, ok, e = a.pager.WriteViewWithWork(Local(r.PageID), image, w)
	}
	if !ok || e != nil {
		return ok, e
	}
	for i, edge := range edges {
		child := a.slot(edge.PageID)
		child.refs++
		refs[i] = Ref{edge.PageID, child.incarnation}
	}
	b.edges = refs
	b.metadataCharge += charge
	adopted = true
	b.image = physical
	b.digest = sha256.Sum256(physical)
	b.sealed = true
	// A separately constructed ordinary pair becomes an exportable publication
	// only when its actual sealed record adopts the matching directory edge.
	if publicationDirectory != nil {
		b.privatePublication = false
		publicationDirectory.privatePublication = false
	}
	a.counters.EdgeReads += uint64(len(edges))
	a.counters.EdgeWrites += uint64(len(edges))
	a.counters.PagesWritten++
	a.counters.Bytes += bytes + edgeBytes
	return true, nil
}

// Identity returns the physically sealed bank identity without observing a
// mutable current head. The containing root/claim handle must remain owned.
func (a *Arena) Identity(r Ref, w *iterator.OrdinalScanWork) (Class, [32]byte, bool, error) {
	if !reserve(w, 1, 2*uint64(unsafe.Sizeof(bank{}))) {
		return 0, [32]byte{}, false, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	b := a.valid(r)
	if b == nil || !b.sealed {
		return 0, [32]byte{}, false, ErrStale
	}
	a.counters.EdgeReads++
	a.counters.Bytes += 2 * uint64(unsafe.Sizeof(bank{}))
	return b.class, b.digest, true, nil
}
