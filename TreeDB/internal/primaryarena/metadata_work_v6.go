package primaryarena

import (
	"crypto/sha256"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/page"
)

// sealMetadataWithWorkV6 uses the original bank and codec. Private edge backing
// is installed on that bank before any fallible physical write, so retry/abort
// retains its original accounting without a synchronous error refund. Only the
// fixed record/manifest decoder is supported; dependency pages refuse before
// decoding or allocating. All aliases below stay under the original a.mu.
func (a *Arena) sealMetadataWithWorkV6(r Ref, image []byte, w *iterator.OrdinalScanWork) (bool, error) {
	bytes := uint64(16*page.PageSize) + 2*uint64(unsafe.Sizeof(bank{})) + 2*uint64(unsafe.Sizeof(a.metadataDecodeScratch)) + 2*uint64(unsafe.Sizeof([2]*bank{}))
	if !w.Reserve(1, bytes) {
		return false, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	b, ready := a.validWithWorkV6(r, w)
	if !ready {
		return false, nil
	}
	if b == nil || (b.class != Manifest && b.class != Record) || b.sealed || a.borrowedMetadataDecoder == nil {
		return false, retainedalloc.ErrWorkRequired
	}
	if len(image) != page.PageSize || page.DecodeHeader(image).PageID != r.PageID || !page.VerifyChecksumNonMutating(image) {
		return false, ErrFormat
	}
	defer clear(a.metadataDecodeScratch[:])
	edges, err := a.borrowedMetadataDecoder(b.class, image, a.metadataDecodeScratch[:0])
	if err != nil {
		return false, err
	}
	if len(edges) > len(a.metadataDecodeScratch) {
		return false, ErrFormat
	}
	edgeBytes := uint64(len(edges)) * (2*uint64(unsafe.Sizeof(bank{})) + 2*uint64(unsafe.Sizeof(Ref{})))
	if !w.Reserve(uint64(len(edges)), edgeBytes) {
		return false, nil
	}
	var children [2]*bank
	var publicationDirectory *bank
	for i, edge := range edges {
		child, ok := a.slotWithWorkV6(edge.PageID, w)
		if !ok {
			return false, nil
		}
		if !validMetadataEdge(b.class, edge.Class) || child == nil || child.refs == 0 || !child.sealed || child.class != edge.Class || child.refs == ^uint64(0) || (edge.Digest != ([32]byte{}) && child.digest != edge.Digest) {
			return false, ErrStale
		}
		children[i] = child
		if b.class == Record && b.pair != 0 && edge.Class == Directory && Local(edge.PageID) == b.pair && child.pair == b.pair {
			publicationDirectory = child
		}
	}
	if b.class == Record && b.pair != 0 && publicationDirectory == nil {
		return false, ErrFormat
	}
	if len(b.edges) != 0 {
		return false, ErrStale
	}
	if len(edges) > cap(b.edges) {
		// Retrying an already admitted different geometry is unsupported before
		// replacement allocation/refund; the original private bank remains intact.
		if cap(b.edges) != 0 {
			return false, retainedalloc.ErrWorkRequired
		}
		charge := a.edgeCharge(len(edges))
		if ready, err := retainedalloc.AddWithOrdinalWork(&a.metadata, charge, w); !ready || err != nil {
			return false, err
		}
		b.edges = make([]Ref, 0, len(edges))
		b.metadataCharge += charge
	}
	var physical []byte
	var ok bool
	if b.class == Record {
		physical, ok, err = a.pager.WritePublicationRecordViewWithWork(Local(r.PageID), image, w)
	} else {
		physical, ok, err = a.pager.WriteViewWithWork(Local(r.PageID), image, w)
	}
	if !ok || err != nil {
		return false, err
	}
	// Every lookup, validation and edge-install visit was admitted before the
	// physical write. No fallible operation or yield follows that first effect.
	refs := b.edges[:len(edges)]
	for i, edge := range edges {
		child := children[i]
		child.refs++
		refs[i] = Ref{edge.PageID, child.incarnation}
	}
	b.edges = refs
	b.image = physical
	b.digest = sha256.Sum256(physical)
	b.sealed = true
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
