package pager

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/page"
)

// PrimaryReadRootLocalBaseV6 separates immutable RAM read addresses from all
// physical component IDs. These addresses are never file offsets or recovery
// operands. The primary arena owns each registered image and its child custody.
const PrimaryReadRootLocalBaseV6 uint64 = 1 << 60

// RegisterPrimaryReadRootV6 transfers a sealed immutable image to the shared
// point/iterator reader. It installs no file page and grants no write authority.
// The owner must keep the image immutable until its actual last ref is retired.
func (p *Pager) RegisterPrimaryReadRootV6(local uint64, image []byte, w *iterator.OrdinalScanWork) (bool, error) {
	if ok, err := pagerReserve(w, 1, uint64(2*page.PageSize+128)); !ok || err != nil {
		return ok, err
	}
	if p == nil || local < PrimaryReadRootLocalBaseV6 || local >= page.PrimaryBankNamespace || len(image) != page.PageSize || page.DecodeHeader(image).PageID != page.PrimaryBankNamespace+local || !page.VerifyChecksumNonMutating(image) {
		return false, errors.New("pager: invalid immutable primary read root")
	}
	if _, loaded := p.immutableReadRoots.LoadOrStore(local, image); loaded {
		return false, errors.New("pager: duplicate immutable primary read address")
	}
	return true, nil
}

// RemovePrimaryReadRootV6 retires the address after the owner's incoming refs
// and outgoing edges have reached zero. A loaded Go slice cannot be reclaimed
// underneath an in-flight read; read-root custody still gates component reuse.
func (p *Pager) RemovePrimaryReadRootV6(local uint64, w *iterator.OrdinalScanWork) (bool, error) {
	if ok, err := pagerReserve(w, 1, 128); !ok || err != nil {
		return ok, err
	}
	if p == nil || local < PrimaryReadRootLocalBaseV6 || local >= page.PrimaryBankNamespace {
		return false, errors.New("pager: invalid read address retirement")
	}
	p.immutableReadRoots.Delete(local)
	return true, nil
}
