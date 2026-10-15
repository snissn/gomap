package node

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/page"
)

// ErrBuilderWorkFormat refuses formats whose finish scratch does not yet have
// a retained continuation. No entry or format flag is changed on this refusal.
var ErrBuilderWorkFormat = errors.New("node: bounded builder format needs retained scratch")

func builderReserve(w *iterator.OrdinalScanWork, r, b uint64) (bool, error) {
	if w == nil {
		return true, nil
	}
	if r > w.RecordLimit || b > w.ByteLimit {
		return false, &iterator.OrdinalUnitTooLarge{Records: r, Bytes: b}
	}
	return w.Reserve(r, b), nil
}

// AddLeafEntryWithRevisionWithWork uses the ordinary encoder. Plain and prefix
// pages use only the original page and embedded previous-key storage. Prefix
// probes, encoded stores and both key copies are admitted before observation;
// refusal leaves the logical builder and its format flags unchanged.
func (b *Builder) AddLeafEntryWithRevisionWithWork(key, value []byte, flags byte, ptr page.ValuePtr, revision page.EntryRevision, w *iterator.OrdinalScanWork) (bool, error) {
	if b.pType != page.PageTypeLeaf {
		return false, ErrInvalidType
	}
	if b.leafColumnar && b.leafPrefixCompression {
		return b.addColumnarPrefixWithWork(key, value, flags, ptr, revision, 0, w)
	}
	if w != nil {
		if b.leafColumnar || (b.leafPrefixCompression && len(key) > len(b.leafPrevKeyBuf)) {
			return false, ErrBuilderWorkFormat
		}
		probes := 0
		if b.leafPrefixCompression && b.leafIndex%leafPrefixRestartInterval != 0 {
			probes = min(len(key), len(b.leafPrevKey))
		}
		// One unit per possible compared byte, header/slot, payload, revision,
		// and builder/previous-key transition. The byte bound includes ptr encodings.
		bytes := uint64(2*probes + 3*len(key) + len(value) + page.ValuePtrSize + page.EntryRevisionSize + 64)
		if ok, e := builderReserve(w, uint64(probes+6), bytes); !ok || e != nil {
			return false, e
		}
	}
	if err := b.addLeafEntryWithRevision(key, value, flags, ptr, revision); err != nil {
		return false, err
	}
	return true, nil
}

// FinishNoNodeWithWork shares the original header and checksum writer, without
// allocating a Node or performing a columnar scratch pass.
func (b *Builder) FinishNoNodeWithWork(w *iterator.OrdinalScanWork) (bool, error) {
	if b.pType == page.PageTypeLeaf && b.leafColumnar && b.leafPrefixCompression {
		ready, e := b.packLeafColumnarPrefixV2(w)
		if !ready || e != nil {
			return false, e
		}
		if ok, e := builderReserve(w, 2, uint64(len(b.data)+NodeHeaderSize)); !ok || e != nil {
			return false, e
		}
		b.finalizeHeader(false)
		return true, nil
	}
	if w != nil && (b.pType != page.PageTypeLeaf || b.leafColumnar) {
		return false, ErrBuilderWorkFormat
	}
	if ok, e := builderReserve(w, 2, uint64(len(b.data)+NodeHeaderSize)); !ok || e != nil {
		return false, e
	}
	b.finalize()
	return true, nil
}

// AddLeafEntryWithRevisionWithWorkAndReserve preserves the zipper's ordinary
// soft-fill reserve after the same admitted prefix comparison, without a hidden
// second prefix scan. The first entry always uses the ordinary hard capacity.
func (b *Builder) AddLeafEntryWithRevisionWithWorkAndReserve(key, value []byte, flags byte, ptr page.ValuePtr, revision page.EntryRevision, reserveBytes int, w *iterator.OrdinalScanWork) (bool, error) {
	if b.pType != page.PageTypeLeaf {
		return false, ErrInvalidType
	}
	if b.leafColumnar && b.leafPrefixCompression {
		return b.addColumnarPrefixWithWork(key, value, flags, ptr, revision, reserveBytes, w)
	}
	if reserveBytes != 0 && b.count != 0 {
		return false, ErrBuilderWorkFormat
	}
	return b.AddLeafEntryWithRevisionWithWork(key, value, flags, ptr, revision, w)
}
