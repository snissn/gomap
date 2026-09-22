package collections

import (
	"encoding/binary"
	"math"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

// Each root value may become a singleton raw vlog frame. Grouping and selected
// shrink-only compression cannot exceed this per-entry framing reservation.
const typedGraphRootEntryFrameBytes int64 = valuelog.HeaderSize + valuelog.FrameHeaderSize + 8 + 2*4

func addTypedGraphEncodedBytes(total *int64, value int64) error {
	if value < 0 || *total < 0 || value > math.MaxInt64-*total {
		return errTypedGraphOverlayFoldNeeded
	}
	*total += value
	return nil
}

// typedGraphTextInsertEncodedBound consumes already prepared native V2 text,
// not JSON. A receipt is indivisible at flush; unknown starting ordinal can
// touch at most ceil((rows+127)/128) blocks. tailBytes is the current raw docmap
// tail; pendingIDBytes covers IDs admitted ahead of this receipt. Both remain
// deliberately conservative when multiple whole receipts share a flush.
// The result covers key/value payload and singleton framing, not COW pages.
func typedGraphTextInsertEncodedBound(def TextIndexDefinition, ids []columnWriteDocument, states []textDocumentStateValue, tailBytes, pendingIDBytes int64) (int64, error) {
	if def.Version != TextIndexVersionV2 || len(def.Fields) == 0 || len(def.Fields) > int(textV2PostingBlockMaxFieldCount) || len(ids) != len(states) || tailBytes < 0 || pendingIDBytes < 0 {
		return 0, ErrHybridSearchUnsupported
	}
	if len(ids) == 0 {
		return 0, nil
	}
	const width = int64(binary.MaxVarintLen64)
	var total int64
	entry := func(key, value int64) error {
		for _, n := range [...]int64{key, value, typedGraphRootEntryFrameBytes} {
			if err := addTypedGraphEncodedBytes(&total, n); err != nil {
				return err
			}
		}
		return nil
	}
	// Whole blocks are charged at maximum scalar widths; only arbitrary ID
	// payload requires actual tail/pending lengths rather than a fixed bound.
	blockSize := int64(textV2DefaultDocMapBlockSize)
	rows := int64(len(ids))
	if rows > math.MaxInt64-2*(blockSize-1) {
		return 0, errTypedGraphOverlayFoldNeeded
	}
	blocks := (rows + 2*(blockSize-1)) / blockSize
	blockBytes := 2*(10+typedGraphRootEntryFrameBytes) + (1 + 3*width) + (1 + 4*width) + blockSize*((3*width+1)+(2*width+1+int64(len(def.Fields))*width))
	if blocks > math.MaxInt64/blockBytes {
		return 0, errTypedGraphOverlayFoldNeeded
	}
	for _, n := range [...]int64{blocks * blockBytes, tailBytes, pendingIDBytes} {
		if err := addTypedGraphEncodedBytes(&total, n); err != nil {
			return 0, err
		}
	}
	if err := entry(2, 1+9*width); err != nil { // status
		return 0, err
	}
	if err := entry(2, 1+2*width); err != nil { // corpus stats
		return 0, err
	}
	for _, field := range def.Fields {
		if err := entry(2+int64(encodedTextStringLen(field.Field)), 1+3*width); err != nil {
			return 0, err
		}
	}
	// One batch-local field-width scratch. Singleton field/term bounds dominate
	// merged postings and positions without reconstructing the posting builder.
	frequencies := make([]uint32, len(def.Fields))
	posting := textV2PostingBlockValue{Summary: textV2PostingBlockSummary{MaxFieldTermFrequencies: frequencies}}
	postingEntries := [...]textV2PostingBlockEntry{{FieldFrequencies: frequencies}}
	postingBytes := int64(estimateTextV2PostingBlockValueLen(posting, postingEntries[:], uint32(len(def.Fields))))
	for i, doc := range ids {
		if err := entry(2+int64(len(doc.ID)), 2+2*width); err != nil {
			return 0, err
		}
		if err := addTypedGraphEncodedBytes(&total, int64(len(doc.ID))); err != nil { // docmap ID
			return 0, err
		}
		for _, field := range states[i].Fields {
			fieldIndex := textV2FieldIndex(def, field.Field)
			if fieldIndex < 0 {
				return 0, ErrHybridSearchUnsupported
			}
			for _, term := range field.Terms {
				if term.Frequency == 0 {
					continue
				}
				keyBytes := int64(encodedTextStringLen(term.Term))
				if err := entry(2+keyBytes+16, postingBytes); err != nil {
					return 0, err
				}
				if err := entry(2+keyBytes, 1+4*width); err != nil { // term stats
					return 0, err
				}
				if def.StorePositions {
					offsets := term.Offsets
					if !def.StoreOffsets {
						offsets = nil
					}
					fields := [...]textV2PositionFieldValue{{FieldIndex: uint32(fieldIndex), Frequency: term.Frequency, Positions: term.Positions, Offsets: offsets}}
					value := textV2PositionValue{Term: term.Term}
					if err := entry(10+keyBytes, int64(estimateTextV2PositionValueLen(value, fields[:]))); err != nil {
						return 0, err
					}
				}
			}
		}
	}
	return total, nil
}
