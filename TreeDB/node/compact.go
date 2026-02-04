package node

import (
	"encoding/binary"

	"github.com/snissn/gomap/TreeDB/page"
)

// Compact rebuilds the node's slotted-page layout in-place, removing heap holes
// created by overwrites/splits. It preserves logical entry order (directory
// order) and updates offsets and checksum.
func (n *Node) Compact() error {
	if n.Type() != page.PageTypeLeaf && n.Type() != page.PageTypeInternal {
		return ErrInvalidType
	}

	count := n.Count()
	if count == 0 {
		// Nothing to do.
		n.UpdateChecksum()
		return nil
	}

	newData := make([]byte, page.PageSize)

	// Preserve page ID/type/count; checksum is recomputed at the end.
	pageID := n.PageID()
	copy(newData[:NodeHeaderSize], n.data[:NodeHeaderSize])
	binary.LittleEndian.PutUint64(newData[0:8], pageID)
	binary.LittleEndian.PutUint16(newData[12:14], n.rawFlags())
	binary.LittleEndian.PutUint16(newData[14:16], count)

	dirEnd := NodeHeaderSize + int(count)*DirectoryEntrySize
	heapStart := int(page.PageSize)

	for i := uint16(0); i < count; i++ {
		off, err := n.getOffset(i)
		if err != nil {
			return err
		}
		if off == 0 || int(off) >= len(n.data) {
			return ErrCorruptedNode
		}

		entryLen, err := entryLength(n, int(off))
		if err != nil {
			return err
		}

		heapStart -= entryLen
		if heapStart < dirEnd {
			// This should never happen for a valid node. Treat as corruption.
			return ErrCorruptedNode
		}

		copy(newData[heapStart:heapStart+entryLen], n.data[int(off):int(off)+entryLen])
		binary.LittleEndian.PutUint16(newData[NodeHeaderSize+int(i)*2:], uint16(heapStart))
	}

	copy(n.data, newData)
	n.setRawFlags(binary.LittleEndian.Uint16(n.data[12:14]))
	n.count = binary.LittleEndian.Uint16(n.data[14:16])
	n.UpdateChecksum()
	return nil
}

func entryLength(n *Node, offset int) (int, error) {
	switch n.Type() {
	case page.PageTypeLeaf:
		layout, err := n.leafEntryLayoutAt(offset)
		if err != nil {
			return 0, err
		}
		flags := layout.flags
		valSize := layout.valLen
		if flags&FlagPointer != 0 {
			if n.leafPackedValuePtr() {
				valSize = page.PackedValuePtrSize
			} else {
				valSize = page.ValuePtrSize
			}
		}
		keyEnd := layout.keyOff + layout.keyLen
		valEnd := layout.valOff + valSize
		entryLen := keyEnd
		if valEnd > entryLen {
			entryLen = valEnd
		}
		if offset+entryLen > len(n.data) {
			return 0, ErrCorruptedNode
		}
		return entryLen, nil

	case page.PageTypeInternal:
		if offset+2+8 > len(n.data) {
			return 0, ErrCorruptedNode
		}
		keyLen := int(binary.LittleEndian.Uint16(n.data[offset : offset+2]))
		if keyLen < 0 {
			return 0, ErrCorruptedNode
		}
		if offset+2+8+keyLen > len(n.data) {
			return 0, ErrCorruptedNode
		}
		return 2 + 8 + keyLen, nil

	default:
		return 0, ErrInvalidType
	}
}
