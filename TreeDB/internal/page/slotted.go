package page

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sort"
)

const (
	slotHeaderSize = 4 // bytes reserved at start of body
)

var (
	ErrPageFull      = errors.New("page: not enough free space")
	ErrPageCorrupt   = errors.New("page: corrupt slotted page")
	ErrWrongPageType = errors.New("page: wrong page type")
)

// heapTop returns the current heap end (offset within body).
func heapTop(body []byte) uint16 {
	if len(body) < slotHeaderSize {
		return 0
	}
	return binary.LittleEndian.Uint16(body[0:2])
}

func setHeapTop(body []byte, v uint16) {
	binary.LittleEndian.PutUint16(body[0:2], v)
}

func dirStart(body []byte, count int) int {
	return len(body) - count*2
}

func dirEntry(body []byte, count int, i int) (uint16, error) {
	if i < 0 || i >= count {
		return 0, ErrPageCorrupt
	}
	start := dirStart(body, count)
	offPos := start + i*2
	if offPos+2 > len(body) {
		return 0, ErrPageCorrupt
	}
	return binary.LittleEndian.Uint16(body[offPos : offPos+2]), nil
}

func setDirEntry(body []byte, count int, i int, off uint16) error {
	if i < 0 || i >= count {
		return ErrPageCorrupt
	}
	start := dirStart(body, count)
	offPos := start + i*2
	if offPos+2 > len(body) {
		return ErrPageCorrupt
	}
	binary.LittleEndian.PutUint16(body[offPos:offPos+2], off)
	return nil
}

func insertDirEntry(body []byte, count int, i int, off uint16) error {
	if i < 0 || i > count {
		return ErrPageCorrupt
	}
	oldStart := dirStart(body, count)
	newStart := oldStart - 2
	if newStart < int(heapTop(body)) {
		return ErrPageFull
	}
	// Shift directory down by 2 bytes to make room for new entry.
	copy(body[newStart:newStart+count*2], body[oldStart:oldStart+count*2])
	// Shift entries after i to the right.
	copy(body[newStart+(i+1)*2:newStart+(count+1)*2], body[newStart+i*2:newStart+count*2])
	binary.LittleEndian.PutUint16(body[newStart+i*2:newStart+i*2+2], off)
	return nil
}

func removeDirEntry(body []byte, count int, i int) error {
	if i < 0 || i >= count {
		return ErrPageCorrupt
	}
	oldStart := dirStart(body, count)
	newStart := oldStart + 2
	// Rebuild directory in-place at newStart, skipping entry i.
	for j := 0; j < count-1; j++ {
		srcIdx := j
		if j >= i {
			srcIdx = j + 1
		}
		val := binary.LittleEndian.Uint16(body[oldStart+srcIdx*2 : oldStart+srcIdx*2+2])
		binary.LittleEndian.PutUint16(body[newStart+j*2:newStart+j*2+2], val)
	}
	// Zero out the freed 2 bytes for determinism.
	binary.LittleEndian.PutUint16(body[oldStart:oldStart+2], 0)
	return nil
}

func freeSpace(body []byte, count int) int {
	return dirStart(body, count) - int(heapTop(body))
}

// initSlottedPage initializes a page buffer as a slotted page of the given type.
func initSlottedPage(buf []byte, pid PageID, flags PageFlags) (*Header, []byte, error) {
	if len(buf) != PageSize {
		return nil, nil, ErrPageTooSmall
	}
	h, body, err := SplitPage(buf)
	if err != nil {
		return nil, nil, err
	}
	*h = Header{
		PageID: pid,
		CRC:    0,
		Flags:  flags,
		Count:  0,
	}
	for i := range body {
		body[i] = 0
	}
	setHeapTop(body, slotHeaderSize)
	return h, body, nil
}

// binarySearchOffsets performs binary search over directory, returning insertion point.
// keyAt must return the key bytes for a given heap offset.
func binarySearchOffsets(body []byte, count int, target []byte, keyAt func(uint16) ([]byte, error)) (int, bool, error) {
	lo, hi := 0, count
	for lo < hi {
		mid := (lo + hi) / 2
		off, err := dirEntry(body, count, mid)
		if err != nil {
			return 0, false, err
		}
		k, err := keyAt(off)
		if err != nil {
			return 0, false, err
		}
		cmp := bytes.Compare(k, target)
		if cmp < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo < count {
		off, err := dirEntry(body, count, lo)
		if err != nil {
			return 0, false, err
		}
		k, err := keyAt(off)
		if err != nil {
			return 0, false, err
		}
		if bytes.Equal(k, target) {
			return lo, true, nil
		}
	}
	return lo, false, nil
}

// physicalLengths returns a map from heap offset to entry length, based on physical order.
// It assumes there are no unreferenced gaps between referenced entries.
func physicalLengths(body []byte, offsets []uint16) (map[uint16]int, error) {
	if len(offsets) == 0 {
		return map[uint16]int{}, nil
	}
	phys := append([]uint16(nil), offsets...)
	sort.Slice(phys, func(i, j int) bool { return phys[i] < phys[j] })
	top := int(heapTop(body))
	if top < slotHeaderSize || top > len(body) {
		return nil, ErrPageCorrupt
	}
	lengths := make(map[uint16]int, len(phys))
	for i := 0; i < len(phys); i++ {
		cur := int(phys[i])
		if cur < slotHeaderSize || cur >= top {
			return nil, ErrPageCorrupt
		}
		var next int
		if i+1 < len(phys) {
			next = int(phys[i+1])
			if next < cur {
				return nil, ErrPageCorrupt
			}
		} else {
			next = top
		}
		l := next - cur
		if l <= 0 {
			return nil, ErrPageCorrupt
		}
		lengths[phys[i]] = l
	}
	return lengths, nil
}
