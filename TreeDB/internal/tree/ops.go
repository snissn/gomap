package tree

import (
	"bytes"

	"treedb/internal/page"
)

func buildLeafPage(buf []byte, pid page.PageID, entries []leafKV) error {
	lp, err := page.InitLeafPage(buf, pid)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if _, err := lp.Set(e.key, e.entry.Flags, e.entry.InlineValue, e.entry.Ptr); err != nil {
			return err
		}
	}
	h, body, _ := page.SplitPage(buf)
	h.SetBodyCRC(body)
	return nil
}

func buildInternalPage(buf []byte, pid page.PageID, entries []internalKV) error {
	ip, err := page.InitInternalPage(buf, pid)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if _, err := ip.Set(e.key, e.child); err != nil {
			return err
		}
	}
	h, body, _ := page.SplitPage(buf)
	h.SetBodyCRC(body)
	return nil
}

// cowSet performs a copy-on-write update starting at pid.
// Returns newPid (left), optional splitKey/splitPid (right), retired pages, and old leaf entry (if any).
func (t *Tree) cowSet(pid page.PageID, key []byte, val LeafEntry) (page.PageID, []byte, page.PageID, []page.PageID, *LeafEntry, error) {
	ref, err := t.pager.ReadPageRef(pid)
	if err != nil {
		return 0, nil, 0, nil, nil, err
	}
	defer ref.Release()
	oldBuf := ref.Bytes()

	h, _, err := page.SplitPage(oldBuf)
	if err != nil {
		return 0, nil, 0, nil, nil, err
	}
	switch h.Flags {
	case page.PageTypeLeaf:
		return t.cowSetLeaf(pid, oldBuf, key, val)
	case page.PageTypeInternal:
		return t.cowSetInternal(pid, oldBuf, key, val)
	default:
		return 0, nil, 0, nil, nil, ErrCorrupt
	}
}

func (t *Tree) cowSetLeaf(oldPid page.PageID, oldBuf []byte, key []byte, val LeafEntry) (page.PageID, []byte, page.PageID, []page.PageID, *LeafEntry, error) {
	// Capture old entry (if present) for dead-byte accounting.
	var oldEnt *LeafEntry
	oldPage, err := page.OpenLeafPage(oldBuf)
	if err != nil {
		return 0, nil, 0, nil, nil, err
	}
	if idx, found, err := oldPage.Search(key); err != nil {
		return 0, nil, 0, nil, nil, err
	} else if found {
		_, flags, inline, ptr, err := oldPage.EntryAt(idx)
		if err != nil {
			return 0, nil, 0, nil, nil, err
		}
		e := LeafEntry{Flags: flags, InlineValue: inline, Ptr: ptr}
		oldEnt = &e
	}

	leftPid, err := t.pager.AllocPage()
	if err != nil {
		return 0, nil, 0, nil, oldEnt, err
	}
	retired := []page.PageID{oldPid}

	// Fast path: clone old page and apply in-page Set.
	leftBuf := make([]byte, page.PageSize)
	copy(leftBuf, oldBuf)
	h, body, err := page.SplitPage(leftBuf)
	if err != nil {
		return 0, nil, 0, nil, oldEnt, err
	}
	h.PageID = leftPid
	lp, err := page.OpenLeafPage(leftBuf)
	if err != nil {
		return 0, nil, 0, nil, oldEnt, err
	}
	if _, err := lp.Set(key, val.Flags, val.InlineValue, val.Ptr); err == nil {
		h.SetBodyCRC(body)
		if err := t.pager.WritePage(leftPid, leftBuf); err != nil {
			return 0, nil, 0, nil, oldEnt, err
		}
		return leftPid, nil, 0, retired, oldEnt, nil
	} else if !isPageFull(err) {
		return 0, nil, 0, nil, oldEnt, err
	}

	// Split leaf (rare): fall back to full rebuild.
	entries, err := parseLeafEntries(oldBuf)
	if err != nil {
		return 0, nil, 0, nil, oldEnt, err
	}
	idx, found := findLeafIndex(entries, key)
	if found {
		entries[idx].entry = val
	} else {
		rec := leafKV{key: append([]byte(nil), key...), entry: val}
		entries = append(entries, leafKV{})
		copy(entries[idx+1:], entries[idx:])
		entries[idx] = rec
	}

	if err := buildLeafPage(leftBuf, leftPid, entries); err == nil {
		if err := t.pager.WritePage(leftPid, leftBuf); err != nil {
			return 0, nil, 0, nil, oldEnt, err
		}
		return leftPid, nil, 0, retired, oldEnt, nil
	} else if !isPageFull(err) {
		return 0, nil, 0, nil, oldEnt, err
	}

	rightPid, err := t.pager.AllocPage()
	if err != nil {
		return 0, nil, 0, nil, oldEnt, err
	}
	mid := len(entries) / 2
	if mid == 0 {
		mid = 1
	}
	if mid == len(entries) {
		mid = len(entries) - 1
	}
	leftEntries := entries[:mid]
	rightEntries := entries[mid:]
	rightBuf := make([]byte, page.PageSize)
	leftBuf = make([]byte, page.PageSize)
	if err := buildLeafPage(leftBuf, leftPid, leftEntries); err != nil {
		return 0, nil, 0, nil, oldEnt, err
	}
	if err := buildLeafPage(rightBuf, rightPid, rightEntries); err != nil {
		return 0, nil, 0, nil, oldEnt, err
	}
	if err := t.pager.WritePage(leftPid, leftBuf); err != nil {
		return 0, nil, 0, nil, oldEnt, err
	}
	if err := t.pager.WritePage(rightPid, rightBuf); err != nil {
		return 0, nil, 0, nil, oldEnt, err
	}
	return leftPid, rightEntries[0].key, rightPid, retired, oldEnt, nil
}

func (t *Tree) cowSetInternal(oldPid page.PageID, oldBuf []byte, key []byte, val LeafEntry) (page.PageID, []byte, page.PageID, []page.PageID, *LeafEntry, error) {
	oldIP, err := page.OpenInternalPage(oldBuf)
	if err != nil {
		return 0, nil, 0, nil, nil, err
	}
	if oldIP.Count() == 0 {
		return 0, nil, 0, nil, nil, ErrCorrupt
	}

	insIdx, found, err := oldIP.Search(key)
	if err != nil {
		return 0, nil, 0, nil, nil, err
	}
	childIdx := insIdx
	if !found {
		if insIdx == 0 {
			childIdx = 0
		} else {
			childIdx = insIdx - 1
		}
	}
	childKey, oldChild, err := oldIP.EntryAt(childIdx)
	if err != nil {
		return 0, nil, 0, nil, nil, err
	}

	var oldMin []byte
	if childIdx == 0 {
		oldMin = childKey
	}

	newChild, childSplitKey, childSplitPid, childRetired, oldEnt, err := t.cowSet(oldChild, key, val)
	if err != nil {
		return 0, nil, 0, nil, oldEnt, err
	}
	retired := append(childRetired, oldPid)

	var newMin []byte
	minChanged := false
	if childIdx == 0 {
		newMin, err = t.minKey(newChild)
		if err != nil {
			return 0, nil, 0, nil, oldEnt, err
		}
		minChanged = !bytes.Equal(newMin, oldMin)
	}

	leftPid, err := t.pager.AllocPage()
	if err != nil {
		return 0, nil, 0, nil, oldEnt, err
	}

	// Fast path: clone and update in place when min key unchanged.
	if !minChanged {
		leftBuf := make([]byte, page.PageSize)
		copy(leftBuf, oldBuf)
		h, body, err := page.SplitPage(leftBuf)
		if err != nil {
			return 0, nil, 0, nil, oldEnt, err
		}
		h.PageID = leftPid
		ipNew, err := page.OpenInternalPage(leftBuf)
		if err != nil {
			return 0, nil, 0, nil, oldEnt, err
		}
		// Update existing child pointer.
		if _, err := ipNew.Set(childKey, newChild); err != nil {
			return 0, nil, 0, nil, oldEnt, err
		}
		// Insert split child if needed.
		if childSplitKey != nil {
			if _, err := ipNew.Set(childSplitKey, childSplitPid); err != nil {
				if !isPageFull(err) {
					return 0, nil, 0, nil, oldEnt, err
				}
				// fall through to rebuild/split
			} else {
				childSplitKey = nil
			}
		}
		if childSplitKey == nil {
			h.SetBodyCRC(body)
			if err := t.pager.WritePage(leftPid, leftBuf); err != nil {
				return 0, nil, 0, nil, oldEnt, err
			}
			return leftPid, nil, 0, retired, oldEnt, nil
		}
		// page full on insertion; rebuild below
	}

	// Rebuild/split internal (rare): fall back to slice rebuild.
	entries, err := parseInternalEntries(oldBuf)
	if err != nil {
		return 0, nil, 0, nil, oldEnt, err
	}
	if len(entries) == 0 {
		return 0, nil, 0, nil, oldEnt, ErrCorrupt
	}

	entries[childIdx].child = newChild
	if childIdx == 0 {
		entries[0].key = newMin
	}
	if childSplitKey != nil {
		ins := internalKV{key: append([]byte(nil), childSplitKey...), child: childSplitPid}
		insIdx2, _ := findInternalIndex(entries, ins.key)
		entries = append(entries, internalKV{})
		copy(entries[insIdx2+1:], entries[insIdx2:])
		entries[insIdx2] = ins
	}

	leftBuf := make([]byte, page.PageSize)
	if err := buildInternalPage(leftBuf, leftPid, entries); err == nil {
		if err := t.pager.WritePage(leftPid, leftBuf); err != nil {
			return 0, nil, 0, nil, oldEnt, err
		}
		return leftPid, nil, 0, retired, oldEnt, nil
	} else if !isPageFull(err) {
		return 0, nil, 0, nil, oldEnt, err
	}

	rightPid, err := t.pager.AllocPage()
	if err != nil {
		return 0, nil, 0, nil, oldEnt, err
	}
	mid := len(entries) / 2
	if mid == 0 {
		mid = 1
	}
	if mid == len(entries) {
		mid = len(entries) - 1
	}
	leftEntries := entries[:mid]
	rightEntries := entries[mid:]
	rightBuf := make([]byte, page.PageSize)
	leftBuf = make([]byte, page.PageSize)
	if err := buildInternalPage(leftBuf, leftPid, leftEntries); err != nil {
		return 0, nil, 0, nil, oldEnt, err
	}
	if err := buildInternalPage(rightBuf, rightPid, rightEntries); err != nil {
		return 0, nil, 0, nil, oldEnt, err
	}
	if err := t.pager.WritePage(leftPid, leftBuf); err != nil {
		return 0, nil, 0, nil, oldEnt, err
	}
	if err := t.pager.WritePage(rightPid, rightBuf); err != nil {
		return 0, nil, 0, nil, oldEnt, err
	}
	return leftPid, rightEntries[0].key, rightPid, retired, oldEnt, nil
}

func findInternalIndex(entries []internalKV, key []byte) (int, bool) {
	for i := 0; i < len(entries); i++ {
		if bytesCompare(entries[i].key, key) >= 0 {
			if bytesCompare(entries[i].key, key) == 0 {
				return i, true
			}
			return i, false
		}
	}
	return len(entries), false
}

func bytesCompare(a, b []byte) int { return bytesCompareImpl(a, b) }

func bytesCompareImpl(a, b []byte) int {
	// local copy of bytes.Compare to avoid importing bytes in ops.go.
	la, lb := len(a), len(b)
	min := la
	if lb < min {
		min = lb
	}
	for i := 0; i < min; i++ {
		if a[i] == b[i] {
			continue
		}
		if a[i] < b[i] {
			return -1
		}
		return 1
	}
	switch {
	case la < lb:
		return -1
	case la > lb:
		return 1
	default:
		return 0
	}
}
