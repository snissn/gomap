package tree

import "treedb/internal/page"

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
// Returns newPid (left), optional splitKey/splitPid (right), and retired pages.
func (t *Tree) cowSet(pid page.PageID, key []byte, val LeafEntry) (page.PageID, []byte, page.PageID, []page.PageID, error) {
	oldBuf, err := t.pager.ReadPage(pid)
	if err != nil {
		return 0, nil, 0, nil, err
	}
	h, _, err := page.SplitPage(oldBuf)
	if err != nil {
		return 0, nil, 0, nil, err
	}
	switch h.Flags {
	case page.PageTypeLeaf:
		return t.cowSetLeaf(pid, oldBuf, key, val)
	case page.PageTypeInternal:
		return t.cowSetInternal(pid, oldBuf, key, val)
	default:
		return 0, nil, 0, nil, ErrCorrupt
	}
}

func (t *Tree) cowSetLeaf(oldPid page.PageID, oldBuf []byte, key []byte, val LeafEntry) (page.PageID, []byte, page.PageID, []page.PageID, error) {
	entries, err := parseLeafEntries(oldBuf)
	if err != nil {
		return 0, nil, 0, nil, err
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

	leftPid, err := t.pager.AllocPage()
	if err != nil {
		return 0, nil, 0, nil, err
	}
	retired := []page.PageID{oldPid}

	leftBuf := make([]byte, page.PageSize)
	if err := buildLeafPage(leftBuf, leftPid, entries); err == nil {
		if err := t.pager.WritePage(leftPid, leftBuf); err != nil {
			return 0, nil, 0, nil, err
		}
		return leftPid, nil, 0, retired, nil
	} else if !isPageFull(err) {
		return 0, nil, 0, nil, err
	}

	// Split leaf.
	rightPid, err := t.pager.AllocPage()
	if err != nil {
		return 0, nil, 0, nil, err
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
	leftBuf = make([]byte, page.PageSize)
	rightBuf := make([]byte, page.PageSize)
	if err := buildLeafPage(leftBuf, leftPid, leftEntries); err != nil {
		return 0, nil, 0, nil, err
	}
	if err := buildLeafPage(rightBuf, rightPid, rightEntries); err != nil {
		return 0, nil, 0, nil, err
	}
	if err := t.pager.WritePage(leftPid, leftBuf); err != nil {
		return 0, nil, 0, nil, err
	}
	if err := t.pager.WritePage(rightPid, rightBuf); err != nil {
		return 0, nil, 0, nil, err
	}
	return leftPid, rightEntries[0].key, rightPid, retired, nil
}

func (t *Tree) cowSetInternal(oldPid page.PageID, oldBuf []byte, key []byte, val LeafEntry) (page.PageID, []byte, page.PageID, []page.PageID, error) {
	entries, err := parseInternalEntries(oldBuf)
	if err != nil {
		return 0, nil, 0, nil, err
	}
	if len(entries) == 0 {
		return 0, nil, 0, nil, ErrCorrupt
	}

	idx := findChildIndex(entries, key)
	oldChild := entries[idx].child
	newChild, childSplitKey, childSplitPid, childRetired, err := t.cowSet(oldChild, key, val)
	if err != nil {
		return 0, nil, 0, nil, err
	}

	entries[idx].child = newChild
	// Only leftmost child can decrease min key.
	if idx == 0 {
		min, err := t.minKey(newChild)
		if err != nil {
			return 0, nil, 0, nil, err
		}
		entries[0].key = min
	}
	if childSplitKey != nil {
		ins := internalKV{key: append([]byte(nil), childSplitKey...), child: childSplitPid}
		insIdx, _ := findInternalIndex(entries, ins.key)
		entries = append(entries, internalKV{})
		copy(entries[insIdx+1:], entries[insIdx:])
		entries[insIdx] = ins
	}

	leftPid, err := t.pager.AllocPage()
	if err != nil {
		return 0, nil, 0, nil, err
	}
	retired := append(childRetired, oldPid)

	leftBuf := make([]byte, page.PageSize)
	if err := buildInternalPage(leftBuf, leftPid, entries); err == nil {
		if err := t.pager.WritePage(leftPid, leftBuf); err != nil {
			return 0, nil, 0, nil, err
		}
		return leftPid, nil, 0, retired, nil
	} else if !isPageFull(err) {
		return 0, nil, 0, nil, err
	}

	// Split internal.
	rightPid, err := t.pager.AllocPage()
	if err != nil {
		return 0, nil, 0, nil, err
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
	leftBuf = make([]byte, page.PageSize)
	rightBuf := make([]byte, page.PageSize)
	if err := buildInternalPage(leftBuf, leftPid, leftEntries); err != nil {
		return 0, nil, 0, nil, err
	}
	if err := buildInternalPage(rightBuf, rightPid, rightEntries); err != nil {
		return 0, nil, 0, nil, err
	}
	if err := t.pager.WritePage(leftPid, leftBuf); err != nil {
		return 0, nil, 0, nil, err
	}
	if err := t.pager.WritePage(rightPid, rightBuf); err != nil {
		return 0, nil, 0, nil, err
	}
	return leftPid, rightEntries[0].key, rightPid, retired, nil
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
