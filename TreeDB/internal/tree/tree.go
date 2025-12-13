package tree

import (
	"bytes"
	"fmt"

	"treedb/internal/page"
	"treedb/internal/pager"
)

// Tree is a B+Tree rooted at a pager page. It performs copy-on-write updates.
type Tree struct {
	pager *pager.Pager
	root  page.PageID
	kind  Kind
}

// NewUserTree returns a user tree rooted at root.
func NewUserTree(p *pager.Pager, root page.PageID) *Tree {
	return &Tree{pager: p, root: root, kind: KindUser}
}

// NewSystemTree returns a system tree rooted at root.
func NewSystemTree(p *pager.Pager, root page.PageID) *Tree {
	return &Tree{pager: p, root: root, kind: KindSystem}
}

// Root returns the current root PageID.
func (t *Tree) Root() page.PageID { return t.root }

func (t *Tree) encodeKey(key []byte) []byte {
	switch t.kind {
	case KindUser:
		return encodeUserKey(key)
	case KindSystem:
		return encodeSystemKey(key)
	default:
		return encodeUserKey(key)
	}
}

// GetRaw searches for key and returns the stored leaf payload (including tombstones).
func (t *Tree) GetRaw(key []byte) (LeafEntry, error) {
	if key == nil {
		return LeafEntry{}, fmt.Errorf("tree: nil key")
	}
	if t.root == 0 {
		return LeafEntry{}, ErrNotFound
	}
	encKey := t.encodeKey(key)
	leafBuf, _, err := t.search(encKey)
	if err != nil {
		return LeafEntry{}, err
	}
	entries, err := parseLeafEntries(leafBuf)
	if err != nil {
		return LeafEntry{}, err
	}
	idx, found := findLeafIndex(entries, encKey)
	if !found {
		return LeafEntry{}, ErrNotFound
	}
	return entries[idx].entry, nil
}

// SetRaw inserts or updates key with val and returns retired page IDs.
func (t *Tree) SetRaw(key []byte, val LeafEntry) ([]page.PageID, error) {
	if key == nil {
		return nil, fmt.Errorf("tree: nil key")
	}
	encKey := t.encodeKey(key)

	// Empty tree: create a root leaf.
	if t.root == 0 {
		pid, err := t.pager.AllocPage()
		if err != nil {
			return nil, err
		}
		buf := make([]byte, page.PageSize)
		lp, err := page.InitLeafPage(buf, pid)
		if err != nil {
			return nil, err
		}
		_, err = lp.Set(encKey, val.Flags, val.InlineValue, val.Ptr)
		if err != nil {
			return nil, err
		}
		h, body, _ := page.SplitPage(buf)
		h.SetBodyCRC(body)
		if err := t.pager.WritePage(pid, buf); err != nil {
			return nil, err
		}
		t.root = pid
		return nil, nil
	}

	newRoot, splitKey, splitPid, retired, err := t.cowSet(t.root, encKey, val)
	if err != nil {
		return nil, err
	}
	if splitKey != nil {
		// Root split: create a new internal root referencing left and right.
		rootPid, err := t.pager.AllocPage()
		if err != nil {
			return nil, err
		}
		leftMin, err := t.minKey(newRoot)
		if err != nil {
			return nil, err
		}
		entries := []internalKV{
			{key: leftMin, child: newRoot},
			{key: splitKey, child: splitPid},
		}
		rootBuf := make([]byte, page.PageSize)
		if err := buildInternalPage(rootBuf, rootPid, entries); err != nil {
			return nil, err
		}
		if err := t.pager.WritePage(rootPid, rootBuf); err != nil {
			return nil, err
		}
		t.root = rootPid
	} else {
		t.root = newRoot
	}
	return retired, nil
}

// search descends to the leaf for key, returning the leaf buffer and path pids.
func (t *Tree) search(key []byte) ([]byte, []page.PageID, error) {
	if t.root == 0 {
		return nil, nil, ErrEmptyTree
	}
	var path []page.PageID
	pid := t.root
	for {
		path = append(path, pid)
		buf, err := t.pager.ReadPage(pid)
		if err != nil {
			return nil, nil, err
		}
		
		// Verified Cache
		if !t.pager.IsVerified(uint64(pid)) {
			h, body, err := page.SplitPage(buf)
			if err != nil {
				return nil, nil, err
			}
			if err := h.VerifyBodyCRC(body); err != nil {
				return nil, nil, err
			}
			t.pager.MarkVerified(uint64(pid))
		}

		h, _, err := page.SplitPage(buf)
		if err != nil {
			return nil, nil, err
		}
		switch h.Flags {
		case page.PageTypeLeaf:
			return buf, path, nil
		case page.PageTypeInternal:
			entries, err := parseInternalEntries(buf)
			if err != nil {
				return nil, nil, err
			}
			if len(entries) == 0 {
				return nil, nil, ErrCorrupt
			}
			idx := findChildIndex(entries, key)
			pid = entries[idx].child
		default:
			return nil, nil, ErrCorrupt
		}
	}
}

func (t *Tree) minKey(pid page.PageID) ([]byte, error) {
	buf, err := t.pager.ReadPage(pid)
	if err != nil {
		return nil, err
	}
	
	// Verified Cache
	if !t.pager.IsVerified(uint64(pid)) {
		h, body, err := page.SplitPage(buf)
		if err != nil {
			return nil, err
		}
		if err := h.VerifyBodyCRC(body); err != nil {
			return nil, err
		}
		t.pager.MarkVerified(uint64(pid))
	}

	h, _, err := page.SplitPage(buf)
	if err != nil {
		return nil, err
	}
	switch h.Flags {
	case page.PageTypeLeaf:
		entries, err := parseLeafEntries(buf)
		if err != nil {
			return nil, err
		}
		if len(entries) == 0 {
			return nil, ErrCorrupt
		}
		return append([]byte(nil), entries[0].key...), nil
	case page.PageTypeInternal:
		entries, err := parseInternalEntries(buf)
		if err != nil {
			return nil, err
		}
		if len(entries) == 0 {
			return nil, ErrCorrupt
		}
		return append([]byte(nil), entries[0].key...), nil
	default:
		return nil, ErrCorrupt
	}
}

func isPageFull(err error) bool {
	return err == page.ErrPageFull || bytes.Contains([]byte(err.Error()), []byte("page: not enough free space"))
}