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
	pid := t.root
	for {
		ref, err := t.pager.ReadPageRef(pid)
		if err != nil {
			return LeafEntry{}, err
		}
		buf := ref.Bytes()

		h, body, err := page.SplitPage(buf)
		if err != nil {
			ref.Release()
			return LeafEntry{}, err
		}
		if err := h.VerifyBodyCRC(body); err != nil {
			ref.Release()
			return LeafEntry{}, err
		}

		switch h.Flags {
		case page.PageTypeLeaf:
			lp, err := page.OpenLeafPage(buf)
			if err != nil {
				ref.Release()
				return LeafEntry{}, err
			}
			idx, found, err := lp.Search(encKey)
			if err != nil {
				ref.Release()
				return LeafEntry{}, err
			}
			if !found {
				ref.Release()
				return LeafEntry{}, ErrNotFound
			}
			off, err := dirEntry(body, int(h.Count), idx)
			if err != nil {
				ref.Release()
				return LeafEntry{}, err
			}
			_, flags, inline, ptr, _, err := decodeLeafEntry(body, off)
			ref.Release()
			if err != nil {
				return LeafEntry{}, err
			}
			out := LeafEntry{Flags: flags, Ptr: ptr}
			if flags == page.LeafFlagInline {
				out.InlineValue = append([]byte(nil), inline...)
			}
			return out, nil
		case page.PageTypeInternal:
			ip, err := page.OpenInternalPage(buf)
			if err != nil {
				ref.Release()
				return LeafEntry{}, err
			}
			if ip.Count() == 0 {
				ref.Release()
				return LeafEntry{}, ErrCorrupt
			}
			insIdx, found, err := ip.Search(encKey)
			if err != nil {
				ref.Release()
				return LeafEntry{}, err
			}
			childIdx := insIdx
			if !found {
				if insIdx == 0 {
					childIdx = 0
				} else {
					childIdx = insIdx - 1
				}
			}
			next, err := ip.ChildAt(childIdx)
			ref.Release()
			if err != nil {
				return LeafEntry{}, err
			}
			pid = next
		default:
			ref.Release()
			return LeafEntry{}, ErrCorrupt
		}
	}
}

// SetRaw inserts or updates key with val and returns retired page IDs and the previous entry (if any).
func (t *Tree) SetRaw(key []byte, val LeafEntry) ([]page.PageID, *LeafEntry, error) {
	if key == nil {
		return nil, nil, fmt.Errorf("tree: nil key")
	}
	encKey := t.encodeKey(key)

	// Empty tree: create a root leaf.
	if t.root == 0 {
		pid, err := t.pager.AllocPage()
		if err != nil {
			return nil, nil, err
		}
		if err := t.pager.WithMutablePage(pid, func(buf []byte) error {
			lp, err := page.InitLeafPage(buf, pid)
			if err != nil {
				return err
			}
			_, err = lp.Set(encKey, val.Flags, val.InlineValue, val.Ptr)
			return err
		}); err != nil {
			return nil, nil, err
		}
		t.root = pid
		return nil, nil, nil
	}

	newRoot, splitKey, splitPid, retired, oldEnt, err := t.cowSet(t.root, encKey, val)
	if err != nil {
		return nil, nil, err
	}
	if splitKey != nil {
		// Root split: create a new internal root referencing left and right.
		rootPid, err := t.pager.AllocPage()
		if err != nil {
			return nil, nil, err
		}
		leftMin, err := t.minKey(newRoot)
		if err != nil {
			return nil, nil, err
		}
		entries := []internalKV{
			{key: leftMin, child: newRoot},
			{key: splitKey, child: splitPid},
		}
		if err := t.pager.WithMutablePage(rootPid, func(rootBuf []byte) error {
			return buildInternalPage(rootBuf, rootPid, entries)
		}); err != nil {
			return nil, nil, err
		}
		t.root = rootPid
	} else {
		t.root = newRoot
	}
	return retired, oldEnt, nil
}

func (t *Tree) minKey(pid page.PageID) ([]byte, error) {
	for {
		ref, err := t.pager.ReadPageRef(pid)
		if err != nil {
			return nil, err
		}
		buf := ref.Bytes()

		h, _, err := page.SplitPage(buf)
		if err != nil {
			ref.Release()
			return nil, err
		}
		switch h.Flags {
		case page.PageTypeLeaf:
			entries, err := parseLeafEntries(buf)
			ref.Release()
			if err != nil {
				return nil, err
			}
			if len(entries) == 0 {
				return nil, ErrCorrupt
			}
			return append([]byte(nil), entries[0].key...), nil
		case page.PageTypeInternal:
			entries, err := parseInternalEntries(buf)
			ref.Release()
			if err != nil {
				return nil, err
			}
			if len(entries) == 0 {
				return nil, ErrCorrupt
			}
			pid = entries[0].child
		default:
			ref.Release()
			return nil, ErrCorrupt
		}
	}
}

func isPageFull(err error) bool {
	return err == page.ErrPageFull || bytes.Contains([]byte(err.Error()), []byte("page: not enough free space"))
}
