package tree

import (
	"bytes"
	"fmt"
	"sort"
	"testing"

	"treedb/internal/page"
	"treedb/internal/pager"
)

func openTestPager(t *testing.T) *pager.Pager {
	t.Helper()
	dir := t.TempDir()
	p, err := pager.Open(dir, int64(page.PageSize*4))
	if err != nil {
		t.Fatalf("pager.Open: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func makeKey(i int) []byte {
	// Large keys reduce fanout to force splits quickly.
	k := bytes.Repeat([]byte{byte(i)}, 512)
	// Make ordering deterministic by suffixing i.
	k = append(k, byte(i>>8), byte(i))
	return k
}

func collectUserKeys(t *Tree) ([][]byte, error) {
	var out [][]byte
	var walk func(pid page.PageID) error
	walk = func(pid page.PageID) error {
		buf, err := t.pager.ReadPage(pid)
		if err != nil {
			return err
		}
		h, _, err := page.SplitPage(buf)
		if err != nil {
			return err
		}
		switch h.Flags {
		case page.PageTypeLeaf:
			ents, err := parseLeafEntries(buf)
			if err != nil {
				return err
			}
			for _, e := range ents {
				if e.entry.Flags == page.LeafFlagTombstone {
					continue
				}
				out = append(out, decodeUserKey(e.key))
			}
		case page.PageTypeInternal:
			ents, err := parseInternalEntries(buf)
			if err != nil {
				return err
			}
			for _, e := range ents {
				if err := walk(e.child); err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("unexpected page type %d", h.Flags)
		}
		return nil
	}
	if t.Root() == 0 {
		return nil, nil
	}
	return out, walk(t.Root())
}

func checkMinKeyInvariants(t *Tree, pid page.PageID) error {
	buf, err := t.pager.ReadPage(pid)
	if err != nil {
		return err
	}
	h, _, err := page.SplitPage(buf)
	if err != nil {
		return err
	}
	switch h.Flags {
	case page.PageTypeLeaf:
		ents, err := parseLeafEntries(buf)
		if err != nil {
			return err
		}
		for i := 1; i < len(ents); i++ {
			if bytes.Compare(ents[i-1].key, ents[i].key) >= 0 {
				return fmt.Errorf("leaf not sorted")
			}
		}
		return nil
	case page.PageTypeInternal:
		ents, err := parseInternalEntries(buf)
		if err != nil {
			return err
		}
		for i, e := range ents {
			min, err := t.minKey(e.child)
			if err != nil {
				return err
			}
			if !bytes.Equal(min, e.key) {
				return fmt.Errorf("internal min key mismatch at %d", i)
			}
			if err := checkMinKeyInvariants(t, e.child); err != nil {
				return err
			}
		}
		for i := 1; i < len(ents); i++ {
			if bytes.Compare(ents[i-1].key, ents[i].key) >= 0 {
				return fmt.Errorf("internal not sorted")
			}
		}
		return nil
	default:
		return ErrCorrupt
	}
}

func TestInsertSplitsMaintainOrderAndMinKeys(t *testing.T) {
	p := openTestPager(t)
	ut := NewUserTree(p, 0)

	const n = 40
	keys := make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		k := makeKey(i)
		keys = append(keys, k)
		// Insert in reverse order to stress search paths.
			_, _, err := ut.SetRaw(k, LeafEntry{Flags: page.LeafFlagInline, InlineValue: []byte{byte(i)}})
			if err != nil {
				t.Fatalf("SetRaw %d: %v", i, err)
			}
	}

	got, err := collectUserKeys(ut)
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 })
	if len(got) != len(keys) {
		t.Fatalf("expected %d keys, got %d", len(keys), len(got))
	}
	for i := range keys {
		if !bytes.Equal(keys[i], got[i]) {
			t.Fatalf("ordering mismatch at %d", i)
		}
	}
	if err := checkMinKeyInvariants(ut, ut.Root()); err != nil {
		t.Fatalf("invariants: %v", err)
	}
}

func TestDeleteAddsTombstones(t *testing.T) {
	p := openTestPager(t)
	ut := NewUserTree(p, 0)

	for i := 0; i < 20; i++ {
		k := makeKey(i)
			if _, _, err := ut.SetRaw(k, LeafEntry{Flags: page.LeafFlagInline, InlineValue: []byte("v")}); err != nil {
				t.Fatalf("insert: %v", err)
			}
	}
	// Delete even keys.
	for i := 0; i < 20; i += 2 {
		k := makeKey(i)
			if _, _, err := ut.SetRaw(k, LeafEntry{Flags: page.LeafFlagTombstone}); err != nil {
				t.Fatalf("delete: %v", err)
			}
		e, err := ut.GetRaw(k)
		if err != nil {
			t.Fatalf("GetRaw after delete: %v", err)
		}
		if e.Flags != page.LeafFlagTombstone {
			t.Fatalf("expected tombstone")
		}
	}
	got, err := collectUserKeys(ut)
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	for _, k := range got {
		// Ensure no even keys are present.
		if len(k) > 0 && k[len(k)-1]%2 == 0 {
			t.Fatalf("found deleted key")
		}
	}
}

func TestCopyOnWriteRetiresOldRoot(t *testing.T) {
	p := openTestPager(t)
	ut := NewUserTree(p, 0)
	for i := 0; i < 15; i++ {
		k := makeKey(i)
			if _, _, err := ut.SetRaw(k, LeafEntry{Flags: page.LeafFlagInline, InlineValue: []byte("v")}); err != nil {
				t.Fatalf("insert: %v", err)
			}
	}
	oldRoot := ut.Root()
	k := makeKey(5)
	retired, _, err := ut.SetRaw(k, LeafEntry{Flags: page.LeafFlagInline, InlineValue: []byte("vv")})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if ut.Root() == oldRoot {
		t.Fatalf("expected new root after COW")
	}
	foundOld := false
	for _, r := range retired {
		if r == oldRoot {
			foundOld = true
			break
		}
	}
	if !foundOld {
		t.Fatalf("expected old root retired")
	}
}

func TestUserKeyEncodingMatchesTestSpec(t *testing.T) {
	p := openTestPager(t)
	ut := NewUserTree(p, 0)
	userKey := []byte{0x00, 0x01}
	if _, _, err := ut.SetRaw(userKey, LeafEntry{Flags: page.LeafFlagInline, InlineValue: []byte("v")}); err != nil {
		t.Fatalf("SetRaw: %v", err)
	}
	rawKeys, err := collectRawKeys(ut)
	if err != nil {
		t.Fatalf("collectRawKeys: %v", err)
	}
	wantRaw := encodeUserKey(userKey)
	found := false
	for _, k := range rawKeys {
		if bytes.Equal(k, wantRaw) {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected raw key %x present", wantRaw)
	}

	st := NewSystemTree(p, 0)
	if _, _, err := st.SetRaw(userKey, LeafEntry{Flags: page.LeafFlagInline, InlineValue: []byte("v")}); err != nil {
		t.Fatalf("SetRaw system: %v", err)
	}
	rawSys, err := collectRawKeys(st)
	if err != nil {
		t.Fatalf("collectRawKeys system: %v", err)
	}
	if len(rawSys) != 1 || !bytes.Equal(rawSys[0], userKey) {
		t.Fatalf("system tree should store raw key without prefix")
	}
}

func collectRawKeys(t *Tree) ([][]byte, error) {
	var out [][]byte
	var walk func(pid page.PageID) error
	walk = func(pid page.PageID) error {
		buf, err := t.pager.ReadPage(pid)
		if err != nil {
			return err
		}
		h, _, err := page.SplitPage(buf)
		if err != nil {
			return err
		}
		switch h.Flags {
		case page.PageTypeLeaf:
			ents, err := parseLeafEntries(buf)
			if err != nil {
				return err
			}
			for _, e := range ents {
				out = append(out, append([]byte(nil), e.key...))
			}
		case page.PageTypeInternal:
			ents, err := parseInternalEntries(buf)
			if err != nil {
				return err
			}
			for _, e := range ents {
				if err := walk(e.child); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if t.Root() == 0 {
		return nil, nil
	}
	return out, walk(t.Root())
}
