package zipper

import (
	"encoding/binary"
	"path/filepath"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/slab"
)

func TestZipperDepthLimit(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	sm, err := slab.NewSlabManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Close()

	alloc := &MockAllocator{p: p}
	z := New(p, alloc)

	makeLeaf := func(key, value []byte) uint64 {
		id, err := p.Alloc(1)
		if err != nil {
			t.Fatalf("alloc leaf: %v", err)
		}
		data, err := p.GetForWrite(id)
		if err != nil {
			t.Fatalf("get leaf: %v", err)
		}
		b := node.NewBuilder(data, page.PageTypeLeaf)
		b.SetPageID(id)
		if err := b.AddLeafEntry(key, value, node.FlagInline, page.ValuePtr{}); err != nil {
			t.Fatalf("add leaf: %v", err)
		}
		b.Finish()
		return id
	}

	targetKey := []byte("a")
	targetVal := []byte("value")
	leafID := makeLeaf(targetKey, targetVal)

	// Build a chain of single-child internal nodes; these should collapse on write.
	currentID := leafID
	for i := 0; i < 60; i++ {
		id, err := p.Alloc(1)
		if err != nil {
			t.Fatalf("alloc internal: %v", err)
		}
		data, err := p.GetForWrite(id)
		if err != nil {
			t.Fatalf("get internal: %v", err)
		}
		b := node.NewBuilder(data, page.PageTypeInternal)
		b.SetPageID(id)
		if err := b.AddInternalChild([]byte{}, currentID); err != nil {
			t.Fatalf("add child: %v", err)
		}
		b.Finish()
		currentID = id
	}

	b := batch.New(sm, page.DefaultInlineThreshold)
	defer func() { _ = b.Close() }()
	if err := b.Set(targetKey, []byte("next")); err != nil {
		t.Fatalf("set: %v", err)
	}

	_, _, _, err = z.Apply(currentID, b)
	if err != nil {
		t.Fatalf("unexpected depth error: %v", err)
	}
}

func TestZipperDepthLimitOverride(t *testing.T) {
	run := func(limit int) error {
		dir := t.TempDir()
		zipperDepthLimit = limit
		p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
		if err != nil {
			t.Fatal(err)
		}
		defer p.Close()

		sm, err := slab.NewSlabManager(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer sm.Close()

		alloc := &MockAllocator{p: p}
		z := New(p, alloc)
		z.SetFillTargets(1_000_000, 1_000_000)

		rootID, _ := p.Alloc(1)
		data, _ := p.Get(rootID)
		n := node.NewNode(data)
		n.SetPageID(rootID)
		n.SetType(page.PageTypeLeaf)
		n.UpdateChecksum()

		const keyLen = 2000
		makeKey := func(prefix byte, idx uint64) []byte {
			key := make([]byte, keyLen)
			for i := 0; i < keyLen-2; i++ {
				key[i] = prefix
			}
			binary.BigEndian.PutUint16(key[keyLen-2:], uint16(idx))
			return key
		}

		const (
			batches   = 200
			batchSize = 8
		)
		lowKey := makeKey(0x66, 0)
		value := []byte("v")

		for b := 0; b < batches; b++ {
			bt := batch.New(sm, page.DefaultInlineThreshold)
			_ = bt.Set(lowKey, value)
			start := uint64(b * batchSize)
			for i := 0; i < batchSize; i++ {
				key := makeKey(0x73, start+uint64(i))
				_ = bt.Set(key, value)
			}
			newRoot, _, _, err := z.Apply(rootID, bt)
			_ = bt.Close()
			if err != nil {
				return err
			}
			rootID = newRoot
		}
		return nil
	}

	orig := zipperDepthLimit
	defer func() {
		zipperDepthLimit = orig
	}()

	err := run(50)
	if err == nil || !strings.Contains(err.Error(), "tree too deep") {
		t.Fatalf("expected depth error at limit 50, got %v", err)
	}

	err = run(200)
	if err != nil {
		t.Fatalf("unexpected error at limit 200: %v", err)
	}
}
