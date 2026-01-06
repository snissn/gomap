package db

import (
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/slab"
)

func TestRecovery(t *testing.T) {
	dir := t.TempDir()
	chunkSize := int64(65536)

	// 1. Setup Initial State manually
	// Create Index
	idxPath := filepath.Join(dir, "index.db")
	p, err := pager.Open(idxPath, chunkSize)
	if err != nil {
		t.Fatal(err)
	}

	// Alloc Meta Pages
	p.Alloc(2)
	// Allocate root pages for user/system trees.
	userRoot, _ := p.Alloc(1)
	sysRoot, _ := p.Alloc(1)

	// Create Slab 0
	sm, err := slab.NewSlabManager(dir)
	if err != nil {
		t.Fatal(err)
	}

	// Write some data to Slab 0
	ptr, err := sm.Append([]byte("key1"), []byte("val1"))
	if err != nil {
		t.Fatal(err)
	}
	// Header(10) + Key(4) + Val(4) = 18 bytes.
	// Ptr.Offset points to KeyLen (start+4). So 4.
	if ptr.Offset != 4 {
		t.Errorf("Expected offset 4, got %d", ptr.Offset)
	}
	// Length excludes CRC(4). So 18-4 = 14.
	if page.ValuePtrRecordLength(ptr) != 14 {
		t.Errorf("Expected length 14, got %d", page.ValuePtrRecordLength(ptr))
	}

	// Simulate "Torn Write" on Slab: Append garbage but don't update meta
	// We can cheat: append to file directly or use sm.Append and ignore result.
	sm.Append([]byte("garbage"), []byte("data"))
	// Now slab size is > 18.

	// Build empty roots with valid checksums.
	rootData, _ := p.Get(userRoot)
	root := node.NewNode(rootData)
	root.SetPageID(userRoot)
	root.SetType(page.PageTypeLeaf)
	root.SetCount(0)
	root.UpdateChecksum()

	sysData, _ := p.Get(sysRoot)
	sysRootNode := node.NewNode(sysData)
	sysRootNode.SetPageID(sysRoot)
	sysRootNode.SetType(page.PageTypeLeaf)
	sysRootNode.SetCount(0)
	sysRootNode.UpdateChecksum()

	// Write Meta 0 (Seq 1) -> Valid
	m0 := page.MetaPageBody{
		CommitSeq:        1,
		UserRootPageID:   userRoot,
		SystemRootPageID: sysRoot,
		ActiveSlabID:     0,
		ActiveSlabTail:   18, // Points before garbage
		TotalPages:       4,
	}

	data0, _ := p.Get(0)
	m0.Encode(data0[page.PageHeaderSize:])
	n0 := node.NewNode(data0)
	n0.SetPageID(0)
	n0.SetType(page.PageTypeMeta)
	n0.UpdateChecksum()

	// Write Meta 1 (Seq 2) -> Corrupted Checksum
	m1 := page.MetaPageBody{
		CommitSeq: 2,
	}
	data1, _ := p.Get(1)
	m1.Encode(data1[page.PageHeaderSize:])
	// Don't set header or checksum -> should be invalid (all zeros or garbage)

	p.Sync()
	p.Close()
	sm.Close()

	// 2. Run Recovery
	db, err := Open(Options{Dir: dir, ChunkSize: chunkSize})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	// 3. Verify
	// Should have picked Meta 0 (Seq 1)
	// Active Slab should be truncated to 18

	if db.meta.CommitSeq != 1 {
		t.Errorf("Expected CommitSeq 1, got %d", db.meta.CommitSeq)
	}

	// Check Slab Size
	// Access internal slab manager or check file size
	sm2 := db.SlabManager()
	if sm2.ActiveSlabTail() != 18 {
		t.Errorf("Expected ActiveSlabTail 18, got %d", sm2.ActiveSlabTail())
	}
}

func TestCommit(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, ChunkSize: 65536})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Initial State: Seq 0 (from Open recovery of empty DB)
	if db.meta.CommitSeq != 0 {
		t.Errorf("Expected initial Seq 0, got %d", db.meta.CommitSeq)
	}

	// Fake a New Root
	newRootID, _ := db.Pager().Alloc(1)
	newRootData, _ := db.Pager().GetForWrite(newRootID)
	newRoot := node.NewNode(newRootData)
	newRoot.SetPageID(newRootID)
	newRoot.SetType(page.PageTypeLeaf)
	newRoot.SetCount(0)
	newRoot.UpdateChecksum()

	// Commit
	if err := db.Commit(newRootID); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Check Seq incremented
	if db.meta.CommitSeq != 1 {
		t.Errorf("Expected Seq 1, got %d", db.meta.CommitSeq)
	}

	// Re-Open to verify persistence
	db.Close()

	db2, err := Open(Options{Dir: dir, ChunkSize: 65536})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	if db2.meta.CommitSeq != 1 {
		t.Errorf("Restored Seq expected 1, got %d", db2.meta.CommitSeq)
	}
}
