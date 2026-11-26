package bulk

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

type mockLeafBatchLog struct {
	mu          sync.Mutex
	pages       [][]byte
	batches     int
	failAtBatch int
	failErr     error
}

func (m *mockLeafBatchLog) AppendLeafPage(leafPage []byte) (page.LeafLogPtr, error) {
	ptrs, err := m.AppendLeafPages([][]byte{leafPage})
	if err != nil {
		return page.LeafLogPtr{}, err
	}
	return ptrs[0], nil
}

func (m *mockLeafBatchLog) AppendLeafPages(leafPages [][]byte) ([]page.LeafLogPtr, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.batches++
	if m.failAtBatch > 0 && m.batches == m.failAtBatch {
		return nil, m.failErr
	}
	ptrs := make([]page.LeafLogPtr, len(leafPages))
	for i, lp := range leafPages {
		cp := append([]byte(nil), lp...)
		idx := len(m.pages)
		m.pages = append(m.pages, cp)
		ptrs[i] = page.LeafLogPtr{FileID: 1, Offset: uint64(idx+1) * page.PageSize}
	}
	return ptrs, nil
}

func TestBuildWithScanPipelinedLeafLogParityAndErrorTeardown(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 64*1024)
	if err != nil {
		t.Fatalf("pager.Open: %v", err)
	}
	defer func() { _ = p.Close() }()
	alloc := &MockAllocator{p: p}
	if _, err := alloc.Alloc(0); err != nil {
		t.Fatalf("reserve page 0: %v", err)
	}

	// Empty scan must build a valid empty leaf-log root.
	emptyLog := &mockLeafBatchLog{}
	emptyRoot, err := BuildWithScan(func(emit EntryEmitter) error {
		return nil
	}, alloc, p, BuildOptions{
		LeafPrefixCompression: true,
		LeafPageLog:           emptyLog,
	})
	if err != nil {
		t.Fatalf("BuildWithScan empty: %v", err)
	}
	if emptyRoot == 0 || len(emptyLog.pages) != 1 {
		t.Fatalf("emptyRoot=%d leafPages=%d want non-zero root and 1 leaf page", emptyRoot, len(emptyLog.pages))
	}

	// Large multi-batch scan (> 2 * valuelog.MaxFrameK leaf pages) exercises
	// double-buffered slab reuse and asynchronous leaf batch pipelining.
	const numEntries = 10000
	valPayload := bytes.Repeat([]byte("x"), 220)
	fullLog := &mockLeafBatchLog{}
	rootID, err := BuildWithScan(func(emit EntryEmitter) error {
		var key [16]byte
		for i := 0; i < numEntries; i++ {
			copy(key[:8], "key-rec-")
			binary.BigEndian.PutUint64(key[8:], uint64(i))
			if err := emit(key[:], valPayload, page.ValuePtr{}, node.FlagInline, page.LegacyEntryRevision); err != nil {
				return err
			}
		}
		return nil
	}, alloc, p, BuildOptions{
		LeafPrefixCompression: true,
		LeafPageLog:           fullLog,
	})
	if err != nil {
		t.Fatalf("BuildWithScan large: %v", err)
	}
	if rootID == 0 {
		t.Fatalf("expected non-zero rootID")
	}
	if len(fullLog.pages) <= valuelog.MaxFrameK {
		t.Fatalf("expected > %d leaf pages to exercise pipelining, got %d", valuelog.MaxFrameK, len(fullLog.pages))
	}
	// Verify every leaf page written into fullLog has intact keys in sorted order.
	seen := 0
	var lastKey []byte
	for idx, raw := range fullLog.pages {
		n := node.NewNodeView(raw)
		if n.Type() != page.PageTypeLeaf {
			t.Fatalf("leaf page %d type=%d want leaf", idx, n.Type())
		}
		for i := uint16(0); i < n.Count(); i++ {
			k, v, _, _, err := n.GetLeafEntryView(i)
			if err != nil {
				t.Fatalf("leaf %d entry %d: %v", idx, i, err)
			}
			if lastKey != nil && bytes.Compare(lastKey, k) >= 0 {
				t.Fatalf("keys out of order at leaf %d entry %d: %x >= %x", idx, i, lastKey, k)
			}
			if !bytes.Equal(v, valPayload) {
				t.Fatalf("leaf %d entry %d value mismatch", idx, i)
			}
			lastKey = append(lastKey[:0], k...)
			seen++
		}
	}
	if seen != numEntries {
		t.Fatalf("verified %d entries across %d leaves, want %d", seen, len(fullLog.pages), numEntries)
	}

	// Verify error propagation during asynchronous in-flight leaf batch drain.
	wantErr := errors.New("injected leaf batch failure")
	failLog := &mockLeafBatchLog{failAtBatch: 2, failErr: wantErr}
	_, err = BuildWithScan(func(emit EntryEmitter) error {
		for i := 0; i < numEntries; i++ {
			k := fmt.Sprintf("fail-%08d", i)
			if err := emit([]byte(k), valPayload, page.ValuePtr{}, node.FlagInline, page.LegacyEntryRevision); err != nil {
				return err
			}
		}
		return nil
	}, alloc, p, BuildOptions{
		LeafPrefixCompression: true,
		LeafPageLog:           failLog,
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("BuildWithScan error=%v want %v", err, wantErr)
	}
}
