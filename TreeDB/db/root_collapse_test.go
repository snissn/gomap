package db

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func TestDeleteMostKeys_CollapsesRootWhenOneLeafRemains(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(Options{
		Dir:                   dir,
		KeepRecent:            1,
		LeafFillTargetPPM:     850_000,
		InternalFillTargetPPM: 900_000,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	val := bytes.Repeat([]byte("x"), 32)
	const total = 5000

	// Insert keys.
	{
		const batchSize = 512
		for base := 0; base < total; base += batchSize {
			b := d.NewBatch().(*Batch)
			limit := base + batchSize
			if limit > total {
				limit = total
			}
			for i := base; i < limit; i++ {
				var k [8]byte
				k[0] = byte(i >> 24)
				k[1] = byte(i >> 16)
				k[2] = byte(i >> 8)
				k[3] = byte(i)
				if err := b.Set(k[:], val); err != nil {
					t.Fatalf("set: %v", err)
				}
			}
			if err := b.WriteSync(); err != nil {
				t.Fatalf("write: %v", err)
			}
			_ = b.Close()
		}
	}

	// Delete most keys: keep only the first 50 keys so the user tree can shrink
	// down to a single leaf page.
	{
		const keep = 50
		b := d.NewBatch().(*Batch)
		for i := keep; i < total; i++ {
			var k [8]byte
			k[0] = byte(i >> 24)
			k[1] = byte(i >> 16)
			k[2] = byte(i >> 8)
			k[3] = byte(i)
			if err := b.Delete(k[:]); err != nil {
				t.Fatalf("del: %v", err)
			}
		}
		if err := b.WriteSync(); err != nil {
			t.Fatalf("del write: %v", err)
		}
		_ = b.Close()
	}
	rep, err := d.FragmentationReport()
	if err != nil {
		t.Fatalf("FragmentationReport: %v", err)
	}
	leafStr := rep["treedb.user.pages.leaf"]
	if leafStr == "" {
		t.Fatalf("missing treedb.user.pages.leaf")
	}
	leaf, err := strconv.ParseUint(leafStr, 10, 64)
	if err != nil {
		t.Fatalf("parse leaf: %v", err)
	}

	internalStr := rep["treedb.user.pages.internal"]
	if internalStr == "" {
		t.Fatalf("missing treedb.user.pages.internal")
	}
	internal, err := strconv.ParseUint(internalStr, 10, 64)
	if err != nil {
		t.Fatalf("parse internal: %v", err)
	}

	if leaf != 1 {
		t.Fatalf("expected a single leaf page, got %d", leaf)
	}
	if internal != 0 {
		t.Fatalf("expected root to collapse to a leaf, got internal pages=%d", internal)
	}

	// Sanity: remaining keys are present and deleted keys are absent.
	for i := 0; i < 50; i++ {
		var k [8]byte
		k[0] = byte(i >> 24)
		k[1] = byte(i >> 16)
		k[2] = byte(i >> 8)
		k[3] = byte(i)
		got, err := d.Get(k[:])
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if got == nil {
			t.Fatalf("expected key %d to exist", i)
		}
	}
	for i := 50; i < total; i++ {
		var k [8]byte
		k[0] = byte(i >> 24)
		k[1] = byte(i >> 16)
		k[2] = byte(i >> 8)
		k[3] = byte(i)
		got, err := d.Get(k[:])
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if got != nil {
			t.Fatalf("expected key %d to be deleted", i)
		}
	}
}

func testDeleteMostKeysCollapsesRootWithPointerValues(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(Options{
		Dir:                       dir,
		KeepRecent:                1,
		LeafFillTargetPPM:         850_000,
		InternalFillTargetPPM:     900_000,
		MaintenanceOpsPerCoalesce: -1,
		ValueLog: ValueLogOptions{
			PointerThreshold: 1,
		},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	// Force pointer-backed values so this test exercises pointer-mode outer-leaf decode
	// while stressing split/merge maintenance under delete-heavy churn.
	val := bytes.Repeat([]byte("y"), 256)
	const total = 5000

	walDir := filepath.Join(dir, "value_vlog")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("mkdir wal: %v", err)
	}
	fileID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("encode file id: %v", err)
	}
	valueLogPath := filepath.Join(walDir, "value-l0-000001.log")
	vw, err := valuelog.NewWriter(valueLogPath, fileID)
	if err != nil {
		t.Fatalf("new valuelog writer: %v", err)
	}
	registerTestValueLogProducer(t, dir, valueLogPath, fileID)
	vwClosed := false
	defer func() {
		if vwClosed {
			return
		}
		_ = vw.Close()
	}()
	nextRID := uint64(1)

	{
		const batchSize = 512
		for base := 0; base < total; base += batchSize {
			b := d.NewBatch().(*Batch)
			limit := base + batchSize
			if limit > total {
				limit = total
			}
			for i := base; i < limit; i++ {
				var k [8]byte
				k[0] = byte(i >> 24)
				k[1] = byte(i >> 16)
				k[2] = byte(i >> 8)
				k[3] = byte(i)
				ptr, err := vw.Append(0, nil, nextRID, val)
				if err != nil {
					t.Fatalf("append: %v", err)
				}
				nextRID++
				if err := b.SetPointer(k[:], ptr); err != nil {
					t.Fatalf("set: %v", err)
				}
			}
			if err := b.WriteSync(); err != nil {
				t.Fatalf("write: %v", err)
			}
			_ = b.Close()
		}
	}
	if err := vw.Close(); err != nil {
		t.Fatalf("close valuelog writer: %v", err)
	}
	vwClosed = true

	repBefore, err := d.FragmentationReport()
	if err != nil {
		t.Fatalf("FragmentationReport before delete: %v", err)
	}
	beforeLeafStr := repBefore["treedb.user.pages.leaf"]
	if beforeLeafStr == "" {
		t.Fatalf("missing treedb.user.pages.leaf before delete")
	}
	beforeLeaf, err := strconv.ParseUint(beforeLeafStr, 10, 64)
	if err != nil {
		t.Fatalf("parse leaf before delete: %v", err)
	}

	keep := 50
	{
		b := d.NewBatch().(*Batch)
		for i := keep; i < total; i++ {
			var k [8]byte
			k[0] = byte(i >> 24)
			k[1] = byte(i >> 16)
			k[2] = byte(i >> 8)
			k[3] = byte(i)
			if err := b.Delete(k[:]); err != nil {
				t.Fatalf("del: %v", err)
			}
		}
		if err := b.WriteSync(); err != nil {
			t.Fatalf("del write: %v", err)
		}
		_ = b.Close()
	}

	rep, err := d.FragmentationReport()
	if err != nil {
		t.Fatalf("FragmentationReport: %v", err)
	}
	leafStr := rep["treedb.user.pages.leaf"]
	if leafStr == "" {
		t.Fatalf("missing treedb.user.pages.leaf")
	}
	leaf, err := strconv.ParseUint(leafStr, 10, 64)
	if err != nil {
		t.Fatalf("parse leaf: %v", err)
	}

	internalStr := rep["treedb.user.pages.internal"]
	if internalStr == "" {
		t.Fatalf("missing treedb.user.pages.internal")
	}
	internal, err := strconv.ParseUint(internalStr, 10, 64)
	if err != nil {
		t.Fatalf("parse internal: %v", err)
	}

	if beforeLeaf <= leaf {
		t.Fatalf("expected leaf pages to shrink after heavy deletes, before=%d after=%d", beforeLeaf, leaf)
	}
	if leaf > 16 {
		t.Fatalf("expected post-delete leaf pages to stay small, got %d", leaf)
	}
	if internal > 1 {
		t.Fatalf("expected shallow tree after heavy deletes, got internal pages=%d", internal)
	}

	for i := 0; i < keep; i++ {
		var k [8]byte
		k[0] = byte(i >> 24)
		k[1] = byte(i >> 16)
		k[2] = byte(i >> 8)
		k[3] = byte(i)
		got, err := d.Get(k[:])
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if got == nil {
			t.Fatalf("expected key %d to exist", i)
		}
		if !bytes.Equal(got, val) {
			t.Fatalf("value mismatch for key %d", i)
		}
	}
	for i := keep; i < total; i++ {
		var k [8]byte
		k[0] = byte(i >> 24)
		k[1] = byte(i >> 16)
		k[2] = byte(i >> 8)
		k[3] = byte(i)
		got, err := d.Get(k[:])
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if got != nil {
			t.Fatalf("expected key %d to be deleted", i)
		}
	}
}

func TestDeleteMostKeys_CollapsesRootWhenOneLeafRemains_PointerValues(t *testing.T) {
	testDeleteMostKeysCollapsesRootWithPointerValues(t)
}
