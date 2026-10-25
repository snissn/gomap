package db

import (
	"errors"
	"fmt"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/page"
)

type nativePointTestStorage struct {
	reader    SnapshotRootReader
	point     NativePointScratch
	key, leaf [page.PageSize]byte
}

func TestNativeOwnedPointSnapshotBindingAndClose(t *testing.T) {
	dir := t.TempDir()
	d := openOrderedRootSpanNativeTestDB(t, dir, true, 0)
	defer d.Close()
	log, err := NewStandaloneLeafPageLog(dir, StandaloneLeafPageLogOptions{Compression: ValueLogCompressionBlock, BlockCodec: ValueLogBlockLZ4})
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	d.SetLeafPageLog(log)
	delta := newOrderedRootSpanNativeBatch(t, 600, "captured")
	root := publishOrderedRootSpanNativeBatch(t, d, 0, delta, OrderedRootStorageValueLogLeaves)
	delta.Close()
	first := d.AcquireSnapshot()
	defer first.Close()
	second := d.AcquireSnapshot()
	defer second.Close()
	if first == second {
		t.Fatal("fixture requires distinct snapshot owners")
	}
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	lease, err := budget.AdmitNativeBytes(uint64(unsafe.Sizeof(nativePointTestStorage{})))
	if err != nil {
		t.Fatal(err)
	}
	storage := new(nativePointTestStorage)
	defer func() {
		storage.point.Invalidate()
		*storage = nativePointTestStorage{}
		storage = nil
		lease.Close()
		budget.Close()
		if stats := budget.Stats(); stats.TotalBytes != 0 {
			t.Error("point storage retained debt", stats)
		}
	}()
	storage.reader, err = first.NativeReaderAtRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	loads := 0
	read := func(owner *Snapshot) func(page.LeafLogPtr, []byte) ([]byte, error) {
		return func(ptr page.LeafLogPtr, dst []byte) ([]byte, error) {
			// Match the actual collection shared-buffer invalidation protocol.
			storage.point.Invalidate()
			loads++
			data, _, err := owner.reader.ReadLeafLogPageUnsafeTo(ptr, dst)
			if err != nil {
				return nil, err
			}
			if len(data) != page.PageSize {
				return nil, fmt.Errorf("unexpected leaf size%d", len(data))
			}
			copy(dst[:page.PageSize], data)
			return dst[:page.PageSize:page.PageSize], nil
		}
	}
	firstRead := read(first)
	for _, i := range []int{0, 1, 2, 4, 0} {
		// Named-domain callers reconstruct this by-value reader per point.
		storage.reader, err = first.NativeReaderAtRoot(root)
		if err != nil {
			t.Fatal(err)
		}
		entry, err := storage.reader.GetEntryExactWithOwnedScratch([]byte(fmt.Sprintf("key-%06d", i)), storage.key[:], storage.leaf[:], &storage.point, firstRead)
		if err != nil || string(entry.Value) != fmt.Sprintf("captured-%06d", i) {
			t.Fatal("captured exact point", entry, err)
		}
		if storage.point.owner != first {
			t.Fatal("callback invalidation lost snapshot stamp")
		}
	}
	if loads != 1 {
		t.Fatalf("same external leaf loads=%d want1", loads)
	}
	hotKey := []byte("key-000001")
	if allocs := testing.AllocsPerRun(25, func() {
		storage.reader, err = first.NativeReaderAtRoot(root)
		if err != nil {
			t.Fatal(err)
		}
		entry, readErr := storage.reader.GetEntryExactWithOwnedScratch(hotKey, storage.key[:], storage.leaf[:], &storage.point, firstRead)
		if readErr != nil || string(entry.Value) != "captured-000001" {
			t.Fatal("reconstructed hot reader", readErr)
		}
	}); allocs != 0 {
		t.Fatalf("reconstructed hot point allocated %g", allocs)
	}
	if loads != 1 {
		t.Fatal("reconstructed hot reader reloaded its leaf", loads)
	}
	if len(first.rootTrees) != 0 {
		t.Fatal("point scratch populated independent root cache")
	}
	// Reassign the same by-value reader location to the same root ID under a
	// different snapshot. Its stamp, not root ID equality, owns the cached page.
	storage.reader, err = second.NativeReaderAtRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	entry, err := storage.reader.GetEntryExactWithOwnedScratch([]byte("key-000001"), storage.key[:], storage.leaf[:], &storage.point, read(second))
	if err != nil || string(entry.Value) != "captured-000001" || loads != 2 || storage.point.owner != second {
		t.Fatal("snapshot rebind", entry, err, loads)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := storage.reader.GetEntryExactWithOwnedScratch([]byte("key-000001"), storage.key[:], storage.leaf[:], &storage.point, read(second)); !errors.Is(err, ErrClosed) || storage.point.owner != nil {
		t.Fatal("closed owner retained view", err)
	}
	var missing *SnapshotRootReader
	if _, err := missing.GetEntryExactWithOwnedScratch(nil, storage.key[:], storage.leaf[:], &storage.point, nil); !errors.Is(err, ErrClosed) {
		t.Fatal("nil reader", err)
	}
}
