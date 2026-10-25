package pager

import (
	"bytes"
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/page"
	"os"
	"path/filepath"
	"testing"
)

func TestPrimaryCapsuleFixedSpanDurabilityAndIndependentSlots(t *testing.T) {
	path := filepath.Join(t.TempDir(), "index.db.primary")
	p, err := Open(path, 16*page.PageSize)
	if err != nil {
		t.Fatal(err)
	}
	if err = p.GrowTo(9); err != nil {
		t.Fatal(err)
	}
	p.SetPageCount(9)
	a, b := make([]byte, 3*page.PageSize), make([]byte, 3*page.PageSize)
	for i := range a {
		a[i] = byte(i*7 + 3)
		b[i] = byte(i*11 + 5)
	}
	before, _ := p.Get(2)
	saved := append([]byte(nil), before...)
	short := &iterator.OrdinalScanWork{RecordLimit: 6, ByteLimit: 1 << 20}
	if _, ok, err := p.WritePrimaryCapsuleViewWithWork(2, a, short); ok || err == nil {
		t.Fatalf("short span %t %v", ok, err)
	}
	after, _ := p.Get(2)
	if !bytes.Equal(saved, after) {
		t.Fatal("unadmitted capsule changed slot")
	}
	for _, slot := range []struct {
		id    uint64
		image []byte
	}{{2, a}, {5, b}} {
		work := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		view, ok, err := p.WritePrimaryCapsuleViewWithWork(slot.id, slot.image, work)
		if err != nil || !ok || !bytes.Equal(view, slot.image) {
			t.Fatalf("install %d %t %v", slot.id, ok, err)
		}
		if work.Records < 7 || work.Bytes < 6*page.PageSize {
			t.Fatalf("missing span/fence work %d/%d", work.Records, work.Bytes)
		}
	}
	if p.dirtyChunks.count == 0 {
		t.Fatal("span fence silently cleared unrelated dirty membership")
	}
	if err = p.Close(); err != nil {
		t.Fatal(err)
	}
	ro, err := OpenReadOnly(path, 16*page.PageSize)
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	for _, slot := range []struct {
		id    uint64
		image []byte
	}{{2, a}, {5, b}} {
		for i := 0; i < 3; i++ {
			view, err := ro.Get(slot.id + uint64(i))
			if err != nil || !bytes.Equal(view, slot.image[i*page.PageSize:(i+1)*page.PageSize]) {
				t.Fatalf("durable slot %d page %d %v", slot.id, i, err)
			}
		}
	}
}

// The capsule and a bank in another mapped chunk must both enter the existing
// all-dirty operation; staging itself performs no physical file fence. This
// exercises the real file-fence seam rather than counting wrapper invocations.
func TestPrimaryCapsuleStageAllDirtyStableFenceV6(t *testing.T) {
	path := filepath.Join(t.TempDir(), "index.db.primary")
	chunkSize := syncPagesTestChunkSize(16)
	p, err := Open(path, chunkSize)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	bank := uint64(chunkSize / page.PageSize)
	if err := p.GrowTo(bank + 1); err != nil {
		t.Fatal(err)
	}
	p.SetPageCount(bank + 1)
	if err := p.Sync(); err != nil {
		t.Fatal(err)
	}
	stable, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer stable.Close()
	beforeCuts, afterCuts := 0, 0
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Resource != durabilitycut.ResourceIndex || e.Path != path {
			return nil
		}
		if e.Point != durabilitycut.BeforeIndexDataSync && e.Point != durabilitycut.AfterIndexDataSync {
			return nil
		}
		if e.StablePath != stable {
			return errors.New("dirty-file observation lost exact retained handle")
		}
		if e.Point == durabilitycut.BeforeIndexDataSync {
			beforeCuts++
		} else {
			afterCuts++
		}
		return nil
	})
	defer restore()
	original := syncPageFileFn
	calls := 0
	injected := errors.New("final physical fence failure")
	syncPageFileFn = func(file *os.File) error {
		calls++
		if file != stable {
			return errors.New("all-dirty sync lost exact retained handle")
		}
		if calls == 1 {
			return injected
		}
		return original(file)
	}
	defer func() { syncPageFileFn = original }()
	image := bytes.Repeat([]byte{0x6b}, 3*page.PageSize)
	before, err := p.Get(2)
	if err != nil {
		t.Fatal(err)
	}
	before = bytes.Clone(before)
	if _, ok, err := p.StagePrimaryCapsuleV6(3, image, nil); ok || err == nil {
		t.Fatal("invalid capsule geometry admitted")
	}
	if view, err := p.Get(2); err != nil || !bytes.Equal(view, before) {
		t.Fatal("invalid stage changed bytes")
	}
	if err := p.Write(bank, bytes.Repeat([]byte{0xc7}, page.PageSize)); err != nil {
		t.Fatal(err)
	}
	if view, ok, err := p.StagePrimaryCapsuleV6(5, image, nil); err != nil || !ok || !bytes.Equal(view, image) {
		t.Fatalf("stage=%t %v", ok, err)
	}
	if calls != 0 || beforeCuts != 0 || afterCuts != 0 || !p.dirtyChunks.contains(0) || !p.dirtyChunks.contains(1) {
		t.Fatalf("stage fenced or lost bank/capsule dirt: calls%d dirty%d", calls, p.dirtyChunks.count)
	}
	if err := p.SyncIndexDataWithStableFile(stable); !errors.Is(err, injected) {
		t.Fatalf("final failure=%v", err)
	}
	if calls != 1 || beforeCuts != 1 || afterCuts != 0 || !p.dirtyChunks.contains(0) || !p.dirtyChunks.contains(1) {
		t.Fatalf("failed all-dirty operation: calls%d cuts%d/%d dirty%d", calls, beforeCuts, afterCuts, p.dirtyChunks.count)
	}
	// Pager retry is exercised here only to test its original restoration
	// contract. The DB publication owner separately poisons this failure.
	if err := p.SyncIndexDataWithStableFile(stable); err != nil {
		t.Fatal(err)
	}
	if calls != 2 || beforeCuts != 2 || afterCuts != 1 || p.dirtyChunks.count != 0 {
		t.Fatalf("all-dirty completion calls%d cuts%d/%d dirty%d", calls, beforeCuts, afterCuts, p.dirtyChunks.count)
	}
	for _, span := range []struct {
		offset int64
		want   []byte
	}{
		{5 * page.PageSize, image},
		{int64(bank) * page.PageSize, bytes.Repeat([]byte{0xc7}, page.PageSize)},
	} {
		got := make([]byte, len(span.want))
		if _, err := stable.ReadAt(got, span.offset); err != nil || !bytes.Equal(got, span.want) {
			t.Fatalf("completed physical range %d differs: %v", span.offset, err)
		}
	}
}
