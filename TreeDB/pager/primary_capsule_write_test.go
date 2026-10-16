package pager

import (
	"bytes"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/page"
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
