package pager

import (
	"bytes"
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/page"
	"path/filepath"
	"testing"
)

func TestPrimaryCapsuleFenceStatusDistinguishesActualMutationV6(t *testing.T) {
	p, err := Open(filepath.Join(t.TempDir(), "status.primary"), 16*page.PageSize)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.GrowTo(9); err != nil {
		t.Fatal(err)
	}
	p.SetPageCount(9)
	image := bytes.Repeat([]byte{0x7b}, 3*page.PageSize)
	before, err := p.Get(2)
	if err != nil {
		t.Fatal(err)
	}
	saved := bytes.Clone(before)
	work := iterator.OrdinalScanWork{RecordLimit: 6, ByteLimit: 1 << 20}
	if _, ready, mutated, err := p.WritePrimaryCapsuleWithFenceStatusV6(2, image, nil, &work); ready || mutated || err == nil {
		t.Fatal("work refusal became ambiguous authority", ready, mutated, err)
	}
	after, _ := p.Get(2)
	if !bytes.Equal(saved, after) {
		t.Fatal("work refusal changed actual slot")
	}
	injected := errors.New("native authority cut")
	for _, point := range []durabilitycut.Point{durabilitycut.BeforePublicationSealWrite, durabilitycut.AfterPublicationSealWrite} {
		restore := durabilitycut.Install(func(event durabilitycut.Event) error {
			if event.Point == point {
				return injected
			}
			return nil
		})
		work := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		_, ready, mutated, err := p.WritePrimaryCapsuleWithFenceStatusV6(2, image, nil, &work)
		restore()
		if ready || !errors.Is(err, injected) || mutated != (point == durabilitycut.AfterPublicationSealWrite) {
			t.Fatal("cut classified wrong actual mutation", point, ready, mutated, err)
		}
	}
	work = iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	if _, ready, mutated, err := p.WritePrimaryCapsuleWithFenceStatusV6(2, image, nil, &work); !ready || !mutated || err != nil {
		t.Fatal("real authority fence failed", ready, mutated, err)
	}
}

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
