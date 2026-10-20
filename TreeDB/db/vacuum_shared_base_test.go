package db

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/tree"
	"github.com/snissn/gomap/TreeDB/zipper"
)

// Exercise the same construction across a superseded older-root build, as
// recoverable-set recapture does. Those pages may still be the shared base of
// accepted outputs, so retiring the old build must not make them allocatable.
func TestVacuumSharedBaseRetainsVersionsAcrossRecapture(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), IndexPrimaryDirectory: true, ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true, PreferAppendAlloc: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	b := d.NewBatch()
	for i := 0; i < 256; i++ {
		if err = b.Set([]byte(fmt.Sprintf("key/%04d", i)), bytes.Repeat([]byte{byte(i)}, 512)); err != nil {
			t.Fatal(err)
		}
	}
	if err = b.WriteSync(); err != nil {
		t.Fatal(err)
	}
	if err = b.Close(); err != nil {
		t.Fatal(err)
	}
	if err = d.CompactIndex(); err != nil {
		t.Fatal(err)
	}
	var snapshots [3]*Snapshot
	snapshots[0] = d.AcquireSnapshot()
	for i := 1; i < 3; i++ {
		if err = d.SetSync([]byte("key/0000"), bytes.Repeat([]byte{byte(240 + i)}, 512)); err != nil {
			t.Fatal(err)
		}
		snapshots[i] = d.AcquireSnapshot()
	}
	for _, snap := range snapshots {
		defer snap.Close()
	}
	p, err := pager.Open(filepath.Join(t.TempDir(), "replacement"), 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err = p.Alloc(2); err != nil {
		t.Fatal(err)
	}
	alloc := freelist.New(p, 0)
	alloc.SetPreferAppend(true)
	z := zipper.New(p, alloc)
	shared := vacuumSharedBase{zipper: z, pager: p}
	var roots [3]uint64
	var pages [3][]uint64
	for i, snap := range snapshots {
		var handled bool
		roots[i], handled, _, err = shared.build(d, snap, snap.state.RootPageID, alloc)
		if err != nil || !handled {
			t.Fatalf("build %d handled=%v err=%v", i, handled, err)
		}
		pages[i], err = tree.New(p, nil, roots[i]).CollectPageIDs()
		if err != nil {
			t.Fatal(err)
		}
		if i == 1 {
			if err = shared.release(alloc, pages[0]); err != nil {
				t.Fatal(err)
			}
		}
	}
	if shared.count != 1 {
		t.Fatalf("same exact base rebuilt %d times", shared.count)
	}
	common := 0
	for _, a := range pages[1] {
		for _, b := range pages[2] {
			if a == b {
				common++
				break
			}
		}
	}
	if common == 0 {
		t.Fatal("retained versions did not share DATA pages")
	}
	dead, err := shared.finish(context.Background(), d, []page.MetaPageBody{{UserRootPageID: roots[1]}, {UserRootPageID: roots[2]}})
	if err != nil {
		t.Fatal(err)
	}
	if len(dead) == 0 {
		t.Fatal("no displaced construction pages to qualify reclaim")
	}
	generation, err := freelist.NewFreelistGenerationV1(1, p.PageCount(), dead, nil)
	if err != nil {
		t.Fatal(err)
	}
	if generation.FreeCount() != uint64(len(dead)) {
		t.Fatal("adopted generation lost free construction pages")
	}
	for _, id := range dead {
		if !generation.Allocatable(id) {
			t.Fatalf("dead page %d not free in adopted generation", id)
		}
	}
	for _, id := range pages[1] {
		if generation.Allocatable(id) {
			t.Fatalf("retained page %d became free", id)
		}
	}
	for _, id := range pages[2] {
		if generation.Allocatable(id) {
			t.Fatalf("retained page %d became free", id)
		}
	}
	adopted := freelist.New(p, 0)
	if err = adopted.EnableCOWV1(generation, freelist.NewReservationLedger()); err != nil {
		t.Fatal(err)
	}
	if shared.count != 0 || shared.retired != nil || shared.pager != nil {
		t.Fatal("construction aliases survive finish")
	}
	// Force actual allocator activity after the delayed frees, then validate all
	// keys in both required versions. A mistaken free/reuse corrupts this check.
	for i := 0; i < 32; i++ {
		id, e := adopted.Alloc(0)
		if e != nil {
			t.Fatal(e)
		}
		image, e := p.GetForWrite(id)
		if e != nil {
			t.Fatal(e)
		}
		clear(image)
	}
	for version := 1; version < 3; version++ {
		tr := tree.New(p, nil, roots[version])
		for i := 0; i < 256; i++ {
			want := byte(i)
			if i == 0 {
				want = byte(240 + version)
			}
			got, e := tr.Get([]byte(fmt.Sprintf("key/%04d", i)))
			if e != nil || !bytes.Equal(got, bytes.Repeat([]byte{want}, 512)) {
				t.Fatalf("version=%d key=%d value=%x err=%v", version, i, got, e)
			}
		}
	}
}
