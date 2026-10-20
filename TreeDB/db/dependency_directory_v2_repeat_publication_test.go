package db

import (
	"bytes"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/page"
)

// Exercise the actual DATA publisher, with V2 selected before its first Open.
// The second record must append to the first physical vlog segment; rotating
// assets would avoid the repeated-owner case this control is intended to expose.
func TestDependencyDirectoryV2RepeatedPointerPublicationSameSegment(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir, Durability: DurabilityWALOffRelaxed, ResolvedProfile: ProfileNoWALFast, DisableBackgroundPrune: true, rootPublicationFixedDelay: 100 * time.Millisecond}
	cfg := formatConfigFromOptions(opts)
	cfg.RequiredFeatures = appendRequiredFormatFeature(cfg.RequiredFeatures, RequiredFeatureDependencyDirectoryV2)
	if err := SaveFormatConfig(dir, cfg); err != nil {
		t.Fatal(err)
	}
	d, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if d != nil {
			if err := d.Close(); err != nil {
				t.Errorf("cleanup Close: %v", err)
			}
		}
	}()
	owner := d.StableResourceMetadataOwner()
	if idx := d.idx.Load(); idx == nil || owner == nil || d.rootPublication.metadata != owner || d.StableResourceIdentityPinRegistry().MetadataOwner() != owner {
		t.Fatal("not the original-owner DATA publication route")
	}
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer budget.Close()
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	appender, err := newReplayInlineAppender(d, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if appender != nil {
			if d != nil {
				d.SetValueLogAppender(nil)
			}
			if err := appender.close(); err != nil {
				t.Errorf("cleanup appender: %v", err)
			}
		}
	}()
	d.SetValueLogAppender(appender)
	key := []byte("repeated-pointer")
	publish := func(value []byte) page.ValuePtr {
		t.Helper()
		ptrs, err := d.AppendValueLogValues([][]byte{value})
		if err != nil || len(ptrs) != 1 || ptrs[0].FileID == 0 {
			t.Fatalf("append pointers=%+v: %v", ptrs, err)
		}
		batch := d.NewBatch().(*Batch)
		defer batch.Close()
		if err := batch.SetPointer(key, ptrs[0]); err != nil {
			t.Fatal(err)
		}
		if err := batch.Write(); err != nil {
			t.Fatal(err)
		}
		if err := batch.Close(); err != nil {
			t.Fatal(err)
		}
		if err := d.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		selected := d.durableRoot.slotResources[d.durableRoot.slot]
		if d.durableRoot.record.Directory.RootPageID == 0 || selected == nil || selected.Len() == 0 || selected.MetadataOwner() != owner {
			t.Fatal("checkpoint did not select nonempty V2 resources on original owner")
		}
		return ptrs[0]
	}
	check := func(s *Snapshot, ptr page.ValuePtr, want []byte) {
		t.Helper()
		entry, err := s.GetEntryExact(key)
		if err != nil {
			t.Fatal(err)
		}
		got, err := s.Get(key)
		if err != nil || entry.ValuePtr != ptr || !bytes.Equal(got, want) {
			t.Fatalf("pointer=%+v want=%+v value equal=%t: %v", entry.ValuePtr, ptr, bytes.Equal(got, want), err)
		}
	}
	v1, v2 := bytes.Repeat([]byte("first|"), 1024), bytes.Repeat([]byte("second|"), 1024)
	p1 := publish(v1)
	old := d.AcquireSnapshot()
	if old == nil {
		t.Fatal("nil first Snapshot")
	}
	defer func() {
		if old != nil {
			if err := old.Close(); err != nil {
				t.Errorf("cleanup old Snapshot: %v", err)
			}
		}
	}()
	check(old, p1, v1)
	p2 := publish(v2)
	if p1.FileID != p2.FileID || p1 == p2 {
		t.Fatalf("same-segment append premise failed: first=%+v second=%+v", p1, p2)
	}
	check(old, p1, v1)
	current := d.AcquireSnapshot()
	defer func() {
		if current != nil {
			if err := current.Close(); err != nil {
				t.Errorf("cleanup current Snapshot: %v", err)
			}
		}
	}()
	if current == nil {
		t.Fatal("nil current Snapshot")
	}
	check(current, p2, v2)
	for slot, resources := range d.durableRoot.slotResources {
		if resources == nil || resources.Len() == 0 || resources.MetadataOwner() != owner {
			t.Fatalf("slot %d lost nonempty original-owner resources", slot)
		}
		directory, err := resources.DependencyDirectoryV2()
		if err != nil || directory == nil || directory.MetadataOwner() != owner {
			t.Fatalf("slot %d lost original-owner directory: %v", slot, err)
		}
	}
	if err := current.Close(); err != nil {
		t.Fatal(err)
	}
	current = nil
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	old = nil
	d.SetValueLogAppender(nil)
	if err := appender.close(); err != nil {
		t.Fatal(err)
	}
	appender = nil
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d = nil
	enrollment.Close()
	budget.Close()
	if owner.Bytes() != 0 || budget.Stats().TotalBytes != 0 {
		t.Fatalf("original owner did not drain: bytes=%d budget=%+v", owner.Bytes(), budget.Stats())
	}

	d, err = Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	owner = d.StableResourceMetadataOwner()
	budget, err = memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer budget.Close()
	enrollment, err = owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	current = d.AcquireSnapshot()
	if current == nil {
		t.Fatal("nil reopened Snapshot")
	}
	check(current, p2, v2)
	if err := current.Close(); err != nil {
		t.Fatal(err)
	}
	current = nil
	selected := d.durableRoot.slotResources[d.durableRoot.slot]
	if d.durableRoot.record.Directory.RootPageID == 0 || selected == nil || selected.MetadataOwner() != owner {
		t.Fatal("reopen lost original-owner V2 resources")
	}
	selected = nil
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d = nil
	enrollment.Close()
	budget.Close()
	if owner.Bytes() != 0 || budget.Stats().TotalBytes != 0 {
		t.Fatalf("reopened owner did not drain: bytes=%d budget=%+v", owner.Bytes(), budget.Stats())
	}
}
