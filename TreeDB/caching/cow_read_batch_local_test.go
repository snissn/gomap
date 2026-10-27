package caching

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
)

// Hold the real, bounded traversal owners. Reads must not wait for these locks
// or manufacture another shared slot; the public cut owns its local fallback.
func cowBatchOccupyShared(t testing.TB, s *Snapshot) func() {
	t.Helper()
	b := s.cowCut.basis
	b.readMu.Lock()
	for i := range b.readSlots {
		b.readSlots[i].mu.Lock()
	}
	return func() {
		for i := range b.readSlots {
			b.readSlots[i].mu.Unlock()
		}
		b.readMu.Unlock()
	}
}

func cowLocalBatchProbes(n int) ([][]byte, [][]byte, []mvcckey.VersionRangeResult) {
	a, b := make([][]byte, n), make([][]byte, n)
	for i := range a {
		a[i] = []byte(fmt.Sprintf("a%03d", i))
		b[i] = append(append([]byte(nil), a[i]...), 0)
	}
	return a, b, make([]mvcckey.VersionRangeResult, n)
}

func TestCOWReadBatchLocalPublicLeafReuse(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		for _, entries := range []int{128, -1} {
			for _, mode := range []string{"contended", "preowned"} {
				t.Run(fmt.Sprintf("compressed=%t/cache=%d/%s", compressed, entries, mode), func(t *testing.T) {
					db, backend, want := cowLeafCacheModeFixture(t, entries, compressed, "cow_ordered")
					s, err := db.AcquireMVCCReadCut()
					if err != nil {
						t.Fatal(err)
					}
					defer s.Close()
					release := cowBatchOccupyShared(t, s)
					defer func() {
						if release != nil {
							release()
						}
					}()
					a, b, results := cowLocalBatchProbes(32)
					// A public read establishes local scratch and actual verification;
					// no cache credit, decoded page or private workspace is injected.
					if _, _, _, err = s.SeekGEVersionRange(a[0], b[0]); err != nil {
						t.Fatal(err)
					}
					if mode == "preowned" {
						release()
						release = nil
					}
					reads := func() uint64 {
						return cowLeafCacheCounter(t, backend, "hits") + cowLeafCacheCounter(t, backend, "misses")
					}
					before := reads()
					for i := range a {
						k, v, found, e := s.SeekGEVersionRange(a[i], b[i])
						if e != nil || !found || !bytes.Equal(k, a[i]) || !bytes.Equal(v, want) {
							t.Fatalf("scalar%d: key%q found%t err%v", i, k, found, e)
						}
					}
					scalar := reads() - before
					if mode == "contended" {
						// A fresh cut must take the busy-slot branch, not merely the
						// already-local branch exercised separately below.
						s, err = db.AcquireMVCCReadCut()
						if err != nil {
							t.Fatal(err)
						}
						defer s.Close()
					}
					before = reads()
					if err = s.SeekGEVersionRanges(a, b, results); err != nil {
						t.Fatal(err)
					}
					batch := reads() - before
					for i, r := range results {
						if r.Err != nil || !r.Found || !bytes.Equal(r.Key, a[i]) || !bytes.Equal(r.Value, want) {
							t.Fatalf("batch%d: %+v", i, r)
						}
					}
					if entries < 0 {
						t.Log("cache-disabled values checked; leaf-cache callback counters unavailable")
					} else {
						t.Logf("public cut leaf callbacks: scalar=%d batch=%d", scalar, batch)
					}
					if entries >= 0 && (scalar < 32 || batch >= scalar/2) {
						t.Fatalf("local leaf batching inactive: scalar=%d batch=%d", scalar, batch)
					}
				})
			}
		}
	}
}

func TestCOWReadBatchLocalOverlayMergeOwnership(t *testing.T) {
	db, _, want := cowLeafCacheModeFixture(t, 128, false, "cow_ordered")
	old, err := db.AcquireMVCCReadCut()
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	if err = db.Delete([]byte("a000")); err != nil {
		t.Fatal(err)
	}
	if err = db.Set([]byte("a001"), []byte("overlay")); err != nil {
		t.Fatal(err)
	}
	current, err := db.AcquireMVCCReadCut()
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close()
	release := cowBatchOccupyShared(t, current)
	defer release()
	starts := [][]byte{[]byte("a000"), []byte("a002"), []byte("a000")}
	ends := [][]byte{[]byte("a003"), []byte("a003"), []byte("a003")}
	results := make([]mvcckey.VersionRangeResult, 3)
	for _, s := range []*Snapshot{current, old} {
		if err = s.SeekGEVersionRanges(starts, ends, results); err != nil {
			t.Fatal(err)
		}
		for i, r := range results {
			key, value := starts[i], want
			if s == current && i != 1 {
				key, value = []byte("a001"), []byte("overlay")
			}
			if r.Err != nil || !r.Found || !bytes.Equal(r.Key, key) || !bytes.Equal(r.Value, value) {
				t.Fatalf("current=%t probe%d: %+v", s == current, i, r)
			}
		}
		results[0].Key[0] = 'x'
		results[0].Value[0] = 'x'
		if results[2].Key[0] != 'a' || results[2].Value[0] == 'x' {
			t.Fatal("batch results alias another probe")
		}
	}
}

func TestCOWReadBatchLocalPreparationCapacity(t *testing.T) {
	db, _, _ := cowLeafCacheModeFixture(t, 128, false, "cow_ordered")
	beforeCapture := db.cow.budget.Stats()
	s, err := db.AcquireMVCCReadCut()
	if err != nil {
		t.Fatal(err)
	}
	release := cowBatchOccupyShared(t, s)
	defer release()
	defer s.Close()
	budget := db.cow.budget
	overhead := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	workspace := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowReadWorkspace{}))) + cowReadWorkspaceInspectionCharge() + overhead
	stage := memtable.COWAllocationCharge(uint64(unsafe.Sizeof([mvcckey.MaxReadBatch]cowReadBatchStage{}))) + overhead
	allowed := max(workspace, stage)
	before := budget.Stats()
	pressure, err := budget.AcquireExternal(budget.Limits().MaxInFlightBytes - before.ReservedBytes - allowed - overhead)
	if err != nil {
		t.Fatal(err)
	}
	defer pressure.Close()
	pressured := budget.Stats()
	a, z, r := cowLocalBatchProbes(2)
	if err = s.SeekGEVersionRanges(a, z, r); err != nil {
		t.Fatal(err)
	}
	for _, v := range r {
		if !errors.Is(v.Err, memtable.ErrCOWCapacity) || v.Found || v.Value != nil {
			t.Fatal(v)
		}
	}
	// The maintained scalar fallback retains exactly one partially admitted
	// workspace; failed eager preparation must not strand another lease.
	if s.cowReader == nil || s.cowReader.point != nil {
		t.Fatal("expected original partial scalar workspace")
	}
	if after := budget.Stats(); after.ReservedBytes != pressured.ReservedBytes+workspace || after.ExternalLeases != pressured.ExternalLeases+1 {
		t.Fatalf("partial preparation leaked: before%+v after%+v workspace%d", pressured, after, workspace)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	pressure.Close()
	if after := budget.Stats(); after.ReservedBytes != beforeCapture.ReservedBytes || after.ExternalLeases != beforeCapture.ExternalLeases {
		t.Fatalf("Close did not refund: before%+v after%+v", beforeCapture, after)
	}
}
