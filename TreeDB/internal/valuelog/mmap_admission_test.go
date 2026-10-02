package valuelog

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
	templ "github.com/snissn/gomap/TreeDB/template"
)

func openMmapAdmissionFixture(t *testing.T, lane, seq uint32) (*File, page.ValuePtr, []byte) {
	t.Helper()
	id := mustEncodeFileID(t, lane, seq)
	path := filepath.Join(t.TempDir(), "value.log")
	w, err := NewWriter(path, id)
	if err != nil {
		t.Fatal(err)
	}
	want := bytes.Repeat([]byte("owned"), 64)
	ptr, err := w.Append(0, nil, 1, want)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := openFile(path, id, nil, nil, templ.DecodeOptions{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.Close(); err != nil {
			t.Error(err)
		}
	})
	return f, ptr, want
}

func TestMmapAdmission_RechecksBeforePublication(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap unsupported")
	}
	for _, lane := range []uint32{0, ReservedLeafLogLaneID} {
		for _, bytesCap := range []bool{false, true} {
			t.Run(fmt.Sprintf("lane_%d/bytes_cap_%t", lane, bytesCap), func(t *testing.T) {
				withMappedSealedBudget(t, 1)
				withMappedLeafSealedBudget(t, 1)
				withMappedSealedBytesBudget(t, 1<<30)
				withMappedLeafSealedBytesBudget(t, 1<<30)
				a, _, _ := openMmapAdmissionFixture(t, lane, 1)
				b, ptr, want := openMmapAdmissionFixture(t, lane, 2)
				m := &Manager{files: map[uint32]*File{a.ID: a, b.ID: b}}
				a.manager, b.manager = m, m
				info, err := a.File.Stat()
				if err != nil {
					t.Fatal(err)
				}
				if bytesCap {
					MaxMappedSealedSegments, MaxMappedLeafSealedSegments = 8, 8
					MaxMappedSealedBytes, MaxMappedLeafSealedBytes = info.Size(), info.Size()
				}
				// Both eligibility observations precede either publication. No
				// scheduler luck or production hook is needed to reproduce the race.
				m.mu.Lock()
				allowA, _ := m.allowSealedLazyMmapLocked(a, info.Size())
				allowB, _ := m.allowSealedLazyMmapLocked(b, info.Size())
				m.mu.Unlock()
				if !allowA || !allowB {
					t.Fatal("pre-publication admission unexpectedly denied")
				}
				a.remapToFileSize()
				b.remapToFileSize()
				mappedA, _ := a.mmapData.Load().([]byte)
				mappedB, _ := b.mmapData.Load().([]byte)
				if int64(len(mappedA)) != info.Size() || len(mappedB) != 0 {
					t.Fatalf("admission overshot budget: mapped A=%d B=%d file=%d", len(mappedA), len(mappedB), info.Size())
				}
				for _, read := range []func() ([]byte, error){
					func() ([]byte, error) { return b.ReadAppend(ptr, true, nil) },
					func() ([]byte, error) { return b.ReadUnsafe(ptr, true) },
					func() ([]byte, error) { v, _, err := b.ReadUnsafeTo(ptr, true, nil); return v, err },
				} {
					v, err := read()
					if err != nil || !bytes.Equal(v, want) {
						t.Fatalf("fallback: %v", err)
					}
				}
				if b.mmapReadHits.Load() != 0 || b.mmapReadFallbackReadAt.Load() != 3 || b.ReadStats().RecordCRCChecks != 3 || b.sealedMapDeniedByCount.Load()+b.sealedMapDeniedByBytes.Load() != 1 {
					t.Fatal("loser did not preserve one memoized denial and three checked fallbacks")
				}
			})
		}
	}
}

func TestMmapAdmission_SealedRangeGrowthUsesActualSize(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap unsupported")
	}
	withMappedSealedBudget(t, 8)
	withMappedSealedBytesBudget(t, 1)
	f, ptr, want := openMmapAdmissionFixture(t, 0, 1)
	f.manager = &Manager{files: map[uint32]*File{f.ID: f}}
	old, err := mmapReadOnly(f.File, 1)
	if err != nil {
		t.Fatal(err)
	}
	f.mmapData.Store(old)
	f.fileSize.Store(1) // A stale hint must not substitute for Stat's actual size.
	got, err := f.Read(ptr, true)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("safe fallback: %v", err)
	}
	data, _ := f.mmapData.Load().([]byte)
	if len(data) != 1 || f.remapCount.Load() != 0 || f.deadMappingsCount.Load() != 0 || f.sealedMapDeniedByBytes.Load() != 1 || f.mmapReadFallbackReadAt.Load() != 1 {
		t.Fatal("direct safe range refresh bypassed sealed byte budget")
	}
	got, err = f.ReadAppend(ptr, true, nil)
	if err != nil || !bytes.Equal(got, want) || f.sealedMapDeniedByBytes.Load() != 1 {
		t.Fatalf("owned denial fallback: %v", err)
	}
}

func TestMmapAdmission_FailedMappingPreservesOldView(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap unsupported")
	}
	f, ptr, want := openMmapAdmissionFixture(t, 0, 1)
	f.remapToFileSize()
	view, err := f.ReadUnsafe(ptr, true)
	if err != nil {
		t.Fatal(err)
	}
	old, _ := f.mmapData.Load().([]byte)
	if !ptrInMapping(view, old) {
		t.Fatal("expected real unsafe mapped view")
	}
	original := f.File
	writeOnly, err := os.OpenFile(f.Path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writeOnly.Write([]byte("growth")); err != nil {
		t.Fatal(err)
	}
	f.File = writeOnly
	t.Cleanup(func() {
		f.File = original
		if err := writeOnly.Close(); err != nil {
			t.Error(err)
		}
	})
	before := f.remapCount.Load()
	if f.remapToFileSize() {
		t.Fatal("failed mmap reported success")
	}
	data, _ := f.mmapData.Load().([]byte)
	if len(data) != len(old) || &data[0] != &old[0] || f.deadMappingsCount.Load() != 0 || f.deadMappedBytes.Load() != 0 || len(f.deadMappings) != 0 || f.remapCount.Load() != before || !bytes.Equal(view, want) {
		t.Fatal("failed mmap retired or changed the live unsafe mapping")
	}
}

func TestMmapAdmission_ConcurrentSiblingReads(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap unsupported")
	}
	withMappedSealedBudget(t, 1)
	withMappedSealedBytesBudget(t, 1<<30)
	a, pa, wa := openMmapAdmissionFixture(t, 0, 1)
	b, pb, wb := openMmapAdmissionFixture(t, 0, 2)
	m := &Manager{files: map[uint32]*File{a.ID: a, b.ID: b}}
	a.manager, b.manager = m, m
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, row := range []struct {
		f    *File
		p    page.ValuePtr
		want []byte
	}{{a, pa, wa}, {b, pb, wb}} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			v, err := row.f.ReadAppend(row.p, true, nil)
			if err != nil || !bytes.Equal(v, row.want) {
				t.Errorf("concurrent read: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()
	da, _ := a.mmapData.Load().([]byte)
	db, _ := b.mmapData.Load().([]byte)
	if (len(da) > 0) == (len(db) > 0) {
		t.Fatal("expected exactly one admitted sibling")
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMmapAdmission_RemovedHandleCannotPublish(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap unsupported")
	}
	withMappedSealedBudget(t, 8)
	withMappedSealedBytesBudget(t, 1<<30)
	f, _, _ := openMmapAdmissionFixture(t, 0, 1)
	replacement, _, _ := openMmapAdmissionFixture(t, 0, 1)
	f.manager = &Manager{files: map[uint32]*File{f.ID: replacement}}
	if f.remapToFileSize() {
		t.Fatal("replaced managed handle reported admission")
	}
	data, _ := f.mmapData.Load().([]byte)
	if len(data) != 0 || f.remapCount.Load() != 0 || f.sealedLazyMmapDenied.Load() {
		t.Fatal("replaced handle published or changed budget-denial state")
	}
}
