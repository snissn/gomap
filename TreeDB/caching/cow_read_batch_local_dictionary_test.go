//go:build !windows

package caching

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func TestCOWReadBatchLocalCorruptionAndRefund(t *testing.T) {
	for _, cache := range []int{128, -1} {
		for _, warm := range []bool{false, true} {
			t.Run(fmt.Sprintf("cache%d/warm%t", cache, warm), func(t *testing.T) {
				db, _, want, ptr := cowLeafMissFixture(t, cache, 64)
				// Establish physical-cache ownership before measuring local lifetime.
				primer, err := db.AcquireMVCCReadCut()
				if err != nil {
					t.Fatal(err)
				}
				if _, _, _, err = primer.SeekGEVersionRange([]byte("a000"), []byte("a001")); err != nil {
					t.Fatal(err)
				}
				if err = primer.Close(); err != nil {
					t.Fatal(err)
				}
				baseline := db.cow.budget.Stats()
				s, err := db.AcquireMVCCReadCut()
				if err != nil {
					t.Fatal(err)
				}
				release := cowBatchOccupyShared(t, s)
				defer release()
				defer s.Close()
				a, b, r := cowLocalBatchProbes(8)
				if warm {
					if err = s.SeekGEVersionRanges(a, b, r); err != nil {
						t.Fatal(err)
					}
				}
				f, _, err := s.cowCut.basis.snapshot.PinnedValueLogFile(ptr.ValueLogFileID())
				if err != nil {
					t.Fatal(err)
				}
				writer, err := os.OpenFile(f.Name(), os.O_RDWR, 0)
				if err != nil {
					t.Fatal(err)
				}
				defer writer.Close()
				offset := int64(ptr.Offset - 4)
				if warm {
					offset = int64(ptr.Offset)
				}
				var saved [1]byte
				if _, err = writer.ReadAt(saved[:], offset); err != nil {
					t.Fatal(err)
				}
				if _, err = writer.WriteAt([]byte{saved[0] ^ 1}, offset); err != nil {
					t.Fatal(err)
				}
				defer writer.WriteAt(saved[:], offset)
				if err = s.SeekGEVersionRanges(a, b, r); err != nil {
					t.Fatal(err)
				}
				for i, v := range r {
					if !errors.Is(v.Err, valuelog.ErrCorrupt) || v.Found || v.Value != nil {
						t.Fatalf("probe%d lost real error: %+v", i, v)
					}
				}
				if _, err = writer.WriteAt(saved[:], offset); err != nil {
					t.Fatal(err)
				}
				if err = s.SeekGEVersionRanges(a, b, r); err != nil {
					t.Fatal(err)
				}
				for _, v := range r {
					if v.Err != nil || !bytes.Equal(v.Value, want) {
						t.Fatal(v)
					}
				}
				if err = s.Close(); err != nil {
					t.Fatal(err)
				}
				// Successful recovered traversal may retain revocable plain
				// scratch. Pressure must refund it before another admission.
				header := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
				pressure, err := db.cow.budget.AcquireExternal(db.cow.budget.Limits().MaxInFlightBytes - baseline.ReservedBytes - header)
				if err != nil {
					t.Fatal(err)
				}
				pressure.Close()
				if after := db.cow.budget.Stats(); after.ExternalBytes != baseline.ExternalBytes || after.ExternalLeases != baseline.ExternalLeases || after.ReservedBytes != baseline.ReservedBytes || after.TotalBytes != baseline.TotalBytes {
					t.Fatalf("local Close did not refund: before%+v after%+v", baseline, after)
				}
			})
		}
	}
}

// Each operation uses the public cut factory and owned result API. The held
// original slots force actual local activation; shared32 is the unchanged path.
func BenchmarkCOWReadBatchLocal(b *testing.B) {
	for _, shape := range []struct {
		n                 int
		contended, sparse bool
	}{
		{1, false, false}, {1, true, false}, {32, false, false}, {32, true, false}, {32, false, true}, {32, true, true},
	} {
		n, contended := shape.n, shape.contended
		b.Run(fmt.Sprintf("n%d/contended%t/sparse%t", n, contended, shape.sparse), func(b *testing.B) {
			keys := 64
			if shape.sparse {
				keys = 2048
			}
			db, backend, want, _ := cowLeafMissFixture(b, 128, keys)
			anchor, err := db.AcquireMVCCReadCut()
			if err != nil {
				b.Fatal(err)
			}
			defer anchor.Close()
			if _, _, _, err = anchor.SeekGEVersionRange([]byte("a000"), []byte("a001")); err != nil {
				b.Fatal(err)
			}
			if contended {
				release := cowBatchOccupyShared(b, anchor)
				defer release()
			}
			a, z, r := cowLocalBatchProbes(n)
			if shape.sparse {
				for i := range a {
					a[i] = []byte(fmt.Sprintf("a%03d", (i*73)%keys))
					z[i] = append(append([]byte(nil), a[i]...), 0)
				}
			}
			reads := func() uint64 {
				stats := backend.Stats()
				var count uint64
				for _, name := range []string{"hits", "misses"} {
					v, e := strconv.ParseUint(stats["treedb.process.read_path.outer_leaf.cache."+name], 10, 64)
					if e != nil {
						b.Fatal(e)
					}
					count += v
				}
				return count
			}
			before := reads()
			b.ReportAllocs()
			b.ResetTimer()
			for j := 0; j < b.N; j++ {
				s, e := db.AcquireMVCCReadCut()
				if e != nil {
					b.Fatal(e)
				}
				if n == 1 {
					r[0].Key, r[0].Value, r[0].Found, r[0].Err = s.SeekGEVersionRange(a[0], z[0])
				} else if e = s.SeekGEVersionRanges(a, z, r); e != nil {
					b.Fatal(e)
				}
				if e = s.Close(); e != nil {
					b.Fatal(e)
				}
				for _, v := range r {
					if v.Err != nil || !v.Found || !bytes.Equal(v.Value, want) {
						b.Fatal(v)
					}
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(reads()-before)/float64(b.N), "callbacks/op")
		})
	}
}

func TestCOWReadBatchLocalConcurrentClose(t *testing.T) {
	db, _, want, _ := cowLeafMissFixture(t, 128, 64)
	s, err := db.AcquireMVCCReadCut()
	if err != nil {
		t.Fatal(err)
	}
	release := cowBatchOccupyShared(t, s)
	defer release()
	a, z, r := cowLocalBatchProbes(32)
	db.cow.physicalMu.Lock()
	readDone := make(chan error, 1)
	go func() { readDone <- s.SeekGEVersionRanges(a, z, r) }()
	deadline := time.Now().Add(5 * time.Second)
	for s.readState.Load() == 0 && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if s.readState.Load() == 0 {
		db.cow.physicalMu.Unlock()
		t.Fatal("read did not retain its original cut")
	}
	if err = s.Close(); err != nil {
		db.cow.physicalMu.Unlock()
		t.Fatal(err)
	}
	db.cow.physicalMu.Unlock()
	select {
	case err = <-readDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("local read/Close deadlocked")
	}
	for _, v := range r {
		if v.Err != nil || !v.Found || !bytes.Equal(v.Value, want) {
			t.Fatal(v)
		}
	}
	if s.cowReader != nil {
		t.Fatal("Close retained local workspace")
	}
}
