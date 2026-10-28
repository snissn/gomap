package caching

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func cowPointerWorkspaceFixture(t testing.TB, compressed bool) (*DB, []byte) {
	t.Helper()
	dir := t.TempDir()
	backend, e := backenddb.Open(backenddb.Options{Dir: dir, IndexOuterLeavesInValueLog: true, LeafPageReadCacheEntries: 128})
	if e != nil {
		t.Fatal(e)
	}
	compression := vlogCompressionOff
	if compressed {
		compression = vlogCompressionBlock
	}
	db, e := Open(dir, backend, Options{DisableWAL: true, AllowUnsafe: true, MemtableMode: "cow_ordered", FlushThreshold: 1 << 30, IndexOuterLeavesInValueLog: true, ValueLogPointerThreshold: 1, ValueLogCompression: uint8(compression), ValueLogBlockCodec: 0})
	if e != nil {
		_ = backend.Close()
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if e := db.Close(); e != nil {
			t.Error(e)
		}
	})
	value := bytes.Repeat([]byte("pointer-value"), 64)
	for i := 0; i < 64; i++ {
		if e = db.Set([]byte(fmt.Sprintf("a%03d", i)), value); e != nil {
			t.Fatal(e)
		}
	}
	if e = db.Checkpoint(); e != nil {
		t.Fatal(e)
	}
	return db, value
}
func cowPointerWorkspaceRead(s *Snapshot, successor bool, dst []byte) ([]byte, error) {
	if !successor {
		return s.GetAppend([]byte("a000"), dst)
	}
	_, v, found, e := s.SeekGEVersionRange([]byte("a000"), []byte("a001"))
	if e == nil && !found {
		return nil, fmt.Errorf("missing pointer successor")
	}
	return v, e
}
func BenchmarkCOWPointerWorkspaceFreshCut(b *testing.B) {
	for _, compressed := range []bool{false, true} {
		for _, successor := range []bool{false, true} {
			for _, busy := range []bool{false, true} {
				b.Run(fmt.Sprintf("compressed=%t/successor=%t/busy=%t", compressed, successor, busy), func(b *testing.B) {
					db, want := cowPointerWorkspaceFixture(b, compressed)
					anchor, e := db.AcquireMVCCReadCut()
					if e != nil {
						b.Fatal(e)
					}
					defer anchor.Close()
					if v, e := cowPointerWorkspaceRead(anchor, successor, nil); e != nil || !bytes.Equal(v, want) {
						b.Fatalf("warm: %v", e)
					}
					if busy {
						release := cowBatchOccupyShared(b, anchor)
						defer release()
					}
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						s, e := db.AcquireMVCCReadCut()
						if e != nil {
							b.Fatal(e)
						}
						v, e := cowPointerWorkspaceRead(s, successor, nil)
						if e != nil || !bytes.Equal(v, want) {
							b.Fatalf("read: %v", e)
						}
						if e = s.Close(); e != nil {
							b.Fatal(e)
						}
					}
				})
			}
		}
	}
}

func TestCOWPointerWorkspaceSanitizesAndReuses(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		for _, successor := range []bool{false, true} {
			t.Run(fmt.Sprintf("compressed=%t/successor=%t", compressed, successor), func(t *testing.T) {
				db, want := cowPointerWorkspaceFixture(t, compressed)
				anchor, e := db.AcquireMVCCReadCut()
				if e != nil {
					t.Fatal(e)
				}
				defer anchor.Close()
				if _, e = cowPointerWorkspaceRead(anchor, successor, nil); e != nil {
					t.Fatal(e)
				}
				release := cowBatchOccupyShared(t, anchor)
				defer release()
				baseline := db.cow.budget.Stats()
				var previous *cowReadWorkspace
				var owned []byte
				for i := 0; i < 3; i++ {
					s, e := db.AcquireMVCCReadCut()
					if e != nil {
						t.Fatal(e)
					}
					v, e := cowPointerWorkspaceRead(s, successor, nil)
					if e != nil || !bytes.Equal(v, want) {
						t.Fatalf("read: %v", e)
					}
					w := s.cowReader
					if w == nil {
						t.Fatal("pointer read did not admit original workspace")
					}
					if previous != nil && w != previous {
						t.Fatal("fresh pointer read did not reuse sanitized workspace")
					}
					if i == 0 {
						owned = v
					}
					codecs := append([]*cowReadCodec(nil), w.codecs...)
					point := w.point
					if e = s.Close(); e != nil {
						t.Fatal(e)
					}
					if w.snapshot != nil || w.basis != nil || w.codecs != nil || w.input != nil || w.output != nil || w.allocations != nil || w.lease != nil || w.budget == nil {
						t.Fatal("pointer scratch was not sanitized and retained")
					}
					if point != nil && point.lease != nil {
						t.Fatal("parked point kept old receipt")
					}
					for _, c := range codecs {
						if c != nil && (c.definition != nil || c.decoder != nil || c.input != nil || c.raw != nil || c.lease != nil) {
							t.Fatal("codec authority or buffers survived sanitization")
						}
					}
					payload := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowReadWorkspace{}))) + cowReadWorkspaceInspectionCharge()
					if point != nil {
						payload += memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowPointScratch{})))
					}
					if after := db.cow.budget.Stats(); after.ReservedBytes != baseline.ReservedBytes+payload || after.ExternalLeases != baseline.ExternalLeases {
						t.Fatalf("value/codec admission not refunded: base%+v after%+v expected payload%d", baseline, after, payload)
					}
					previous = w
				}
				if !bytes.Equal(owned, want) {
					t.Fatal("owned result changed after workspace reuse")
				}
			})
		}
	}
}

func TestCOWPointerWorkspacePrefixAndPointlessReceipts(t *testing.T) {
	db, want := cowPointerWorkspaceFixture(t, true)
	if e := db.Set([]byte("cached"), want); e != nil {
		t.Fatal(e)
	}
	anchor, e := db.AcquireMVCCReadCut()
	if e != nil {
		t.Fatal(e)
	}
	defer anchor.Close()
	if _, e = anchor.GetAppend([]byte("cached"), nil); e != nil {
		t.Fatal(e)
	}
	baseline := db.cow.budget.Stats()
	var previous *cowReadWorkspace
	var owned []byte
	for i := 0; i < 3; i++ {
		s, e := db.AcquireMVCCReadCut()
		if e != nil {
			t.Fatal(e)
		}
		dst := make([]byte, 5, 5+len(want)+16)
		copy(dst, "keep:")
		out, e := s.GetAppend([]byte("cached"), dst)
		if e != nil || string(out[:5]) != "keep:" || !bytes.Equal(out[5:], want) || &out[0] != &dst[0] {
			t.Fatalf("prefix/spare-capacity append: %v", e)
		}
		w := s.cowReader
		if w == nil || w.point != nil {
			t.Fatal("cached pointer should admit metadata only")
		}
		if previous != nil && w != previous {
			t.Fatal("pointless workspace was not reused")
		}
		old := w.lease
		if e = s.Close(); e != nil {
			t.Fatal(e)
		}
		if i == 0 {
			owned = out
		}
		previous = w
		expected := baseline.ReservedBytes + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowReadWorkspace{}))) + cowReadWorkspaceInspectionCharge()
		parked := db.cow.budget.Stats()
		if parked.ReservedBytes != expected || parked.ExternalLeases != baseline.ExternalLeases {
			t.Fatalf("one receipt accounting: %+v expected%d", parked, expected)
		}
		old.Close()
		if !errors.Is(old.Resize(1), memtable.ErrCOWClosed) || !errors.Is(old.ShrinkNativeBytes(1, 0), memtable.ErrCOWClosed) {
			t.Fatal("stale metadata receipt remains mutable")
		}
		if _, e = old.SplitNativeBytes(1, 0); !errors.Is(e, memtable.ErrCOWClosed) {
			t.Fatal("stale metadata receipt split")
		}
		if db.cow.budget.Stats() != parked {
			t.Fatal("stale receipt changed idle accounting")
		}
	}
	if string(owned[:5]) != "keep:" || !bytes.Equal(owned[5:], want) {
		t.Fatal("reborrow changed caller destination")
	}
}

func TestCOWPointerWorkspaceWaitsForLastAdmittedRead(t *testing.T) {
	db, want := cowPointerWorkspaceFixture(t, true)
	s, e := db.AcquireMVCCReadCut()
	if e != nil {
		t.Fatal(e)
	}
	if e = s.beginRead(); e != nil {
		t.Fatal(e)
	}
	out, _, e := s.cowGetVersionedAppendOpen([]byte("a000"), nil)
	if e != nil {
		t.Fatal(e)
	}
	w := s.cowReader
	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()
	if e = <-closed; e != nil {
		t.Fatal(e)
	}
	if s.finalized.Load() || w.snapshot != s || w.lease == nil {
		t.Fatal("active read lost scratch before ending admission")
	}
	out2, _, e := s.cowGetVersionedAppendOpen([]byte("a000"), nil)
	if e != nil || !bytes.Equal(out2, want) {
		t.Fatal("already-admitted read failed after close request")
	}
	s.endRead()
	if w.snapshot != nil || w.basis != nil || w.lease != nil || w.output != nil || w.codecs != nil || w.allocations != nil {
		t.Fatal("last read did not sanitize its scratch")
	}
	if !bytes.Equal(out, want) {
		t.Fatal("closing scratch changed owned result")
	}
}
