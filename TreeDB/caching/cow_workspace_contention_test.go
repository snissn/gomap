package caching

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
)

// These controls force contention without scheduler timing: every shared owner
// is held throughout each fresh cut's public read. No private scratch is injected.
func TestCOWWorkspaceFreshCutContentionOwnershipAndRefund(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		for _, busy := range []bool{false, true} {
			t.Run(fmt.Sprintf("compressed=%t/busy=%t", compressed, busy), func(t *testing.T) {
				db, _, want := cowLeafCacheModeFixture(t, 128, compressed, "cow_ordered")
				anchor, err := db.AcquireMVCCReadCut()
				if err != nil {
					t.Fatal(err)
				}
				defer anchor.Close()
				// Warm the shared traversal, physical decoder and leaf cache before taking
				// the accounting baseline, so the delta isolates the fresh cut's fallback.
				a, z, _ := cowLocalBatchProbes(2)
				if _, _, _, err = anchor.SeekGEVersionRange(a[0], z[0]); err != nil {
					t.Fatal(err)
				}
				if busy {
					release := cowBatchOccupyShared(t, anchor)
					defer release()
				}
				before := db.cow.budget.Stats()
				var owned []byte
				for i := 0; i < 3; i++ {
					s, e := db.AcquireMVCCReadCut()
					if e != nil {
						t.Fatal(e)
					}
					r := make([]mvcckey.VersionRangeResult, len(a))
					if e = s.SeekGEVersionRanges(a, z, r); e != nil {
						t.Fatal(e)
					}
					for j := range r {
						if r[j].Err != nil || !r[j].Found || !bytes.Equal(r[j].Key, a[j]) || !bytes.Equal(r[j].Value, want) {
							t.Fatalf("result %d: %+v", j, r[j])
						}
					}
					if busy && (s.cowReader == nil || s.cowReader.point == nil) {
						t.Fatal("busy owners did not force cut-local scratch")
					}
					if !busy && s.cowReader != nil {
						t.Fatal("free shared owner unexpectedly created local scratch")
					}
					if i == 0 {
						owned = r[0].Value
						r[1].Value[0] ^= 0xff
						if !bytes.Equal(owned, want) {
							t.Fatal("probe outputs alias")
						}
					}
					w := s.cowReader
					if e = s.Close(); e != nil {
						t.Fatal(e)
					}
					if w != nil && (w.snapshot != nil || w.basis != nil || w.lease != nil || w.codecs != nil || (w.point != nil && w.point.lease != nil)) {
						t.Fatal("closed fallback retained authority or lease capability")
					}
					after := db.cow.budget.Stats()
					if i == 0 && busy {
						before = after
					}
					if after.ReservedBytes != before.ReservedBytes || after.ExternalLeases != before.ExternalLeases {
						t.Fatalf("fresh-cut refund mismatch: before%+v after%+v", before, after)
					}
				}
				if !bytes.Equal(owned, want) {
					t.Fatal("owned output changed after workspace reuse and close")
				}
			})
		}
	}
}

func BenchmarkCOWWorkspaceFreshCutContention(b *testing.B) {
	for _, compressed := range []bool{false, true} {
		for _, busy := range []bool{false, true} {
			b.Run(fmt.Sprintf("compressed=%t/busy=%t", compressed, busy), func(b *testing.B) {
				db, _, want := cowLeafCacheModeFixture(b, 128, compressed, "cow_ordered")
				anchor, err := db.AcquireMVCCReadCut()
				if err != nil {
					b.Fatal(err)
				}
				defer anchor.Close()
				a, z, r := cowLocalBatchProbes(32)
				if err = anchor.SeekGEVersionRanges(a, z, r); err != nil {
					b.Fatal(err)
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
					if e = s.SeekGEVersionRanges(a, z, r); e != nil {
						b.Fatal(e)
					}
					for j := range r {
						if r[j].Err != nil || !r[j].Found || !bytes.Equal(r[j].Value, want) {
							b.Fatalf("result %d: %+v", j, r[j])
						}
					}
					if e = s.Close(); e != nil {
						b.Fatal(e)
					}
				}
			})
		}
	}
}

func TestCOWWorkspaceIdleReuseAcrossCapturedBases(t *testing.T) {
	db, _, original := cowLeafCacheModeFixture(t, 128, false, "cow_ordered")
	old, err := db.AcquireMVCCReadCut()
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	a, z, r := cowLocalBatchProbes(2)
	first, err := db.AcquireMVCCReadCut()
	if err != nil {
		t.Fatal(err)
	}
	release := cowBatchOccupyShared(t, old)
	if err = first.SeekGEVersionRanges(a, z, r); err != nil {
		t.Fatal(err)
	}
	scratch := first.cowReader
	owned := r[0].Value
	if scratch == nil || !bytes.Equal(owned, original) {
		t.Fatal("first local read")
	}
	if err = first.Close(); err != nil {
		t.Fatal(err)
	}
	release()
	changed := bytes.Repeat([]byte("new-inline-value"), 8)
	if err = db.Set(a[0], changed); err != nil {
		t.Fatal(err)
	}
	if err = db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	current, err := db.AcquireMVCCReadCut()
	if err != nil {
		t.Fatal(err)
	}
	if err = current.SeekGEVersionRanges(a, z, r); err != nil {
		t.Fatal(err)
	}
	release = cowBatchOccupyShared(t, current)
	err = current.SeekGEVersionRanges(a, z, r)
	release()
	if err != nil {
		t.Fatal(err)
	}
	// Checkpoint admission may legitimately revoke the first idle entry.
	// The current traversal supplies the entry borrowed by the retained cut.
	scratch = current.cowReader
	if scratch == nil {
		t.Fatal("current cut did not establish local scratch")
	}
	if r[0].Err != nil || !bytes.Equal(r[0].Value, changed) || !bytes.Equal(owned, original) {
		t.Fatalf("cross-cut values: %+v owned%q", r[0], owned)
	}
	if err = current.Close(); err != nil {
		t.Fatal(err)
	}
	release = cowBatchOccupyShared(t, old)
	err = old.SeekGEVersionRanges(a, z, r)
	release()
	if err != nil {
		t.Fatal(err)
	}
	if old.cowReader != scratch || r[0].Err != nil || !bytes.Equal(r[0].Value, original) || !bytes.Equal(owned, original) {
		t.Fatalf("retained old cut scratch=%p want=%p value=%+v", old.cowReader, scratch, r[0])
	}
}
