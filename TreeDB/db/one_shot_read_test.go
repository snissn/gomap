package db

import (
	"bytes"
	"errors"
	"math"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/lifecycle"
	"github.com/snissn/gomap/TreeDB/tree"
)

func TestOneShotReadAllocationBudget(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	key := []byte("key")
	if err := d.SetSync(key, []byte("value123")); err != nil {
		t.Fatal(err)
	}
	dst := make([]byte, 0, 64)
	for _, tc := range []struct {
		name   string
		read   func() error
		budget float64
	}{
		{"Get", func() error { _, err := d.Get(key); return err }, 1},
		{"GetAppend", func() error { _, err := d.GetAppend(key, dst); return err }, 0},
		{"GetVersioned", func() error { _, _, err := d.GetVersioned(key); return err }, 1},
		{"GetVersionedAppend", func() error { _, _, err := d.GetVersionedAppend(key, dst); return err }, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			epoch := d.snapshotAcquireEpoch.Load()
			allocs := testing.AllocsPerRun(100, func() {
				if err := tc.read(); err != nil {
					t.Fatal(err)
				}
			})
			if acquired := d.snapshotAcquireEpoch.Load() - epoch; acquired != 202 {
				t.Fatalf("capture epoch advanced %d, want 202 (101 capture/release pairs)", acquired)
			}
			if d.MinPinnedSnapshotCommitSeq() != math.MaxUint64 {
				t.Fatal("read leaked a registry pin")
			}
			t.Logf("allocations/read=%.0f capture/release pairs=101 live registry pins=0", allocs)
			if allocs > tc.budget {
				t.Fatalf("%.0f allocations/read, want <= %.0f (owned result only)", allocs, tc.budget)
			}
		})
	}
}

func TestOneShotReadOwnedMissingEmptyAndNilKey(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	for _, key := range [][]byte{nil, []byte("empty"), []byte("owned")} {
		value := []byte{}
		if bytes.Equal(key, []byte("owned")) {
			value = []byte("value123")
		}
		if err := d.SetSync(key, value); err != nil {
			t.Fatal(err)
		}
	}
	stale := d.AcquireSnapshot()
	if err := stale.Close(); err != nil {
		t.Fatal(err)
	}
	for _, key := range [][]byte{nil, {}, []byte("empty"), []byte("owned"), []byte("missing")} {
		got, rev, err := d.GetVersioned(key)
		if err != nil {
			t.Fatal(err)
		}
		plain, err := d.Get(key)
		if err != nil || !bytes.Equal(plain, got) || (plain == nil) != (got == nil) {
			t.Fatalf("Get(%q)=%q, %v; versioned=%q", key, plain, err, got)
		}
		base := []byte("prefix:")
		out, appendRev, appendErr := d.GetVersionedAppend(key, base)
		wantErr := error(nil)
		if got == nil {
			wantErr = tree.ErrKeyNotFound
		}
		if !errors.Is(appendErr, wantErr) || appendRev != rev || !bytes.Equal(out, append(append([]byte{}, base...), got...)) {
			t.Fatalf("GetVersionedAppend(%q)=%q,%d,%v; value=%q revision=%d", key, out, appendRev, appendErr, got, rev)
		}
		out, appendErr = d.GetAppend(key, base)
		if !errors.Is(appendErr, wantErr) || !bytes.Equal(out, append(append([]byte{}, base...), got...)) {
			t.Fatalf("GetAppend(%q)=%q,%v", key, out, appendErr)
		}
		if len(plain) > 0 {
			plain[0] ^= 0xff
			again, err := d.Get(key)
			if err != nil || !bytes.Equal(again, got) {
				t.Fatalf("returned bytes alias pooled state: %q, %v", again, err)
			}
		}
	}
	if _, err := stale.Get([]byte("owned")); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed exported alias became readable: %v", err)
	}
}

func TestOneShotReadCapturePublicationAndRelease(t *testing.T) {
	idx := &indexGen{registry: lifecycle.NewReaderRegistry()}
	idx.refs.Store(1)
	d := &DB{snapPool: NewSnapshotPool()}
	d.idx.Store(idx)
	state1 := &DBState{CommitSeq: 11, RootPageID: 1}
	state2 := &DBState{CommitSeq: 22, RootPageID: 2}
	d.publishSnapshotView(idx, state1, nil)

	// A pending capture must see the view published under the exclusive lock.
	d.valueLogPublicationMu.Lock()
	started := make(chan struct{})
	done := make(chan *oneShotRead)
	go func() {
		close(started)
		r, err := d.acquireOneShotReadOrErr()
		if err != nil {
			t.Error(err)
		}
		done <- r
	}()
	<-started
	d.publishSnapshotView(idx, state2, nil)
	d.valueLogPublicationMu.Unlock()
	r := <-done
	if r == nil {
		t.Fatal("capture failed")
	}
	if r.snapshot.state != state2 || idx.registry.MinPinnedSeq() != state2.CommitSeq || d.snapshotAcquireInFlight() != 0 {
		t.Fatal("capture did not register the coherent published view")
	}
	if err := r.close(); err != nil {
		t.Fatal(err)
	}
	if idx.registry.MinPinnedSeq() != math.MaxUint64 || r.snapshot.db != nil || r.snapshot.idx != nil || r.snapshot.state != nil || r.snapshot.reader.vlogs != nil || r.snapshot.treePager != nil || r.snapshot.treeRoot != 0 || r.snapshot.registryID != 0 {
		t.Fatal("released guard retained capture resources")
	}

	// Closing while a capture is waiting for root reuse rejects the capture.
	d.rootReuseMu.Lock()
	closed := make(chan error)
	go func() {
		r, err := d.acquireOneShotReadOrErr()
		if r != nil {
			_ = r.close()
		}
		closed <- err
	}()
	d.closing.Store(true)
	d.rootReuseMu.Unlock()
	if err := <-closed; !errors.Is(err, ErrClosed) {
		t.Fatalf("capture during close: %v", err)
	}
	d.closing.Store(false)
	d.publicationPoisoned.Store(true)
	if r, err := d.acquireOneShotReadOrErr(); r != nil || err == nil {
		t.Fatalf("poisoned capture=%v, %v", r, err)
	}
	if idx.registry.MinPinnedSeq() != math.MaxUint64 {
		t.Fatal("rejected capture leaked registry pin")
	}
}

func TestOneShotReadRejectedCaptureReleasesValueLogPin(t *testing.T) {
	idx := &indexGen{}
	idx.refs.Store(1)
	segment := &valuelog.File{}
	segment.RefCount.Store(1)
	set := &valuelog.Set{Files: map[uint32]*valuelog.File{1: segment}}
	d := &DB{snapPool: NewSnapshotPool()}
	d.publishSnapshotView(idx, &DBState{CommitSeq: 1, RootPageID: 1, ValueLogSet: set}, &valuelog.Manager{})
	if read, err := d.acquireOneShotReadOrErr(); read != nil || !errors.Is(err, ErrClosed) {
		t.Fatalf("capture without registry=%v, %v", read, err)
	}
	if set.RefCount.Load() != 0 || segment.RefCount.Load() != 0 || d.snapshotAcquireInFlight() != 0 {
		t.Fatal("failed capture leaked a value-log or acquisition pin")
	}
}
