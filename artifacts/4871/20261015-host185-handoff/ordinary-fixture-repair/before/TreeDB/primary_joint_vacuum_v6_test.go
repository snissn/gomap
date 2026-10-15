package treedb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/tree"
	"io"
	"path/filepath"
	"testing"
)

func TestPrimaryJointVacuumV6DefaultOuterLeafMaterializationReaderAndCut(t *testing.T) {
	opts := OptionsFor(ProfileNoWALFast, t.TempDir())
	if !opts.IndexOuterLeavesInValueLog {
		t.Fatal("default outer-leaf route lost")
	}
	opts.ValueLog.ForcePointers = true
	opts.ValueLog.PointerThreshold = 1
	d, e := Open(opts)
	if e != nil {
		t.Fatal(e)
	}
	defer func() {
		if d != nil {
			d.Close()
		}
	}()
	value := bytes.Repeat([]byte("joint-persistent-value"), 100)
	b := d.NewBatchWithSize(200)
	for i := 0; i < 200; i++ {
		if e = b.Set([]byte(fmt.Sprintf("base-%03d", i)), value); e != nil {
			t.Fatal(e)
		}
	}
	if e = b.WriteSync(); e != nil {
		t.Fatal(e)
	}
	b.Close()
	held := d.AcquireSnapshot()
	defer held.Close()
	for _, k := range []string{"later-first", "later-second"} {
		if e = d.Set([]byte(k), value); e != nil {
			t.Fatal(e)
		}
	}
	if e = d.Checkpoint(); e != nil {
		t.Fatal(e)
	}
	large := bytes.Repeat([]byte("joint-real-data-materialization"), 120)
	if e = d.SetSync(large, value); e != nil {
		t.Fatal(e)
	}
	snapshot := d.backend.AcquireSnapshot()
	parents := make(map[string]bool)
	for _, f := range snapshot.State().ValueLogSet.Files {
		parents[filepath.Dir(f.Path)] = true
	}
	snapshot.Close()
	if len(parents) < 2 {
		t.Fatal("missing real value and leaf parents")
	}
	cut, e := d.backend.CapturePhysicalSnapshotCutV1(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	if e = d.backend.VacuumIndexOnline(context.Background()); !errors.Is(e, rootpublication.ErrResourcePinned) {
		t.Fatalf("held cut refusal: %v", e)
	}
	if e = cut.Close(); e != nil {
		t.Fatal(e)
	}
	before := d.backend.State().CommitSeq
	if e = d.backend.VacuumIndexOnline(context.Background()); e != nil {
		t.Fatal(e)
	}
	if d.backend.State().CommitSeq != before+1 {
		t.Fatal("joint publication did not advance once")
	}
	if got, e := held.Get([]byte("base-003")); e != nil || !bytes.Equal(got, value) {
		t.Fatalf("old reader: %v", e)
	}
	if got, e := held.Get([]byte("later-second")); !errors.Is(e, tree.ErrKeyNotFound) || got != nil {
		t.Fatalf("old reader newer: %v", e)
	}
	cut, e = d.backend.CapturePhysicalSnapshotCutV1(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer cut.Close()
	for i := 0; i < 4; i++ {
		if e = d.SetSync([]byte(fmt.Sprintf("after-joint-%d", i)), value); e != nil {
			t.Fatal(e)
		}
	}
	if e = d.Close(); e != nil {
		t.Fatal(e)
	}
	d = nil
	if e = cut.WriteToContext(context.Background(), io.Discard); e != nil {
		t.Fatalf("joint cut after Close: %v", e)
	}
	if e = cut.WritePrimaryToContext(context.Background(), io.Discard); e != nil {
		t.Fatalf("joint PRIMARY cut after Close: %v", e)
	}
	if e = cut.Close(); e != nil {
		t.Fatal(e)
	}
	if e = held.Close(); e != nil {
		t.Fatal(e)
	}
	d, e = Open(opts)
	if e != nil {
		t.Fatal(e)
	}
	for _, k := range [][]byte{[]byte("base-003"), []byte("later-first"), []byte("later-second"), large, []byte("after-joint-3")} {
		if got, e := d.Get(k); e != nil || !bytes.Equal(got, value) {
			t.Fatalf("reopen %s: %v", k, e)
		}
	}
}
