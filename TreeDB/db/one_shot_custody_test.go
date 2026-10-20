package db

import (
	"bytes"
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestOneShotReadCustodyMatchesCapturedResources(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if err = d.SetSync([]byte("inline"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	got, _, err := d.GetVersioned([]byte("inline"))
	if err != nil || !bytes.Equal(got, []byte("value")) {
		t.Fatalf("public read: %q %v", got, err)
	}
	r, err := d.acquireOneShotReadOrErr()
	if err != nil {
		t.Fatal(err)
	}
	if r.snapshot.state.ValueLogSet != nil && len(r.snapshot.state.ValueLogSet.Files) != 0 {
		t.Fatal("fixture is not empty-log")
	}
	if r.snapshot.vlogManager != nil || r.snapshot.vlogWrapperBorrowed || r.snapshot.vlogPinned {
		t.Fatal("private empty-log read acquired unrelated Manager custody")
	}
	if r.snapshot.registryID == 0 || !r.snapshot.indexWrapperBorrowed {
		t.Fatal("private read lost original generation pin")
	}
	// This private guard never enters alternate-root, historical-rebind, or
	// iterator APIs. Only its main Tree/reader acquires live aliases.
	checkUnused := func() {
		t.Helper()
		snap := &r.snapshot
		if len(snap.rootTrees) != 0 || snap.rootTreeFirst[0].root != 0 || !reflect.ValueOf(snap.rootTreeFirst[0].tree).IsZero() || len(snap.iterators) != 0 {
			t.Fatal("private one-shot used exported root/iterator state")
		}
	}
	checkUnused()
	got, _, err = r.snapshot.tree.GetVersionedAppend([]byte("inline"), nil)
	if err != nil || !bytes.Equal(got, []byte("value")) {
		t.Fatalf("private main Tree read: %q %v", got, err)
	}
	checkUnused()
	if err = r.close(); err != nil {
		t.Fatal(err)
	}
	checkUnused()
	if r.snapshot.treePager != nil || r.snapshot.treeRoot != 0 || r.snapshot.reader.vlogs != nil || r.snapshot.state != nil || r.snapshot.idx != nil {
		t.Fatal("private pool retained main Tree/reader/owner aliases")
	}
	s := d.AcquireSnapshot()
	if s == nil || s.vlogManager == nil || !s.vlogWrapperBorrowed {
		t.Fatal("exported snapshot lost Manager token authority")
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOneShotLiveValueLogReadSurvivesConcurrentDBClose(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir, DisableBackgroundPrune: true, ValueLog: ValueLogOptions{PointerThreshold: 1}})
	if err != nil {
		t.Fatal(err)
	}
	id := page.ValueLogFileID(1)
	writer, err := valuelog.NewWriterWithStableResourcePinRegistry(valuelog.SegmentPath(ValueLogDirPath(dir), id), id, d.valueLogIdentityPins)
	if err != nil {
		t.Fatal(err)
	}
	value := bytes.Repeat([]byte("physical-value"), 400)
	ptr, err := writer.Append(0, nil, 1, value)
	if err != nil {
		t.Fatal(err)
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err = d.RefreshValueLogSet(); err != nil {
		t.Fatal(err)
	}
	key := []byte("live-key")
	setValueLogPointer(t, d, key, ptr)
	got, _, err := d.GetVersioned(key)
	if err != nil || !bytes.Equal(got, value) {
		t.Fatalf("public pointer read: %v", err)
	}
	r, err := d.acquireOneShotReadOrErr()
	if err != nil {
		t.Fatal(err)
	}
	if r.snapshot.vlogManager == nil || !r.snapshot.vlogWrapperBorrowed || !r.snapshot.vlogPinned {
		t.Fatal("live-log read bypassed original Manager/Set custody")
	}
	generation := r.snapshot.idx
	start := make(chan struct{})
	var wg sync.WaitGroup
	results := make(chan error, 2)
	wg.Add(2)
	go func() { defer wg.Done(); <-start; results <- d.Close() }()
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 64; i++ {
			got, _, e := d.GetVersioned(key)
			if errors.Is(e, ErrClosed) {
				break
			}
			if e != nil || !bytes.Equal(got, value) {
				results <- errors.New("public concurrent pointer read lost captured value")
				return
			}
		}
		results <- nil
	}()
	close(start)
	wg.Wait()
	close(results)
	for e := range results {
		if e != nil {
			t.Fatal(e)
		}
	}
	got, err = r.snapshot.reader.Read(ptr)
	if err != nil || !bytes.Equal(got, value) {
		t.Fatalf("held physical value-log pointer after DB.Close: %v", err)
	}
	if generation.metadataCharge == 0 {
		t.Fatal("DB.Close refunded held generation")
	}
	if err = r.close(); err != nil {
		t.Fatal(err)
	}
	if generation.wrapperBorrowCountV6() != 0 || generation.metadataCharge != 0 {
		t.Fatal("last private read did not complete original wrapper")
	}
}

func TestOneShotEmptyReadRetainsOnlyLegacyStateWrapper(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if err = d.SetSync([]byte("inline"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err = d.RefreshValueLogSet(); err != nil {
		t.Fatal(err)
	}
	original := d.state.Load().ValueLogSet
	sizes, known := original.RetentionSizes()
	if original == nil || !known || sizes.FileCount != 0 {
		t.Fatal("fixture has no owned empty Set")
	}
	r, err := d.acquireOneShotReadOrErr()
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = r.close()
		}
	}()
	if r.snapshot.state.ValueLogSet != original || r.snapshot.reader.vlogs != nil || r.snapshot.vlogManager != nil || r.snapshot.vlogPinned {
		t.Fatal("private read retained an active empty Set/Manager edge")
	}
	if err = d.RefreshValueLogSet(); err != nil {
		t.Fatal(err)
	}
	if original.RefCount.Load() != 0 || original.Files != nil {
		t.Fatal("fixture did not complete original empty Set release")
	}
	// The existing unadmitted DBState/Set wrapper remains GC reachable, but the
	// read uses only its scalar root state and the reader has no Set interface.
	if r.snapshot.state.ValueLogSet != original {
		t.Fatal("legacy state alias changed")
	}
	got, _, err := r.snapshot.GetVersioned([]byte("inline"))
	if err != nil || !bytes.Equal(got, []byte("value")) {
		t.Fatalf("released-empty inline read: %q %v", got, err)
	}
	if err = r.close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	if r.snapshot.state != nil || r.snapshot.reader.vlogs != nil {
		t.Fatal("private completion did not scrub legacy state")
	}
}
