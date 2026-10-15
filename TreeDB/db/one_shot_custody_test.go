package db

import (
	"bytes"
	"errors"
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
	if err = r.close(); err != nil {
		t.Fatal(err)
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
