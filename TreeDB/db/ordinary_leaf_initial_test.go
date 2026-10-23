package db

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

// Like cached COW, this producer immediately registers rotations without
// retaining a pending-created inventory. A file's registration is not proof
// that the final root references it.
type rotatingRegisteredLeafLog struct {
	registeredLeafPageLog
	seq uint32
}

func (l *rotatingRegisteredLeafLog) rotate() error {
	if l.w != nil {
		if err := l.w.Close(); err != nil {
			return err
		}
		l.w = nil
	}
	if err := os.MkdirAll(LeafLogDirPath(l.dir), 0755); err != nil {
		return err
	}
	l.seq++
	id, err := valuelog.EncodeFileID(rewriteLeafLogLaneID, l.seq)
	if err != nil {
		return err
	}
	l.fileID = id
	l.path = filepath.Join(LeafLogDirPath(l.dir), fmt.Sprintf("value-l%d-%06d.log", rewriteLeafLogLaneID, l.seq))
	l.w, err = valuelog.NewWriter(l.path, id)
	if err != nil {
		return err
	}
	l.registered = true
	return l.db.RegisterValueLogSegment(l.path, id)
}
func (l *rotatingRegisteredLeafLog) AppendLeafPage(data []byte) (page.LeafLogPtr, error) {
	if l.w == nil || l.w.Size() >= 8<<10 {
		if err := l.rotate(); err != nil {
			return page.LeafLogPtr{}, err
		}
	}
	return l.registeredLeafPageLog.AppendLeafPage(data)
}

func TestOrdinaryDATAFirstEmptyRootReceiptCapturesRotatedClosure(t *testing.T) {
	opts := Options{Dir: t.TempDir(), Durability: DurabilityWALOffRelaxed, IndexOuterLeavesInValueLog: true, DisableBackgroundPrune: true}
	d, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if d != nil {
			_ = d.Close()
		}
	}()
	log := &rotatingRegisteredLeafLog{registeredLeafPageLog: registeredLeafPageLog{db: d, dir: d.dir}}
	d.SetLeafPageLog(log)
	if err := log.rotate(); err != nil {
		t.Fatal(err)
	}
	if err := log.rotate(); err != nil {
		t.Fatal(err)
	} // first registered file is deliberately unreachable.
	// Open may initialize a raw census before the cached producer is installed.
	// Model the actual first COW publication: logical tracker valid, raw unknown.
	d.valueLogRefTracker.mu.Lock()
	d.valueLogRefTracker.rawValid = false
	d.valueLogRefTracker.rawCounts.close()
	d.valueLogRefTracker.mu.Unlock()
	scans := 0
	d.testScanCandidateExternalReferencesHook = func() { scans++ }
	b := d.NewBatch()
	defer b.Close()
	for i := 0; i < 1024; i++ {
		if err := b.Set([]byte(fmt.Sprintf("key/%05d", i)), bytes.Repeat([]byte{byte(i)}, 128)); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.WriteSync(); err != nil {
		t.Fatal(err)
	}
	if scans != 0 {
		t.Fatalf("first exact empty-basis publication scanned %d times", scans)
	}
	if log.seq < 3 {
		t.Fatalf("control did not cross 8KiB segments: %d", log.seq)
	}
	assertOrdinaryRawCensus(t, d)
	// The next publication also excludes every unreferenced registration,
	// exercising the normal populated projection across another rotation.
	if err := log.rotate(); err != nil {
		t.Fatal(err)
	}
	if err := d.SetSync([]byte("key/00000"), []byte("new")); err != nil {
		t.Fatal(err)
	}
	if scans != 0 {
		t.Fatalf("populated exact publication scanned %d times", scans)
	}
	assertOrdinaryRawCensus(t, d)
	// A populated tree with missing raw evidence cannot seed zero or trust
	// registered/current files. An append-only update must use the maintained
	// exact scanner, then reestablish the ordinary census.
	d.valueLogRefTracker.mu.Lock()
	d.valueLogRefTracker.rawValid = false
	d.valueLogRefTracker.rawCounts.close()
	d.valueLogRefTracker.mu.Unlock()
	if err := d.SetSync([]byte("new/key"), []byte("new")); err != nil {
		t.Fatal(err)
	}
	if scans == 0 {
		t.Fatal("populated unknown raw basis bypassed exact scanner")
	}
	assertOrdinaryRawCensus(t, d)
	d.testScanCandidateExternalReferencesHook = nil
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	d.SetLeafPageLog(nil)
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d = nil
	d, err = Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i < 1024; i++ {
		got, err := d.Get([]byte(fmt.Sprintf("key/%05d", i)))
		if err != nil || !bytes.Equal(got, bytes.Repeat([]byte{byte(i)}, 128)) {
			t.Fatalf("reopen key%d err=%v", i, err)
		}
	}
}
