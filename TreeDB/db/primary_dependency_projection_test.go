package db

import (
	"bytes"
	"fmt"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"testing"
)

func TestPrimaryUnchangedOuterLeafBaseReusesExactDependencies(t *testing.T) {
	opts := Options{Dir: t.TempDir(), Durability: DurabilityWALOffRelaxed, IndexPrimaryDirectory: true, IndexOuterLeavesInValueLog: true}
	d, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	log := &registeredLeafPageLog{db: d, dir: d.dir}
	if err = log.ensureWriter(); err != nil {
		t.Fatal(err)
	}
	d.SetLeafPageLog(log)
	defer closeVacuumTestLeafPageLog(t, d, log)
	// A single ordinary batch exceeding the directory capacity creates a genuine
	// outer-log DATA base before the PRIMARY-only replacements.
	b := d.NewBatch()
	for i := 0; i < node.PrimaryDirectoryMaxEntries+2; i++ {
		if err = b.Set([]byte(fmt.Sprintf("seed/%03d", i)), []byte("old")); err != nil {
			t.Fatal(err)
		}
	}
	if err = b.WriteSync(); err != nil {
		t.Fatal(err)
	}
	b.Close()
	if err = d.SetSync([]byte("hot"), []byte("before")); err != nil {
		t.Fatal(err)
	}
	old := d.AcquireSnapshot()
	defer old.Close()
	base, _ := primaryDBDirectory(t, d).Base()
	scans := 0
	d.testScanCandidateExternalReferencesHook = func() { scans++ }
	for i := 0; i < 4; i++ {
		if err = d.SetSync([]byte("hot"), []byte(fmt.Sprintf("after/%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	d.testScanCandidateExternalReferencesHook = nil
	if scans != 0 {
		t.Fatalf("PRIMARY-only replacement scanned unchanged outer-leaf base %d times", scans)
	}
	gotBase, _ := primaryDBDirectory(t, d).Base()
	if gotBase != base {
		t.Fatal("PRIMARY-only write changed materialized base")
	}
	if got, err := old.Get([]byte("hot")); err != nil || !bytes.Equal(got, []byte("before")) {
		t.Fatalf("held cut %q %v", got, err)
	}
	if got, err := d.Get([]byte("hot")); err != nil || !bytes.Equal(got, []byte("after/3")) {
		t.Fatalf("current %q %v", got, err)
	}
}

func TestPrimaryOuterLeafPointerRetargetMatchesFullProjection(t *testing.T) {
	opts := Options{Dir: t.TempDir(), Durability: DurabilityWALOffRelaxed, IndexPrimaryDirectory: true, IndexOuterLeavesInValueLog: true, DisableBackgroundPrune: true}
	d, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	log := &registeredLeafPageLog{db: d, dir: d.dir}
	if err = log.ensureWriter(); err != nil {
		t.Fatal(err)
	}
	d.SetLeafPageLog(log)
	defer closeVacuumTestLeafPageLog(t, d, log)
	first := appendPointersInNewSegment(t, opts.Dir, 0, 1, 10000, 1, func(int) []byte { return []byte("old pointer") })[0]
	second := appendPointersInNewSegment(t, opts.Dir, 0, 2, 20000, 1, func(int) []byte { return []byte("new pointer") })[0]
	if err = d.RefreshValueLogSet(); err != nil {
		t.Fatal(err)
	}
	b := d.NewBatch()
	for i := 0; i < node.PrimaryDirectoryMaxEntries+2; i++ {
		if err = b.Set([]byte(fmt.Sprintf("seed/%03d", i)), []byte("base")); err != nil {
			t.Fatal(err)
		}
	}
	if err = b.WriteSync(); err != nil {
		t.Fatal(err)
	}
	b.Close()
	put := func(ptr page.ValuePtr) {
		t.Helper()
		b := d.NewBatch().(*Batch)
		defer b.Close()
		if err := b.SetPointer([]byte("hot"), ptr); err != nil {
			t.Fatal(err)
		}
		if err := b.WriteSync(); err != nil {
			t.Fatal(err)
		}
	}
	put(first)
	old := d.AcquireSnapshot()
	defer old.Close()
	scans := 0
	d.testScanCandidateExternalReferencesHook = func() { scans++ }
	// Overwrite both durable slots with exact retargeting deltas.
	for _, ptr := range []page.ValuePtr{second, first, second} {
		put(ptr)
		assertCandidateTrackerMatchesFullScan(t, d)
	}
	if scans != 0 {
		t.Fatalf("exact pointer retarget rescanned %d times", scans)
	}
	if got, err := old.Get([]byte("hot")); err != nil || string(got) != "old pointer" {
		t.Fatalf("held pointer %q %v", got, err)
	}
	// An invalid tracker cannot support subtracting the predecessor pointer.
	d.valueLogRefTracker.invalidate()
	put(first)
	if scans == 0 {
		t.Fatal("invalid tracker bypassed full projection")
	}
	assertCandidateTrackerMatchesFullScan(t, d)
	// Exceeding directory capacity really replaces DATA and must still scan.
	scans = 0
	b = d.NewBatch()
	if err = b.Set([]byte("seed/000"), []byte("replaced base cell")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < node.PrimaryDirectoryMaxEntries+2; i++ {
		if err = b.Set([]byte(fmt.Sprintf("new/%03d", i)), []byte("new base")); err != nil {
			t.Fatal(err)
		}
	}
	if err = b.WriteSync(); err != nil {
		t.Fatal(err)
	}
	b.Close()
	if scans == 0 {
		t.Fatal("DATA materialization bypassed full projection")
	}
	d.testScanCandidateExternalReferencesHook = nil
	assertCandidateTrackerMatchesFullScan(t, d)
	if err = old.Close(); err != nil {
		t.Fatal(err)
	}
	closeVacuumTestLeafPageLog(t, d, log)
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got, err := reopened.Get([]byte("hot")); err != nil || string(got) != "old pointer" {
		t.Fatalf("reopened pointer %q %v", got, err)
	}
}
