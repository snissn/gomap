package caching

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

// Unknown producers do not participate in the shared reservation allocator.
// Preserve the conservative fence for those files, including their identity.
func TestLeafMaintenanceFencesUnreservedExternalSequence(t *testing.T) {
	db, captured, _ := openCachingLeafPageLogLaneTestDB(t)
	defer db.Close()
	provider := captured.leafLog.(backenddb.LeafPageLogLaneProvider)
	appenders := []backenddb.LeafPageLog{captured.leafLog}
	for i := 1; i <= 2; i++ {
		appender, ok := provider.LeafPageLogLane(i)
		if !ok {
			t.Fatalf("missing leaf lane %d", i)
		}
		appenders = append(appenders, appender)
	}
	for i, a := range appenders {
		if _, err := a.AppendLeafPage(testLeafPageBytes(fmt.Sprintf("before-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	externalSeq := int(db.leafLogAppendSeq.Load()) + 10
	externalID, err := valuelog.EncodeFileID(uint32(leafLogLaneID), uint32(externalSeq))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(db.leafLogDir, valueLogName(leafLogLaneID, externalSeq))
	writer, err := valuelog.NewWriter(path, externalID)
	if err != nil {
		t.Fatal(err)
	}
	payload := testLeafPageBytes("external-maintenance")
	ptr, err := writer.Append(0, nil, 1, payload)
	if err != nil {
		_ = writer.Close()
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.RunBackendMaintenance(nil); err != nil {
		t.Fatal(err)
	}
	seen := make(map[int]bool)
	for _, l := range db.leafLogAppendLanesSnapshot() {
		if l.vlogSeq <= externalSeq || seen[l.vlogSeq] {
			t.Fatalf("external fence lost: seq=%d external=%d duplicate=%t", l.vlogSeq, externalSeq, seen[l.vlogSeq])
		}
		seen[l.vlogSeq] = true
	}
	for i, a := range appenders {
		if _, err := a.AppendLeafPage(testLeafPageBytes(fmt.Sprintf("after-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, after) || after.Size() != before.Size() {
		t.Fatalf("external file changed: before=%d after=%d", before.Size(), after.Size())
	}
	got, err := db.valueLogReader.Read(ptr)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("external pointer: err=%v equal=%t", err, bytes.Equal(got, payload))
	}
}

type leafGCPeriodicBackend struct {
	*leafPackMaintenanceRecordingBackend
	valueCalls, estimateCalls int
}

func (b *leafGCPeriodicBackend) ValueLogGC(_ context.Context, opts backenddb.ValueLogGCOptions) (backenddb.ValueLogGCStats, error) {
	if opts.DryRun {
		b.estimateCalls++
	} else {
		b.valueCalls++
	}
	return backenddb.ValueLogGCStats{}, nil
}

func openLeafGCPeriodicTestDB(t *testing.T, stats backenddb.LeafGenerationGCStats, leafErr error) (*DB, *leafGCPeriodicBackend) {
	t.Helper()
	prepareDirectSchedulerTest(t)
	t.Setenv(envDisableVlogGenerationRewrite, "1")
	t.Setenv(envEnableLeafGenerationPackMaintenance, "0")
	backend := &leafGCPeriodicBackend{leafPackMaintenanceRecordingBackend: &leafPackMaintenanceRecordingBackend{DB: mustOpenLeafPackBackend(t), leafGCResp: stats, leafGCErr: leafErr}}
	db, err := Open(t.TempDir(), backend, Options{AllowUnsafe: true, DisableWAL: true, JournalLanes: 1, IndexOuterLeavesInValueLog: true, ValueLogGenerationPolicy: uint8(backenddb.ValueLogGenerationHotWarmCold), ForceValueLogPointers: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	db.testSkipVlogCheckpointKick = true
	skipRetainedPrune(db)
	if err := db.Set([]byte("seed"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	forceVlogMaintenanceIdle(db)
	return db, backend
}

// The value-only eligibility estimate is empty. Leaf deletion must nevertheless
// run, publish progress, and consume the normal shared attempt interval.
func TestLeafMaintenanceProgressBeforeValueEligibility(t *testing.T) {
	db, backend := openLeafGCPeriodicTestDB(t, backenddb.LeafGenerationGCStats{GenerationsDeleted: 1, FilesDeleted: 2, BytesDeleted: 8192, ManifestRevisionsDeleted: 3, ManifestRevisionBytesDeleted: 1024}, nil)
	db.maybeRunVlogGenerationMaintenance(false)
	if _, calls := backend.recordedLeafGC(); calls != 1 {
		t.Fatalf("leaf calls=%d want1", calls)
	}
	if backend.estimateCalls != 1 || backend.valueCalls != 0 {
		t.Fatalf("value estimate/real=%d/%d", backend.estimateCalls, backend.valueCalls)
	}
	if db.vlogGenerationLastGCUnixNano.Load() == 0 || db.vlogGenerationLastGCNoopUnixNano.Load() != 0 {
		t.Fatal("leaf progress lost shared cadence or marked no-op")
	}
	stats := db.Stats()
	for key, want := range map[string]string{"runs": "1", "deleted_generations": "1", "deleted_files": "2", "deleted_bytes": "8192", "deleted_manifest_revisions": "3", "deleted_manifest_bytes": "1024"} {
		if got := stats["treedb.cache.vlog_generation.leaf_gc."+key]; got != want {
			t.Errorf("%s=%q want%q", key, got, want)
		}
	}
	forceVlogMaintenanceIdle(db)
	db.maybeRunVlogGenerationMaintenance(false)
	if _, calls := backend.recordedLeafGC(); calls != 1 {
		t.Fatalf("leaf scan ignored min interval: %d", calls)
	}
	// An old joint no-op expires normally; leaf-only progress clears it even
	// though the value collector continues to report zero eligible bytes.
	db.vlogGenerationLastGCUnixNano.Store(time.Now().Add(-2 * vlogGenerationGCMinInterval).UnixNano())
	db.vlogGenerationLastGCNoopUnixNano.Store(time.Now().Add(-2 * vlogGenerationGCNoopMinInterval).UnixNano())
	forceVlogMaintenanceIdle(db)
	db.maybeRunVlogGenerationMaintenance(false)
	if _, calls := backend.recordedLeafGC(); calls != 2 {
		t.Fatalf("leaf progress blocked by value-only no-op: %d", calls)
	}
	if db.vlogGenerationLastGCNoopUnixNano.Load() != 0 {
		t.Fatal("leaf progress failed to clear joint no-op")
	}
}

func TestLeafMaintenanceJointNoopCooldownAndError(t *testing.T) {
	t.Run("noop", func(t *testing.T) {
		db, backend := openLeafGCPeriodicTestDB(t, backenddb.LeafGenerationGCStats{}, nil)
		db.maybeRunVlogGenerationMaintenance(false)
		if db.vlogGenerationLastGCNoopUnixNano.Load() == 0 {
			t.Fatal("joint no-op lacks cooldown")
		}
		db.vlogGenerationLastGCUnixNano.Store(time.Now().Add(-2 * vlogGenerationGCMinInterval).UnixNano())
		forceVlogMaintenanceIdle(db)
		db.maybeRunVlogGenerationMaintenance(true)
		if _, calls := backend.recordedLeafGC(); calls != 1 {
			t.Fatalf("joint cooldown repeated leaf scan: %d", calls)
		}
	})
	t.Run("error", func(t *testing.T) {
		db, backend := openLeafGCPeriodicTestDB(t, backenddb.LeafGenerationGCStats{}, errors.New("leaf scan failure"))
		db.maybeRunVlogGenerationMaintenance(true)
		if backend.valueCalls != 0 {
			t.Fatal("value GC ran after leaf collector error")
		}
		if got := db.Stats()["treedb.cache.vlog_generation.leaf_gc.errors"]; got != "1" {
			t.Fatalf("errors=%q", got)
		}
		if db.vlogGenerationLastGCNoopUnixNano.Load() != 0 {
			t.Fatal("error treated as successful no-op")
		}
	})
}

// A small target is an explicit qualification fixture, not a default policy
// change. Fixed logical churn crosses real segment boundaries and ages meta
// slots, while the ordinary snapshot must remain readable through reclamation.
func TestLeafMaintenancePeriodicGCReclaimsSealedFilesAndManifests(t *testing.T) {
	prepareDirectSchedulerTest(t)
	t.Setenv(envDisableVlogGenerationRewrite, "1")
	t.Setenv(envEnableLeafGenerationPackMaintenance, "0")
	dir := t.TempDir()
	backendOpts := backenddb.Options{Dir: dir, Durability: backenddb.DurabilityWALOffRelaxed, DisableBackgroundPrune: true, IndexOuterLeavesInValueLog: true}
	cacheOpts := Options{AllowUnsafe: true, DisableWAL: true, RelaxedSync: true, MemtableMode: "cow_ordered", JournalLanes: 1, IndexOuterLeavesInValueLog: true, ValueLogGenerationPolicy: uint8(backenddb.ValueLogGenerationHotWarmCold), ValueLogGenerationLeafSegmentTargetBytes: 8 << 10}
	backend, err := backenddb.Open(backendOpts)
	if err != nil {
		t.Fatal(err)
	}
	db, err := Open(dir, backend, cacheOpts)
	if err != nil {
		_ = backend.Close()
		t.Fatal(err)
	}
	defer func() {
		if db != nil {
			_ = db.Close()
		}
	}()
	db.testSkipVlogCheckpointKick = true
	skipRetainedPrune(db)
	write := func(round int) {
		t.Helper()
		b := db.NewBatch()
		defer b.Close()
		for i := 0; i < 2048; i++ {
			if err := b.Set([]byte(fmt.Sprintf("leaf-%08d", i)), bytes.Repeat([]byte{byte(round + i%251)}, 128)); err != nil {
				t.Fatal(err)
			}
		}
		if err := b.Write(); err != nil {
			t.Fatal(err)
		}
		if err := db.Checkpoint(); err != nil {
			t.Fatal(err)
		}
	}
	maintain := func() {
		t.Helper()
		// Exercise scheduler admission without wall-clock sleeps; the production
		// cadence itself is tested by the joint-noop and min-interval controls.
		db.vlogGenerationLastGCUnixNano.Store(0)
		db.vlogGenerationLastGCNoopUnixNano.Store(0)
		forceVlogMaintenanceIdle(db)
		var passErr error
		db.maybeRunVlogGenerationMaintenanceWithOptions(true, vlogGenerationMaintenanceOptions{resultErr: &passErr})
		if passErr != nil {
			t.Fatal(passErr)
		}
	}
	write(0)
	old := db.AcquireSnapshot()
	if old == nil {
		t.Fatal("missing old snapshot")
	}
	defer old.Close()
	initial := make(map[string]os.FileInfo)
	entries, err := os.ReadDir(backenddb.LeafLogDirPath(dir))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".log" {
			info, err := entry.Info()
			if err != nil {
				t.Fatal(err)
			}
			initial[entry.Name()] = info
		}
	}
	if len(initial) < 2 {
		t.Fatalf("fixture requires crossed leaf target: files=%d", len(initial))
	}
	for round := 1; round <= 3; round++ {
		write(round)
		maintain()
	}
	got, err := old.Get([]byte("leaf-00000000"))
	if err != nil || !bytes.Equal(got, bytes.Repeat([]byte{0}, 128)) {
		t.Fatalf("pinned snapshot after GC: err=%v value=%x", err, got)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	for round := 4; round <= 6; round++ {
		write(round)
		maintain()
	}
	removed := 0
	for name := range initial {
		_, err := os.Stat(filepath.Join(backenddb.LeafLogDirPath(dir), name))
		if os.IsNotExist(err) {
			removed++
		} else if err != nil {
			t.Fatal(err)
		}
	}
	if removed == 0 {
		t.Fatal("no old sealed leaf file reclaimed after snapshot release and meta advancement")
	}
	if db.vlogGenerationLeafGCDeletedGenerations.Load() == 0 || db.vlogGenerationLeafGCDeletedBytes.Load() == 0 || db.vlogGenerationLeafGCDeletedManifestRevisions.Load() == 0 {
		t.Fatalf("missing actual segment/revision deletion counters: generations=%d bytes=%d revisions=%d", db.vlogGenerationLeafGCDeletedGenerations.Load(), db.vlogGenerationLeafGCDeletedBytes.Load(), db.vlogGenerationLeafGCDeletedManifestRevisions.Load())
	}
	t.Logf("small-target actual reclamation: old_files_removed=%d generations=%d bytes=%d manifest_revisions=%d manifest_bytes=%d", removed, db.vlogGenerationLeafGCDeletedGenerations.Load(), db.vlogGenerationLeafGCDeletedBytes.Load(), db.vlogGenerationLeafGCDeletedManifestRevisions.Load(), db.vlogGenerationLeafGCDeletedManifestBytes.Load())
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = nil
	backend, err = backenddb.Open(backendOpts)
	if err != nil {
		t.Fatal(err)
	}
	db, err = Open(dir, backend, cacheOpts)
	if err != nil {
		_ = backend.Close()
		t.Fatal(err)
	}
	for _, i := range []int{0, 1024, 2047} {
		got, err := db.Get([]byte(fmt.Sprintf("leaf-%08d", i)))
		if err != nil || !bytes.Equal(got, bytes.Repeat([]byte{byte(6 + i%251)}, 128)) {
			t.Fatalf("reopen key%d err=%v", i, err)
		}
	}
}
