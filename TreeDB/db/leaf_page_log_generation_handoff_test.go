package db

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/commandwalbarrier"
	"github.com/snissn/gomap/TreeDB/page"
)

type handoffTestProducer struct {
	mu                      sync.Mutex
	begin, advance, release int
	onAdvance               func() error
	onRetirement            func([]LeafPageLogSegment)
	retired                 []LeafPageLogSegment
}

func (p *handoffTestProducer) AppendLeafPage([]byte) (page.LeafLogPtr, error) {
	return page.LeafLogPtr{}, errors.New("unused test append")
}
func (p *handoffTestProducer) Flush() error                                   { return nil }
func (p *handoffTestProducer) Sync() error                                    { return nil }
func (p *handoffTestProducer) LeafPageLogLane(worker int) (LeafPageLog, bool) { return p, worker == 0 }
func (p *handoffTestProducer) BeginLeafPageLogGenerationHandoff(context.Context) (LeafPageLogGenerationHandoff, error) {
	p.mu.Lock()
	p.begin++
	return &handoffTestOwner{producer: p}, nil
}

type handoffTestOwner struct{ producer *handoffTestProducer }

func (o *handoffTestOwner) AdvanceLeafPageLogGeneration(context.Context) error {
	p := o.producer
	p.advance++
	if p.onAdvance != nil {
		return p.onAdvance()
	}
	return nil
}
func (o *handoffTestOwner) Release() {
	if o.producer == nil {
		return
	}
	p := o.producer
	o.producer = nil
	p.release++
	p.mu.Unlock()
}

func assertHandoffBackendGatesReleased(t *testing.T, db *DB) {
	t.Helper()
	if !db.teardownMu.TryLock() {
		t.Fatal("drain retained teardown lease")
	}
	db.teardownMu.Unlock()
	if !db.commandWALRawAdmissionMu.TryLock() {
		t.Fatal("drain retained admission lease")
	}
	db.commandWALRawAdmissionMu.Unlock()
	if !db.commandWALRawPublishMu.TryLock() {
		t.Fatal("drain retained raw lease")
	}
	db.commandWALRawPublishMu.Unlock()
	if !db.writeMu.TryLock() {
		t.Fatal("drain retained backend write lock")
	}
	db.writeMu.Unlock()
}

func TestLeafPageLogGenerationHandoffDrainReleasesBothOwnersAndRebinds(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	defer db.Close()
	db.indexOuterLeavesInValueLog = true
	first := &handoffTestProducer{}
	replacement := &handoffTestProducer{}
	db.SetLeafPageLog(first)
	defer db.SetLeafPageLog(nil)
	calls := 0
	unregister := db.RegisterCommandWALRawPublishBarrier(func() error {
		calls++
		if calls != 1 {
			return nil
		}
		return &commandwalbarrier.PendingDrain{Drain: func() error {
			if first.release != 1 || !first.mu.TryLock() {
				t.Fatal("drain retained cached producer owner")
			}
			first.mu.Unlock()
			assertHandoffBackendGatesReleased(t, db)
			// A replacement is installed while both publication owners are released.
			db.SetLeafPageLog(replacement)
			return nil
		}}
	})
	defer unregister()
	replacement.onAdvance = func() error {
		if db.writeMu.TryLock() {
			db.writeMu.Unlock()
			return errors.New("handoff missing backend builder gate")
		}
		if db.commandWALRawPublishMu.TryLock() {
			db.commandWALRawPublishMu.Unlock()
			return errors.New("handoff missing raw gate")
		}
		if db.commandWALRawAdmissionMu.TryRLock() {
			db.commandWALRawAdmissionMu.RUnlock()
			return errors.New("handoff missing exclusive admission")
		}
		if !db.maintenanceMu.TryLock() {
			return errors.New("handoff entered under maintenanceMu")
		}
		db.maintenanceMu.Unlock()
		return nil
	}
	if err := db.AdvanceLeafPageLogGeneration(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls != 2 || first.begin != 1 || first.advance != 0 || first.release != 1 || replacement.begin != 1 || replacement.advance != 1 || replacement.release != 1 {
		t.Fatalf("unexpected owner phases: barriers=%d first=%+v replacement=%+v", calls, first, replacement)
	}
	assertHandoffBackendGatesReleased(t, db)
}

func TestLeafPageLogGenerationHandoffErrorsAbortMaintenance(t *testing.T) {
	for _, caller := range []string{"pack", "pack-from-plan", "pack-run-once", "gc"} {
		t.Run(caller, func(t *testing.T) {
			db, _, _ := openLeafGenerationPackTestDB(t)
			want := errors.New("partial producer rotation")
			producer := &handoffTestProducer{onAdvance: func() error { return want }}
			db.SetLeafPageLog(producer)
			defer db.SetLeafPageLog(nil)
			var err error
			switch caller {
			case "pack":
				_, err = db.LeafGenerationPack(context.Background(), LeafGenerationPackOptions{})
			case "pack-from-plan":
				_, err = db.LeafGenerationPackFromPlan(context.Background(), LeafGenerationPackFromPlanOptions{})
			case "pack-run-once":
				_, err = db.LeafGenerationPackRunOnce(context.Background(), LeafGenerationPackFromPlanOptions{})
			case "gc":
				_, err = db.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{})
			}
			if !errors.Is(err, want) {
				t.Fatalf("maintenance error=%v want %v", err, want)
			}
			if producer.begin != 1 || producer.advance != 1 || producer.release != 1 {
				t.Fatalf("owner phases=%+v", producer)
			}
			if !producer.mu.TryLock() {
				t.Fatal("error retained producer owner")
			}
			producer.mu.Unlock()
			assertHandoffBackendGatesReleased(t, db)
		})
	}
}

func (p *handoffTestProducer) LeafPageLogSegmentsRetired(segments []LeafPageLogSegment) {
	p.retired = append(p.retired, segments...)
	if p.onRetirement != nil {
		p.onRetirement(segments)
	}
}

func TestLeafPageLogRetirementReportsOnlyPhysicallyMissingFiles(t *testing.T) {
	db, _, dir := openLeafGenerationPackTestDB(t)
	producer := &handoffTestProducer{}
	db.SetLeafPageLog(producer)
	defer db.SetLeafPageLog(nil)
	path := filepath.Join(dir, "physically-present-leaf")
	if err := os.WriteFile(path, []byte("still pinned"), 0600); err != nil {
		t.Fatal(err)
	}
	manifest := &leafGenerationManifest{Generations: []leafGenerationRecord{{GenerationID: 1, State: leafGenerationStateDeleted, FileIDs: []uint32{1}}}}
	paths := map[uint32]string{1: path}
	if _, pruned, _, retired, err := db.pruneDeletedLeafGenerationRecords(manifest, paths); err != nil || pruned || len(retired) != 0 {
		t.Fatalf("present file pruned=%t err=%v", pruned, err)
	}
	if len(producer.retired) != 0 {
		t.Fatal("retirement reported before physical removal")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	next, pruned, count, retired, err := db.pruneDeletedLeafGenerationRecords(manifest, paths)
	if err != nil || !pruned || count != 1 || len(next.Generations) != 0 {
		t.Fatalf("missing file prune count=%d pruned=%t err=%v", count, pruned, err)
	}
	if len(retired) != 1 || retired[0].Path != path || retired[0].FileID != 1 {
		t.Fatalf("retirement receipt=%v", retired)
	}
	if len(producer.retired) != 0 {
		t.Fatal("pruning invoked observer under builder gate")
	}
}

func TestLeafPageLogRetirementApplyUnlocksBuilderBeforeObserver(t *testing.T) {
	db, _, dir := openLeafGenerationPackTestDB(t)
	const rawFileID uint32 = 255<<23 | 123456
	manifest := &leafGenerationManifest{
		Version: leafGenerationManifestVersion, CurrentGenerationID: 2, NextGenerationID: 3,
		Generations: []leafGenerationRecord{
			{GenerationID: 1, State: leafGenerationStateDeleted, FileIDs: []uint32{rawFileID}, CreatedCommitSeq: 1, DeletedCommitSeq: 2, PublishedCommitSeq: 2},
			{GenerationID: 2, State: leafGenerationStateWritable, CreatedCommitSeq: 3, PublishedCommitSeq: 3},
		},
	}
	if err := saveLeafGenerationManifest(LeafLogDirPath(dir), manifest); err != nil {
		t.Fatal(err)
	}
	db.mu.Lock()
	db.leafGenerationManifest = manifest
	db.mu.Unlock()
	if err := db.publishLeafGenerationState(true); err != nil {
		t.Fatal(err)
	}
	producer := &handoffTestProducer{}
	producer.onRetirement = func(segments []LeafPageLogSegment) {
		if !db.writeMu.TryLock() {
			t.Error("observer inherited backend builder gate")
		} else {
			db.writeMu.Unlock()
		}
		if db.teardownMu.TryLock() {
			db.teardownMu.Unlock()
			t.Error("observer missing apply teardown lease")
		}
		if !db.commandWALRawAdmissionMu.TryLock() {
			t.Error("observer inherited admission gate")
		} else {
			db.commandWALRawAdmissionMu.Unlock()
		}
		if !db.commandWALRawPublishMu.TryLock() {
			t.Error("observer inherited raw publication gate")
		} else {
			db.commandWALRawPublishMu.Unlock()
		}
		if len(segments) != 1 || segments[0].FileID != rawFileID || segments[0].Path != leafGenerationFallbackPath(dir, rawFileID) {
			t.Errorf("receipt=%v", segments)
		}
	}
	db.SetLeafPageLog(producer)
	defer db.SetLeafPageLog(nil)
	if _, err := db.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{}); err != nil {
		t.Fatal(err)
	}
	if len(producer.retired) != 1 {
		t.Fatalf("retirement receipts=%v", producer.retired)
	}
	assertHandoffBackendGatesReleased(t, db)
}

func TestLeafPageLogRetirementSurvivesSidecarRemovalError(t *testing.T) {
	db, _, dir := openLeafGenerationPackTestDB(t)
	const rawFileID uint32 = 255<<23 | 123456
	absent := leafGenerationFallbackPath(dir, rawFileID)
	indexPath := leafGenerationRecordLengthIndexPath(dir, rawFileID)
	if err := os.MkdirAll(indexPath, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(indexPath, "block-removal"), []byte("sidecar failure"), 0600); err != nil {
		t.Fatal(err)
	}
	manifest := &leafGenerationManifest{Generations: []leafGenerationRecord{{GenerationID: 1, State: leafGenerationStateDeleted, FileIDs: []uint32{rawFileID}}}}
	_, pruned, _, retired, err := db.pruneDeletedLeafGenerationRecords(manifest, map[uint32]string{rawFileID: absent})
	if err == nil || pruned {
		t.Fatalf("sidecar error=%v pruned=%t", err, pruned)
	}
	if len(retired) != 1 || retired[0].FileID != rawFileID || retired[0].Path != absent {
		t.Fatalf("lost physical absence receipt: %v", retired)
	}
}

func TestLeafPageLogGenerationHandoffMaintenanceRefusesBeforeProducerEffect(t *testing.T) {
	cases := []struct {
		name   string
		limits LeafGenerationMaintenanceLimits
		reason string
	}{
		{"invalid", LeafGenerationMaintenanceLimits{NativeEntries: 1}, "all footprint limits must be positive"},
		{"entries", LeafGenerationMaintenanceLimits{NativeEntries: 1, NativeBytes: 1 << 30, PagerPages: 1 << 20}, "directory entries"},
		{"bytes", LeafGenerationMaintenanceLimits{NativeEntries: 1 << 16, NativeBytes: 1, PagerPages: 1 << 20}, "native bytes"},
		{"pager", LeafGenerationMaintenanceLimits{NativeEntries: 1 << 16, NativeBytes: 1 << 30, PagerPages: 1}, "captured pager pages"},
	}
	for _, caller := range []string{"pack", "pack-from-plan", "pack-run-once", "gc"} {
		for _, tc := range cases {
			t.Run(caller+"/"+tc.name, func(t *testing.T) {
				db, _, _ := openLeafGenerationPackTestDB(t)
				if tc.name != "invalid" {
					if err := tc.limits.validate(); err != nil {
						t.Fatalf("positive limit fixture invalid: %v", err)
					}
				}
				if tc.name == "pager" && db.idx.Load().pager.PageCount() <= tc.limits.PagerPages {
					t.Fatal("fixture does not exceed positive pager allowance")
				}
				producer := &handoffTestProducer{}
				db.SetLeafPageLog(producer)
				defer db.SetLeafPageLog(nil)
				var err error
				switch caller {
				case "pack":
					_, err = db.LeafGenerationPack(context.Background(), LeafGenerationPackOptions{MaintenanceLimits: tc.limits})
				case "pack-from-plan":
					_, err = db.LeafGenerationPackFromPlan(context.Background(), LeafGenerationPackFromPlanOptions{MaintenanceLimits: tc.limits})
				case "pack-run-once":
					_, err = db.LeafGenerationPackRunOnce(context.Background(), LeafGenerationPackFromPlanOptions{MaintenanceLimits: tc.limits})
				case "gc":
					_, err = db.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{MaintenanceLimits: tc.limits})
				}
				if !errors.Is(err, ErrLeafGenerationMaintenanceLimit) || !strings.Contains(err.Error(), tc.reason) {
					t.Fatalf("admission error=%v want %s", err, tc.reason)
				}
				if producer.begin != 0 || producer.advance != 0 || producer.release != 0 {
					t.Fatalf("refused input entered producer: %+v", producer)
				}
			})
		}
	}

	db, _, _ := openLeafGenerationPackTestDB(t)
	producer := &handoffTestProducer{}
	db.SetLeafPageLog(producer)
	defer db.SetLeafPageLog(nil)
	if _, err := db.LeafGenerationPack(context.Background(), LeafGenerationPackOptions{GenerationIDs: []uint64{999999}, Force: true}); err == nil {
		t.Fatal("missing generation accepted")
	}
	if producer.begin != 0 {
		t.Fatal("invalid explicit generation entered producer")
	}
}
