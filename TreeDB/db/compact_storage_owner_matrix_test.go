package db

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

type compactStorageMatrixLeafPageLog struct{}

func (compactStorageMatrixLeafPageLog) AppendLeafPage([]byte) (page.LeafLogPtr, error) {
	return page.LeafLogPtr{}, nil
}

func (compactStorageMatrixLeafPageLog) Flush() error { return nil }

func (compactStorageMatrixLeafPageLog) Sync() error { return nil }

type compactStorageMatrixCachedLeafPageLog struct {
	compactStorageMatrixLeafPageLog
}

func (compactStorageMatrixCachedLeafPageLog) ConcurrentLeafPageAppends() bool { return true }

func (compactStorageMatrixCachedLeafPageLog) LeafPageLogLane(int) (LeafPageLog, bool) {
	return compactStorageMatrixLeafPageLog{}, true
}

func (compactStorageMatrixCachedLeafPageLog) CreatedLeafPageLogSegmentsSnapshot() ([]LeafPageLogSegment, error) {
	return nil, nil
}

func (compactStorageMatrixCachedLeafPageLog) CurrentLeafPageLogSegmentsSnapshot() ([]LeafPageLogSegment, error) {
	return nil, nil
}

func (compactStorageMatrixCachedLeafPageLog) CompactStorageCachedWrapperOwner() bool { return true }

type compactStorageMatrixExternalCachedShapeLeafPageLog struct {
	compactStorageMatrixLeafPageLog
}

func (compactStorageMatrixExternalCachedShapeLeafPageLog) ConcurrentLeafPageAppends() bool {
	return true
}

func (compactStorageMatrixExternalCachedShapeLeafPageLog) LeafPageLogLane(int) (LeafPageLog, bool) {
	return compactStorageMatrixLeafPageLog{}, true
}

func (compactStorageMatrixExternalCachedShapeLeafPageLog) CreatedLeafPageLogSegmentsSnapshot() ([]LeafPageLogSegment, error) {
	return nil, nil
}

func (compactStorageMatrixExternalCachedShapeLeafPageLog) CurrentLeafPageLogSegmentsSnapshot() ([]LeafPageLogSegment, error) {
	return nil, nil
}

type compactStorageMatrixCachedHandoffLeafPageLog struct {
	compactStorageMatrixCachedLeafPageLog
	advanced   uint32
	advanceErr error
}

func (l *compactStorageMatrixCachedHandoffLeafPageLog) AdvanceCompactStorageLeafPageLogSeqAtLeast(seq uint32) error {
	if l.advanceErr != nil {
		return l.advanceErr
	}
	l.advanced = seq
	return nil
}

func TestCompactStorageLeafPageLogOwnerMatrix(t *testing.T) {
	externalLeafLog := newRewriteWriter(ValueLogDirPath(t.TempDir()), 0, 0, 0)
	externalLeafLog.ConfigureLeafLog(LeafLogDirPath(t.TempDir()), rewriteLeafLogLaneID, 0)
	externalLeafLog.blockCompression = true
	externalLeafLog.ridAlloc = newRewriteRIDAllocator(1, nil)
	defer func() { _ = externalLeafLog.Close() }()

	tests := []struct {
		name            string
		log             LeafPageLog
		lifecycle       CompactStorageLifecycleState
		wantOwner       CompactStorageLeafPageLogOwnerClass
		wantStatus      CompactStorageOwnerStatus
		wantReplaceable bool
		wantQuiescence  bool
	}{
		{
			name:            "no installed leaf log",
			lifecycle:       CompactStorageLifecycleExclusiveMaintenance,
			wantOwner:       CompactStorageLeafPageLogOwnerNone,
			wantStatus:      CompactStorageOwnerStatusSupportedTarget,
			wantReplaceable: true,
		},
		{
			name:            "command WAL replay inline internal owner",
			log:             replayInlineLeafPageLog{},
			lifecycle:       CompactStorageLifecycleExclusiveMaintenance,
			wantOwner:       CompactStorageLeafPageLogOwnerCommandWALReplayInline,
			wantStatus:      CompactStorageOwnerStatusSupportedTarget,
			wantReplaceable: true,
		},
		{
			name:            "command WAL replay inline with record length wrapper",
			log:             &leafPageLogWithRecordLengthHints{inner: replayInlineLeafPageLog{}},
			lifecycle:       CompactStorageLifecycleQuiescedMaintenance,
			wantOwner:       CompactStorageLeafPageLogOwnerCommandWALReplayInline,
			wantStatus:      CompactStorageOwnerStatusSupportedTarget,
			wantReplaceable: true,
		},
		{
			name:            "command WAL replay inline hidden by lane wrapper",
			log:             wrapLeafPageLogWithLaneSelection(&leafPageLogWithRecordLengthHints{inner: replayInlineLeafPageLog{}}),
			lifecycle:       CompactStorageLifecycleExclusiveMaintenance,
			wantOwner:       CompactStorageLeafPageLogOwnerInternalHiddenByWrapper,
			wantStatus:      CompactStorageOwnerStatusSupportedTarget,
			wantReplaceable: true,
			wantQuiescence:  false,
		},
		{
			name:           "command WAL replay inline hidden by lane wrapper active writer",
			log:            wrapLeafPageLogWithLaneSelection(&leafPageLogWithRecordLengthHints{inner: replayInlineLeafPageLog{}}),
			lifecycle:      CompactStorageLifecycleActiveWriter,
			wantOwner:      CompactStorageLeafPageLogOwnerInternalHiddenByWrapper,
			wantStatus:     CompactStorageOwnerStatusLiveWriterFailClosed,
			wantQuiescence: true,
		},
		{
			name:       "standalone caller external owner with lane RID compression variants",
			log:        wrapLeafPageLogWithLaneSelection(&leafPageLogWithRecordLengthHints{inner: externalLeafLog}),
			lifecycle:  CompactStorageLifecycleExclusiveMaintenance,
			wantOwner:  CompactStorageLeafPageLogOwnerStandaloneCallerExternal,
			wantStatus: CompactStorageOwnerStatusExternalUnsupported,
		},
		{
			name:       "plain caller external owner",
			log:        compactStorageMatrixLeafPageLog{},
			lifecycle:  CompactStorageLifecycleQuiescedMaintenance,
			wantOwner:  CompactStorageLeafPageLogOwnerStandaloneCallerExternal,
			wantStatus: CompactStorageOwnerStatusExternalUnsupported,
		},
		{
			name:       "caller external owner with cached-like concurrency shape",
			log:        compactStorageMatrixExternalCachedShapeLeafPageLog{},
			lifecycle:  CompactStorageLifecycleQuiescedMaintenance,
			wantOwner:  CompactStorageLeafPageLogOwnerStandaloneCallerExternal,
			wantStatus: CompactStorageOwnerStatusExternalUnsupported,
		},
		{
			name:           "cached wrapper owner active writer",
			log:            &compactStorageMatrixCachedHandoffLeafPageLog{},
			lifecycle:      CompactStorageLifecycleActiveWriter,
			wantOwner:      CompactStorageLeafPageLogOwnerCachedWrapper,
			wantStatus:     CompactStorageOwnerStatusLiveWriterFailClosed,
			wantQuiescence: true,
		},
		{
			name:           "cached wrapper owner exclusive maintenance",
			log:            &compactStorageMatrixCachedHandoffLeafPageLog{},
			lifecycle:      CompactStorageLifecycleExclusiveMaintenance,
			wantOwner:      CompactStorageLeafPageLogOwnerCachedWrapper,
			wantStatus:     CompactStorageOwnerStatusLiveWriterFailClosed,
			wantQuiescence: true,
		},
		{
			name:           "cached wrapper owner quiesced maintenance",
			log:            &compactStorageMatrixCachedHandoffLeafPageLog{},
			lifecycle:      CompactStorageLifecycleQuiescedMaintenance,
			wantOwner:      CompactStorageLeafPageLogOwnerCachedWrapper,
			wantStatus:     CompactStorageOwnerStatusLiveWriterFailClosed,
			wantQuiescence: true,
		},
		{
			name:           "cached-shaped wrapper without handoff capability",
			log:            compactStorageMatrixCachedLeafPageLog{},
			lifecycle:      CompactStorageLifecycleQuiescedMaintenance,
			wantOwner:      CompactStorageLeafPageLogOwnerCachedWrapper,
			wantStatus:     CompactStorageOwnerStatusBlockingBug,
			wantQuiescence: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := compactStorageClassifyLeafPageLogOwner(tt.log, tt.lifecycle)
			if got.OwnerClass != tt.wantOwner {
				t.Fatalf("owner=%q want %q (classification=%+v)", got.OwnerClass, tt.wantOwner, got)
			}
			if got.Status != tt.wantStatus {
				t.Fatalf("status=%q want %q (classification=%+v)", got.Status, tt.wantStatus, got)
			}
			if got.Replaceable != tt.wantReplaceable {
				t.Fatalf("replaceable=%t want %t (classification=%+v)", got.Replaceable, tt.wantReplaceable, got)
			}
			if got.RequiresQuiescence != tt.wantQuiescence {
				t.Fatalf("requires quiescence=%t want %t (classification=%+v)", got.RequiresQuiescence, tt.wantQuiescence, got)
			}
		})
	}
}

func TestCompactStorageRawBackendCommandWALValueLogLeafPageLogCurrentSupported(t *testing.T) {
	requireLeafGenerationPackPromotionSupport(t)
	dir := t.TempDir()
	d, err := Open(Options{
		Dir:                        dir,
		CommandWAL:                 true,
		Durability:                 DurabilityWALOnRelaxed,
		DisableSideStores:          true,
		IndexOuterLeavesInValueLog: true,
	})
	if err != nil {
		t.Fatalf("Open command WAL value-log outer leaves: %v", err)
	}
	defer func() { _ = d.Close() }()
	if d.leafPageLog == nil {
		t.Fatal("expected command WAL open to install replay-inline leaf page log")
	}
	for i := 0; i < 64; i++ {
		if err := d.SetSync([]byte(fmt.Sprintf("k%04d", i)), bytes.Repeat([]byte{byte('a' + i%26)}, 256)); err != nil {
			t.Fatalf("SetSync(%d): %v", i, err)
		}
	}
	wantValue := bytes.Repeat([]byte("z"), 512)
	if err := d.SetSync([]byte("canonical"), wantValue); err != nil {
		t.Fatalf("SetSync canonical: %v", err)
	}

	classification := compactStorageClassifyLeafPageLogOwner(d.leafPageLog, CompactStorageLifecycleExclusiveMaintenance)
	if classification.OwnerClass != CompactStorageLeafPageLogOwnerInternalHiddenByWrapper {
		t.Fatalf("owner=%q want %q (classification=%+v)", classification.OwnerClass, CompactStorageLeafPageLogOwnerInternalHiddenByWrapper, classification)
	}
	if classification.Status != CompactStorageOwnerStatusSupportedTarget {
		t.Fatalf("status=%q want %q (classification=%+v)", classification.Status, CompactStorageOwnerStatusSupportedTarget, classification)
	}

	stats, err := d.CompactStorage(context.Background(), CompactStorageOptions{Mode: CompactStorageExhaustive})
	if err != nil {
		t.Fatalf("CompactStorage exhaustive: %v", err)
	}
	if !compactStoragePhaseSeen(stats.Phases, "seal-current-leaf-generation") {
		t.Fatalf("missing seal-current-leaf-generation phase: %+v", stats.Phases)
	}
	got, err := d.Get([]byte("canonical"))
	if err != nil {
		t.Fatalf("Get canonical after compact: %v", err)
	}
	if !bytes.Equal(got, wantValue) {
		t.Fatalf("canonical value mismatch after compact: got %q want %q", got, wantValue)
	}
	if err := d.SetSync([]byte("post-compact"), []byte("ok")); err != nil {
		t.Fatalf("post-compact SetSync through restored replay-inline owner: %v", err)
	}
	got, err = d.Get([]byte("post-compact"))
	if err != nil {
		t.Fatalf("Get post-compact: %v", err)
	}
	if string(got) != "ok" {
		t.Fatalf("post-compact value=%q want ok", got)
	}
	segments, err := listValueLogSegments(dir)
	if err != nil {
		t.Fatalf("list value-log segments after post-compact write: %v", err)
	}
	if _, err := scanValueLogSegments(segments, nil); err != nil {
		t.Fatalf("scan value-log segments after post-compact write: %v", err)
	}
}

func TestCompactStorageActiveWriterLifecycleIsModeledStatus(t *testing.T) {
	log := &compactStorageMatrixCachedHandoffLeafPageLog{}
	active := compactStorageClassifyLeafPageLogOwner(log, CompactStorageLifecycleActiveWriter)
	if active.Status != CompactStorageOwnerStatusLiveWriterFailClosed {
		t.Fatalf("active status=%q want %q", active.Status, CompactStorageOwnerStatusLiveWriterFailClosed)
	}
	if !strings.Contains(active.Detail, "modeled active-writer status") {
		t.Fatalf("active detail=%q, want modeled-status wording", active.Detail)
	}
	exclusive := compactStorageClassifyLeafPageLogOwner(log, CompactStorageLifecycleExclusiveMaintenance)
	if exclusive.Status != CompactStorageOwnerStatusLiveWriterFailClosed {
		t.Fatalf("exclusive status=%q want %q", exclusive.Status, CompactStorageOwnerStatusLiveWriterFailClosed)
	}
	if exclusive.Replaceable {
		t.Fatalf("exclusive replaceable=true, want false")
	}
	missingHandoff := compactStorageClassifyLeafPageLogOwner(compactStorageMatrixCachedLeafPageLog{}, CompactStorageLifecycleExclusiveMaintenance)
	if missingHandoff.Status != CompactStorageOwnerStatusBlockingBug {
		t.Fatalf("missing-handoff status=%q want %q", missingHandoff.Status, CompactStorageOwnerStatusBlockingBug)
	}
}

func TestCompactStorageDefaultProducedOwnerContractMatrix(t *testing.T) {
	tests := []struct {
		name               string
		producerPath       string
		owner              CompactStorageLeafPageLogOwnerClass
		lifecycle          CompactStorageLifecycleState
		status             CompactStorageOwnerStatus
		quiescence         []string
		missingFixtureWork string
	}{
		{
			name:         "backend raw no installed leaf log value-log backed outer leaves",
			producerPath: "TreeDB/db.Open with IndexOuterLeavesInValueLog and no installed leaf log",
			owner:        CompactStorageLeafPageLogOwnerNone,
			lifecycle:    CompactStorageLifecycleExclusiveMaintenance,
			status:       CompactStorageOwnerStatusSupportedTarget,
			quiescence:   []string{"checkpoint-close-drain"},
		},
		{
			name:         "public command_wal_relaxed default profile value-log backed outer leaves",
			producerPath: "treedb.Open(treedb.OptionsFor(ProfileCommandWALRelaxed)) cached wrapper; fixture-proved in TreeDB/compact_storage_test.go",
			owner:        CompactStorageLeafPageLogOwnerCachedWrapper,
			lifecycle:    CompactStorageLifecycleQuiescedMaintenance,
			status:       CompactStorageOwnerStatusLiveWriterFailClosed,
			quiescence:   []string{"background-flush-apply-workers", "checkpoint-close-drain", "command-wal-cleanup", "cached-backlog"},
		},
		{
			name:         "public cached wrapper value-log backed outer leaves",
			producerPath: "treedb.Open cached wrapper",
			owner:        CompactStorageLeafPageLogOwnerCachedWrapper,
			lifecycle:    CompactStorageLifecycleQuiescedMaintenance,
			status:       CompactStorageOwnerStatusLiveWriterFailClosed,
			quiescence:   []string{"background-flush-apply-workers", "checkpoint-close-drain", "cached-backlog"},
		},
		{
			name:         "raw backend command WAL maintenance path",
			producerPath: "TreeDB/db.Open or TreeDB/cmd/treemap compact -rw -mode exhaustive over a raw command-WAL backend dir; not the public cached canonical collection path",
			owner:        CompactStorageLeafPageLogOwnerInternalHiddenByWrapper,
			lifecycle:    CompactStorageLifecycleExclusiveMaintenance,
			status:       CompactStorageOwnerStatusSupportedTarget,
			quiescence:   []string{"checkpoint-close-drain", "command-wal-cleanup"},
		},
		{
			name:         "ordered-root value-log backed outer leaves",
			producerPath: "ordered-root route matrix",
			owner:        CompactStorageLeafPageLogOwnerCachedWrapper,
			lifecycle:    CompactStorageLifecycleQuiescedMaintenance,
			status:       CompactStorageOwnerStatusLiveWriterFailClosed,
			quiescence:   []string{"background-flush-apply-workers", "checkpoint-close-drain", "ordered-root-publishers", "cached-backlog"},
		},
	}

	coveredQuiescence := make(map[string]bool)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.producerPath == "" {
				t.Fatal("producer path must be recorded")
			}
			if tt.owner == "" || tt.lifecycle == "" || tt.status == "" {
				t.Fatalf("incomplete contract row: %+v", tt)
			}
			if tt.status == CompactStorageOwnerStatusBlockingBug && tt.missingFixtureWork == "" &&
				tt.owner != CompactStorageLeafPageLogOwnerInternalHiddenByWrapper {
				t.Fatalf("blocking bug row without fixture/design work: %+v", tt)
			}
			for _, dimension := range tt.quiescence {
				coveredQuiescence[dimension] = true
			}
		})
	}
	for _, required := range []string{
		"background-flush-apply-workers",
		"checkpoint-close-drain",
		"ordered-root-publishers",
		"cached-backlog",
		"command-wal-cleanup",
	} {
		if !coveredQuiescence[required] {
			t.Fatalf("missing quiescence dimension %q", required)
		}
	}
}
