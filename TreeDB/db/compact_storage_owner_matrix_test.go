package db

import (
	"context"
	"errors"
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
			name:           "command WAL replay inline hidden by lane wrapper",
			log:            wrapLeafPageLogWithLaneSelection(&leafPageLogWithRecordLengthHints{inner: replayInlineLeafPageLog{}}),
			lifecycle:      CompactStorageLifecycleExclusiveMaintenance,
			wantOwner:      CompactStorageLeafPageLogOwnerInternalHiddenByWrapper,
			wantStatus:     CompactStorageOwnerStatusBlockingBug,
			wantQuiescence: false,
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
			name:           "cached wrapper owner active writer",
			log:            compactStorageMatrixCachedLeafPageLog{},
			lifecycle:      CompactStorageLifecycleActiveWriter,
			wantOwner:      CompactStorageLeafPageLogOwnerCachedWrapper,
			wantStatus:     CompactStorageOwnerStatusLiveWriterFailClosed,
			wantQuiescence: true,
		},
		{
			name:           "cached wrapper owner quiesced maintenance",
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

func TestCompactStorageRawBackendCommandWALValueLogLeafPageLogCurrentBlockingBug(t *testing.T) {
	d, err := Open(Options{
		Dir:                        t.TempDir(),
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

	classification := compactStorageClassifyLeafPageLogOwner(d.leafPageLog, CompactStorageLifecycleExclusiveMaintenance)
	if classification.OwnerClass != CompactStorageLeafPageLogOwnerInternalHiddenByWrapper {
		t.Fatalf("owner=%q want %q (classification=%+v)", classification.OwnerClass, CompactStorageLeafPageLogOwnerInternalHiddenByWrapper, classification)
	}
	if classification.Status != CompactStorageOwnerStatusBlockingBug {
		t.Fatalf("status=%q want %q (classification=%+v)", classification.Status, CompactStorageOwnerStatusBlockingBug, classification)
	}

	_, err = d.CompactStorage(context.Background(), CompactStorageOptions{Mode: CompactStorageExhaustive})
	if !errors.Is(err, ErrCompactStorageLeafPageLogOwnerUnsupported) {
		t.Fatalf("CompactStorage exhaustive error=%v, want owner unsupported", err)
	}
	var ownerErr *CompactStorageLeafPageLogOwnerError
	if !errors.As(err, &ownerErr) {
		t.Fatalf("CompactStorage exhaustive error=%T, want CompactStorageLeafPageLogOwnerError", err)
	}
	if ownerErr.Classification.Status != CompactStorageOwnerStatusBlockingBug {
		t.Fatalf("error status=%q want %q", ownerErr.Classification.Status, CompactStorageOwnerStatusBlockingBug)
	}
}

func TestCompactStorageActiveWriterLifecycleIsModeledStatus(t *testing.T) {
	log := compactStorageMatrixCachedLeafPageLog{}
	active := compactStorageClassifyLeafPageLogOwner(log, CompactStorageLifecycleActiveWriter)
	if active.Status != CompactStorageOwnerStatusLiveWriterFailClosed {
		t.Fatalf("active status=%q want %q", active.Status, CompactStorageOwnerStatusLiveWriterFailClosed)
	}
	if !strings.Contains(active.Detail, "modeled active-writer status") {
		t.Fatalf("active detail=%q, want modeled-status wording", active.Detail)
	}
	exclusive := compactStorageClassifyLeafPageLogOwner(log, CompactStorageLifecycleExclusiveMaintenance)
	if exclusive.Status != CompactStorageOwnerStatusBlockingBug {
		t.Fatalf("exclusive status=%q want %q", exclusive.Status, CompactStorageOwnerStatusBlockingBug)
	}
	if strings.Contains(exclusive.Detail, "active-writer") {
		t.Fatalf("exclusive detail=%q should not imply active-writer runtime proof", exclusive.Detail)
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
			name:               "public command_wal_relaxed default profile value-log backed outer leaves",
			producerPath:       "treedb.Open(treedb.OptionsFor(ProfileCommandWALRelaxed)) cached wrapper; fixture-proved in TreeDB/compact_storage_test.go",
			owner:              CompactStorageLeafPageLogOwnerCachedWrapper,
			lifecycle:          CompactStorageLifecycleQuiescedMaintenance,
			status:             CompactStorageOwnerStatusBlockingBug,
			quiescence:         []string{"background-flush-apply-workers", "checkpoint-close-drain", "command-wal-cleanup", "cached-backlog"},
			missingFixtureWork: "supported-target requires #3049 to implement explicit cached owner handoff/unwrap; #3048 proves the canonical owner class only",
		},
		{
			name:               "public cached wrapper value-log backed outer leaves",
			producerPath:       "treedb.Open cached wrapper",
			owner:              CompactStorageLeafPageLogOwnerCachedWrapper,
			lifecycle:          CompactStorageLifecycleQuiescedMaintenance,
			status:             CompactStorageOwnerStatusBlockingBug,
			quiescence:         []string{"background-flush-apply-workers", "checkpoint-close-drain", "cached-backlog"},
			missingFixtureWork: "cached exhaustive compact needs explicit quiesced owner handoff or unwrap fixture before it can be marked supported-target",
		},
		{
			name:         "raw backend command WAL maintenance path",
			producerPath: "TreeDB/db.Open or TreeDB/cmd/treemap compact -rw -mode exhaustive over a raw command-WAL backend dir; not the public cached canonical collection path",
			owner:        CompactStorageLeafPageLogOwnerInternalHiddenByWrapper,
			lifecycle:    CompactStorageLifecycleExclusiveMaintenance,
			status:       CompactStorageOwnerStatusBlockingBug,
			quiescence:   []string{"checkpoint-close-drain", "command-wal-cleanup"},
		},
		{
			name:               "ordered-root value-log backed outer leaves",
			producerPath:       "ordered-root route matrix",
			owner:              CompactStorageLeafPageLogOwnerCachedWrapper,
			lifecycle:          CompactStorageLifecycleQuiescedMaintenance,
			status:             CompactStorageOwnerStatusBlockingBug,
			quiescence:         []string{"background-flush-apply-workers", "checkpoint-close-drain", "ordered-root-publishers", "cached-backlog"},
			missingFixtureWork: "ordered-root route fixtures are owned by sibling #3021/#3032; #3048 records expected compact owner classification only",
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
