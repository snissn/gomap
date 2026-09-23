package db

import (
	"math"
	"testing"

	"github.com/snissn/gomap/TreeDB/freelist"
)

func TestPreparedRootPublicationBaseProfileCountsHistoricalManifest(t *testing.T) {
	database := &DB{
		leafGenerationManifest: &leafGenerationManifest{Generations: []leafGenerationRecord{
			{State: leafGenerationStateDeleted, FileIDs: []uint32{1, 2}},
			{State: leafGenerationStateRetiring, FileIDs: []uint32{3}},
			{State: leafGenerationStateWritable, FileIDs: []uint32{4, 5, 6}},
		}},
		leafGenerationPendingFileIDs: []uint32{7, 8},
		rootPublication: &rootPublicationRuntimeV1{
			visibleMembers: map[uint64]*rootPublicationVisibleMemberV1{1: {}, 2: {}},
			debt:           make([]*freelist.PreparedCOWCandidateV1, 3),
			seals: []*rootPublicationSealV1{
				{prefix: make([]*freelist.PreparedCOWCandidateV1, 2)},
				{prefix: make([]*freelist.PreparedCOWCandidateV1, 3)},
			},
		},
	}
	profile := database.PreparedRootPublicationBaseProfile()
	if profile.LeafGenerations != 3 || profile.LeafGenerationFileIDs != 6 || profile.PendingLeafFileIDs != 2 {
		t.Fatalf("base profile=%+v, want all historical generations and pending IDs", profile)
	}
	if profile.VisibleMembers != 2 || profile.AllocatorDebt != 3 || profile.Seals != 2 || profile.SealPrefixEntries != 5 {
		t.Fatalf("base profile=%+v, want queued visible members, debt, seals, and prefixes", profile)
	}
	if got := preparedProfileAdd(math.MaxUint64-1, 2); got != math.MaxUint64 {
		t.Fatalf("saturated sum=%d", got)
	}
}

func TestPreparedRootPublicationVisibleResourceCloneChecksLimitBeforeCopy(t *testing.T) {
	resources, _ := dbSelectorFixture(t)
	defer resources.Release()
	runtime := &rootPublicationRuntimeV1{visibleResources: resources}
	cloned, _, err := runtime.cloneVisibleResourcesWithWorkMax(0)
	if err == nil || cloned != nil {
		t.Fatalf("oversized visible-resource clone=(%v, %v), want rejection", cloned, err)
	}
	cloned, _, err = runtime.cloneVisibleResourcesWithWorkMax(1)
	if err != nil {
		t.Fatal(err)
	}
	if cloned.Len() != 1 {
		t.Fatalf("bounded clone has %d resources, want 1", cloned.Len())
	}
	cloned.Release()
}

func TestPreparedRootPublicationPendingLeafSnapshotChecksGrowthBeforeCopy(t *testing.T) {
	database := &DB{
		leafGenerationManifest:       newLeafGenerationManifest(1),
		leafGenerationPendingFileIDs: []uint32{1, 2},
		leafGenerationPendingSet:     map[uint32]struct{}{1: {}, 2: {}},
	}
	if pending, err := database.snapshotLeafGenerationPendingFileIDsWithLimit(0, 1); pending != nil || err == nil {
		t.Fatalf("oversized pending snapshot=(%v, %v), want rejection before copy", pending, err)
	}
	if pending, err := database.snapshotLeafGenerationPendingFileIDsWithLimit(0, 2); err != nil || len(pending) != 2 {
		t.Fatalf("bounded pending snapshot=(%v, %v), want two IDs", pending, err)
	}
	if pending, err := database.snapshotLeafGenerationPendingFileIDsWithLimit(3, 2); pending != nil || err == nil {
		t.Fatalf("new current file exceeded limit: (%v, %v)", pending, err)
	}
	if pending, err := database.snapshotLeafGenerationPendingFileIDsWithLimit(2, 2); err != nil || len(pending) != 2 {
		t.Fatalf("existing current file was counted twice: (%v, %v)", pending, err)
	}
}

func TestPreparedRootPublicationLeafManifestChecksHistoricalBaseBeforeClone(t *testing.T) {
	base := &leafGenerationManifest{Generations: []leafGenerationRecord{
		{GenerationID: 1, FileIDs: []uint32{1, 2}},
		{GenerationID: 2, FileIDs: []uint32{3}},
	}}
	database := &DB{
		leafGenerationPendingFileIDs: []uint32{4},
		leafGenerationPendingSet:     map[uint32]struct{}{4: {}},
	}
	limits := &PreparedRootPublicationLimits{
		MaxPendingLeafFileIDs: 1, MaxLeafGenerations: 1, MaxLeafGenerationFileIDs: 3,
	}
	result, err := database.stagedLeafGenerationManifestWithPendingResultAndLimit(base, 0, 2, limits)
	if err == nil || result.manifest != base || result.changed {
		t.Fatalf("generation limit result=%+v err=%v, want pre-clone rejection", result, err)
	}
	limits.MaxLeafGenerations = 2
	limits.MaxLeafGenerationFileIDs = 2
	result, err = database.stagedLeafGenerationManifestWithPendingResultAndLimit(base, 0, 2, limits)
	if err == nil || result.manifest != base || result.changed {
		t.Fatalf("file-ID limit result=%+v err=%v, want pre-clone rejection", result, err)
	}
}

func TestPreparedRootPublicationVisibleInstallRejectsManagerGrowthBeforePin(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(Options{
		Dir:                    dir,
		DisableBackgroundPrune: true,
		ValueLog:               ValueLogOptions{PointerThreshold: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	appendPointersInNewSegment(t, dir, 0, 1, 30_000, 1, func(int) []byte {
		return []byte("registered value-log payload")
	})
	if err := database.RefreshValueLogSet(); err != nil {
		t.Fatal(err)
	}
	registered := database.valueLogManager.RegisteredFileCountNoRefresh()
	if registered == 0 {
		t.Fatal("expected registered value-log file")
	}
	baseCommit := database.currentCommitSeq()
	limits := &PreparedRootPublicationLimits{MaxRegisteredValueLogFiles: registered - 1}
	_, err = database.scanCandidateValueLogReferencesWithCountsAndLimitsV1(
		database.idx.Load(), database.meta, false, nil, limits,
	)
	if err == nil {
		t.Fatal("candidate scan accepted a Manager set above its prepared limit")
	}
	_, err = database.prepareRootPublicationVisibleInstallV1(
		database.idx.Load(), database.meta, database.meta.UserRootPageID,
		database.meta.UserRootPageID, true, finalizeCommitPost{}, nil, nil,
		finalizeCommitOptions{preparedLimits: limits},
	)
	if err == nil {
		t.Fatal("visible install accepted a Manager set above its prepared limit")
	}
	if got := database.currentCommitSeq(); got != baseCommit {
		t.Fatalf("failed visible install advanced commit sequence: %d, want %d", got, baseCommit)
	}
	if got := database.valueLogManager.RegisteredFileCountNoRefresh(); got != registered {
		t.Fatalf("failed visible install changed registered files: %d, want %d", got, registered)
	}
}
