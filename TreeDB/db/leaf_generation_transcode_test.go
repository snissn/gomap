package db

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestLeafGenerationTranscodeRunOnce_UsesPreparedPlan(t *testing.T) {
	db, leafLog, dir := openLeafGenerationPackTestDB(t)

	writeLeafGenerationKeys(t, db, "k", 2048, 'a')
	path1, fileID1 := currentLeafSegmentOrFatal(t, leafLog)
	rawFileID1 := page.ValueLogSegmentID(fileID1)
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeyRange(t, db, "k", 0, 1024, 'b')
	writeLeafGenerationKeys(t, db, "z", 32, 'z')

	manifestBefore := loadLeafGenerationManifestOrFatal(t, dir)
	gen1 := findLeafGenerationByFileID(t, manifestBefore, rawFileID1)
	info, err := os.Stat(path1)
	if err != nil {
		t.Fatalf("Stat(%q): %v", path1, err)
	}
	segmentBytes := info.Size()

	prev := leafGenerationTranscodePlanPreparedHook
	leafGenerationTranscodePlanPreparedHook = func(_ *DB, _ context.Context, _ LeafGenerationTranscodeOptions) (LeafGenerationTranscodePlan, leafGenerationTranscodePreparedDict, error) {
		return LeafGenerationTranscodePlan{
			Admission:           leafGenerationTranscodePlanAdmissionEligible,
			CurrentCommitSeq:    1,
			CurrentGenerationID: manifestBefore.CurrentGenerationID,
			Candidates: []LeafGenerationTranscodePlanGeneration{
				{
					GenerationID:                  gen1.GenerationID,
					State:                         leafGenerationStateSealed,
					FileIDs:                       append([]uint32(nil), gen1.FileIDs...),
					FileCount:                     len(gen1.FileIDs),
					BytesTotal:                    segmentBytes,
					BytesLive:                     segmentBytes,
					BytesToCopy:                   segmentBytes,
					LivePages:                     1,
					SamplePages:                   1,
					EstimatedBytesAfter:           segmentBytes - 256,
					ExpectedBytesSaved:            256,
					ExpectedSavedPerByteCopiedPPM: ratioPPM(256, segmentBytes),
				},
			},
			CandidateGenerationIDs:             []uint64{gen1.GenerationID},
			CandidateBytesTotal:                segmentBytes,
			CandidateBytesLive:                 segmentBytes,
			CandidateBytesToCopy:               segmentBytes,
			CandidateLivePages:                 1,
			CandidateSamplePages:               1,
			CandidateEstimatedBytesAfter:       segmentBytes - 256,
			CandidateBytesSaved:                256,
			ExpectedBytesSaved:                 256,
			ExpectedBytesSavedRatioPPM:         ratioPPM(256, segmentBytes),
			ExpectedBytesSavedPerByteCopiedPPM: ratioPPM(256, segmentBytes),
		}, leafGenerationTranscodePreparedDict{}, nil
	}
	defer func() {
		leafGenerationTranscodePlanPreparedHook = prev
	}()

	stats, err := db.LeafGenerationTranscodeRunOnce(context.Background(), LeafGenerationTranscodeOptions{
		MaxGenerations: 1,
		Sync:           true,
	})
	if err != nil {
		t.Fatalf("LeafGenerationTranscodeRunOnce: %v", err)
	}
	if !stats.Ran {
		t.Fatalf("expected transcode run once to execute, skip_reason=%q", stats.SkipReason)
	}
	if got, want := stats.Selection.GenerationIDs, []uint64{gen1.GenerationID}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("Selection.GenerationIDs=%v, want %v", got, want)
	}
	expectLeafGenerationValue(t, db, leafGenerationKey("k", 0), 'b')
	expectLeafGenerationValue(t, db, leafGenerationKey("k", 1), 'b')
	expectLeafGenerationValue(t, db, leafGenerationKey("k", 1024), 'a')
	if _, err := db.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{}); err != nil {
		t.Fatalf("LeafGenerationGC after transcode run once: %v", err)
	}
	if err := waitForPathRemoval(path1, 5*time.Second); err != nil {
		t.Fatalf("waitForPathRemoval(%s): %v", path1, err)
	}
}

func TestLeafGenerationTranscodeRunOnce_SkipsIneligiblePlan(t *testing.T) {
	prev := leafGenerationTranscodePlanPreparedHook
	leafGenerationTranscodePlanPreparedHook = func(_ *DB, _ context.Context, _ LeafGenerationTranscodeOptions) (LeafGenerationTranscodePlan, leafGenerationTranscodePreparedDict, error) {
		return LeafGenerationTranscodePlan{
			Admission: leafGenerationTranscodePlanAdmissionSavedPerCopyTooLow,
		}, leafGenerationTranscodePreparedDict{}, nil
	}
	defer func() {
		leafGenerationTranscodePlanPreparedHook = prev
	}()

	db, _, _ := openLeafGenerationPackTestDB(t)
	stats, err := db.LeafGenerationTranscodeRunOnce(context.Background(), LeafGenerationTranscodeOptions{})
	if err != nil {
		t.Fatalf("LeafGenerationTranscodeRunOnce: %v", err)
	}
	if stats.Ran {
		t.Fatalf("expected ineligible plan to skip, got pack=%+v", stats.Pack)
	}
	if got, want := stats.SkipReason, "plan_admission:saved_per_copy_too_low"; got != want {
		t.Fatalf("SkipReason=%q, want %q", got, want)
	}
}

func TestLeafGenerationTranscodeEligibility_AllowsCurrentWritableWhenRequested(t *testing.T) {
	gen := LeafGenerationPlanGeneration{
		GenerationID: 7,
		State:        leafGenerationStateWritable,
		BytesLive:    1024,
		LivePages:    4,
	}
	if ok, skip := leafGenerationTranscodeEligibility(gen, 7, LeafGenerationTranscodeOptions{}); ok || skip != leafGenerationPlanSkipWritableGeneration {
		t.Fatalf("default writable eligibility=(%t,%q), want false/%q", ok, skip, leafGenerationPlanSkipWritableGeneration)
	}
	if ok, skip := leafGenerationTranscodeEligibility(gen, 7, LeafGenerationTranscodeOptions{IncludeWritableCurrent: true}); !ok || skip != "" {
		t.Fatalf("include-current eligibility=(%t,%q), want true/empty", ok, skip)
	}
	if ok, skip := leafGenerationTranscodeEligibility(gen, 8, LeafGenerationTranscodeOptions{IncludeWritableCurrent: true}); ok || skip != leafGenerationPlanSkipWritableGeneration {
		t.Fatalf("non-current writable eligibility=(%t,%q), want false/%q", ok, skip, leafGenerationPlanSkipWritableGeneration)
	}
}

func TestPrepareLeafGenerationTranscodeDict_FreshSingleIgnoresCurrentDict(t *testing.T) {
	db, _, _ := openLeafGenerationPackTestDB(t)

	writeLeafGenerationKeys(t, db, "k", 2048, 'a')
	snap := db.AcquireSnapshot()
	if snap == nil || snap.state == nil {
		t.Fatal("snapshot unavailable")
	}
	defer func() { _ = snap.Close() }()

	var setCurrentCalled bool
	db.valueLogDictCurrentForClass = func(context.Context, string) (uint64, error) {
		return 77, nil
	}
	db.valueLogDictLookup = func(dictID uint64) ([]byte, error) {
		if dictID == 77 {
			return []byte("legacy-dict"), nil
		}
		return nil, nil
	}
	db.valueLogDictPut = func(context.Context, []byte) (uint64, error) {
		return 88, nil
	}
	db.valueLogDictSetCurrentForClass = func(context.Context, string, uint64) error {
		setCurrentCalled = true
		return nil
	}
	db.valueLogDictSetLeafPayloadMode = func(context.Context, uint64, bool) error { return nil }
	prevTrain := leafGenerationTranscodeTrainFreshDictHook
	leafGenerationTranscodeTrainFreshDictHook = func(*DB, *DBState) ([]byte, error) {
		return []byte("fresh-dict"), nil
	}
	defer func() {
		leafGenerationTranscodeTrainFreshDictHook = prevTrain
	}()

	gotID, gotDict, useRawPages, err := prepareLeafGenerationTranscodeDict(db, snap.state, leafGenerationTranscodeDictStrategyFreshSingle)
	if err != nil {
		t.Fatalf("prepareLeafGenerationTranscodeDict: %v", err)
	}
	if gotID != 88 {
		t.Fatalf("dictID=%d, want 88", gotID)
	}
	if len(gotDict) == 0 {
		t.Fatal("expected fresh dict bytes")
	}
	if useRawPages {
		t.Fatal("fresh compact leaf dict should not use raw pages")
	}
	if !setCurrentCalled {
		t.Fatal("expected fresh dict to publish current outer_leaf class")
	}
}

func TestResolveLeafGenerationPackSourceFileIDs_AllowsCurrentWritableWhenRequested(t *testing.T) {
	db, leafLog, dir := openLeafGenerationPackTestDB(t)
	writeLeafGenerationKeys(t, db, "k", 32, 'a')
	_, fileID := currentLeafSegmentOrFatal(t, leafLog)
	manifest := loadLeafGenerationManifestOrFatal(t, dir)
	if manifest.CurrentGenerationID == 0 {
		t.Fatal("expected current generation id")
	}
	if _, _, err := db.resolveLeafGenerationPackSourceFileIDs([]uint64{manifest.CurrentGenerationID}, false); err == nil {
		t.Fatal("expected writable current generation to be rejected without opt-in")
	}
	fileIDs, matched, err := db.resolveLeafGenerationPackSourceFileIDs([]uint64{manifest.CurrentGenerationID}, true)
	if err != nil {
		t.Fatalf("resolveLeafGenerationPackSourceFileIDs allow current: %v", err)
	}
	if matched != 1 {
		t.Fatalf("matched=%d, want 1", matched)
	}
	wantRaw := page.ValueLogSegmentID(fileID)
	if len(fileIDs) != 1 || fileIDs[0] != wantRaw {
		t.Fatalf("fileIDs=%v, want [%d]", fileIDs, wantRaw)
	}
}
