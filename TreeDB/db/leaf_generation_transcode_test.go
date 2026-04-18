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
