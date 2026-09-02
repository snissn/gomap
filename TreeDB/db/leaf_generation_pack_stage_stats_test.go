package db

import (
	"context"
	"encoding/json"
	"testing"
)

func TestLeafGenerationPack_ApplyStageAccounting(t *testing.T) {
	db, leafLog, _ := openLeafGenerationPackTestDB(t)
	candidate := prepareLeafGenerationPackTestCandidate(t, db, leafLog, 1024)

	stats, err := db.LeafGenerationPack(context.Background(), LeafGenerationPackOptions{
		GenerationIDs: []uint64{candidate.generation.GenerationID},
		Force:         true,
	})
	if err != nil {
		t.Fatalf("LeafGenerationPack: %v", err)
	}
	stages := stats.ApplyStages
	copyAttributed := stages.TreeRewriteTimeNanos + stages.LeafSyncTimeNanos + stages.CopyCloseTimeNanos
	if copyAttributed <= 0 {
		t.Fatalf("copy stage timings were not populated: %+v", stages)
	}
	if copyAttributed > stats.CopyTimeNanos {
		t.Fatalf("copy stages=%d exceed copy wall=%d: %+v", copyAttributed, stats.CopyTimeNanos, stages)
	}
	// Promotion runs under the publication visibility gate so a concurrent
	// Refresh cannot discover and pin an uncommitted segment.
	if stages.PromotionTimeNanos <= 0 {
		t.Fatalf("promotion timing was not populated: %+v", stages)
	}
	publishAttributed := stages.RevalidateTimeNanos + stages.PromotionTimeNanos + stages.RelocationTimeNanos +
		stages.PageSyncTimeNanos + stages.DirectorySyncWaitTimeNanos + stages.RegistrationTimeNanos +
		stages.CollectionPublishTimeNanos
	if publishAttributed <= 0 {
		t.Fatalf("publish stage timings were not populated: %+v", stages)
	}
	if publishAttributed > stats.PublishHoldNanos {
		t.Fatalf("exclusive publish stages=%d exceed publish hold=%d: %+v", publishAttributed, stats.PublishHoldNanos, stages)
	}
	// Finalization prepares the durable-root candidate under the root lock, then
	// performs stable-resource and metadata I/O after releasing that lock. Its
	// full wall time is therefore observable but not additive to PublishHold.
	if stages.FinalizeTimeNanos <= 0 {
		t.Fatalf("finalize timing was not populated: %+v", stages)
	}
	// DirectorySyncTimeNanos is measured inside the async worker, while the
	// wait includes channel delivery and timer quantization. In particular,
	// Windows can report a zero operation duration for a sub-millisecond sync,
	// and scheduler delay can make the measured receive wait exceed a small
	// non-zero operation duration. The publish-hold assertion above is the
	// meaningful additive bound for the non-overlapping wait.
	if stats.RetryApplyStages != (LeafGenerationPackApplyStageStats{}) {
		t.Fatalf("successful first attempt reported retry stages: %+v", stats.RetryApplyStages)
	}
}

func TestLeafGenerationPackStats_JSONTimingFieldsRemainAdditive(t *testing.T) {
	const legacy = `{"CopyTimeNanos":11,"RetryCopyTimeNanos":12,"PublishWaitNanos":13,"PublishHoldNanos":14}`
	var decoded LeafGenerationPackStats
	if err := json.Unmarshal([]byte(legacy), &decoded); err != nil {
		t.Fatalf("Unmarshal legacy stats: %v", err)
	}
	if decoded.CopyTimeNanos != 11 || decoded.RetryCopyTimeNanos != 12 || decoded.PublishWaitNanos != 13 || decoded.PublishHoldNanos != 14 {
		t.Fatalf("legacy timing fields changed: %+v", decoded)
	}

	decoded.ApplyStages.TreeRewriteTimeNanos = 15
	encoded, err := json.Marshal(decoded)
	if err != nil {
		t.Fatalf("Marshal stats: %v", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatalf("Unmarshal encoded stats: %v", err)
	}
	for _, name := range []string{
		"CopyTimeNanos",
		"RetryCopyTimeNanos",
		"PublishWaitNanos",
		"PublishHoldNanos",
		"ApplyStages",
		"RetryApplyStages",
	} {
		if _, ok := fields[name]; !ok {
			t.Fatalf("encoded stats missing %q: %s", name, encoded)
		}
	}
}
