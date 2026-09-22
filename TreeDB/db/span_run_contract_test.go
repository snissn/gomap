package db

import (
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
)

func TestFlushSpanRunFallbackReasonStringsAreStable(t *testing.T) {
	want := map[FlushSpanRunFallbackReason]string{
		FlushSpanRunFallbackUnknown:                  "unknown",
		FlushSpanRunFallbackDisabled:                 "disabled",
		FlushSpanRunFallbackBelowThreshold:           "below_threshold",
		FlushSpanRunFallbackSpanNativeNotImplemented: "span_native_not_implemented",
		FlushSpanRunFallbackPrepareError:             "prepare_error",
		FlushSpanRunFallbackValidationFailed:         "validation_failed",
		FlushSpanRunFallbackRootMismatch:             "root_mismatch",
		FlushSpanRunFallbackRangeDeleteBarrier:       "range_delete_barrier",
		FlushSpanRunFallbackLaneBarrier:              "lane_barrier",
		FlushSpanRunFallbackCommandWALBarrier:        "command_wal_barrier",
		FlushSpanRunFallbackInexactLeafSpans:         "inexact_leaf_spans",
		FlushSpanRunFallbackColdBuild:                "cold_build",
		FlushSpanRunFallbackMaintenance:              "maintenance",
		FlushSpanRunFallbackBackendChunkSplit:        "backend_chunk_split",
		FlushSpanRunFallbackCloseOrCheckpoint:        "close_or_checkpoint",
		FlushSpanRunFallbackMemoryEmergencyCap:       "memory_or_emergency_cap",
		FlushSpanRunFallbackOutputOwnershipFailure:   "output_ownership_failure",
		FlushSpanRunFallbackReducerValidationFailed:  "reducer_validation_failed",
		FlushSpanRunFallbackRouteIneligible:          "route_ineligible",
		FlushSpanRunFallbackAdmissionPolicyDecline:   "admission_policy_decline",
		FlushSpanRunFallbackReducerValidationGuard:   "reducer_validation_guard",
	}
	if got := len(FlushSpanRunFallbackReasons()); got != len(want) {
		t.Fatalf("fallback reason count=%d want %d", got, len(want))
	}
	for reason, name := range want {
		if got := reason.String(); got != name {
			t.Fatalf("reason %d string=%q want %q", reason, got, name)
		}
		parsed, ok := ParseFlushSpanRunFallbackReason(name)
		if !ok || parsed != reason {
			t.Fatalf("parse %q=(%v,%v), want %v,true", name, parsed, ok, reason)
		}
	}
	if _, ok := ParseFlushSpanRunFallbackReason("does_not_exist"); ok {
		t.Fatalf("unknown fallback reason parsed successfully")
	}
}

func TestValidateFlushSpanRunMetadataCoversShadowingAndBarriers(t *testing.T) {
	meta := FlushSpanRunMetadata{
		RunID:            7,
		BaseRoot:         FlushSpanRunBaseRootValidation{CapturedRootID: 100, CurrentRootID: 100, Matched: true},
		SourceMemtables:  3,
		SourcePointOps:   6,
		PlannedPointOps:  4,
		ShadowedPointOps: 2,
		RangeBarriers:    1,
		LaneBarriers:     1,
		TargetLeafSpans: []FlushSpanRunTargetLeafSpan{
			{PointOpStart: 0, PointOpEnd: 2, OpCount: 2, ByteCount: 200},
			{SpanIndex: 1, PointOpStart: 2, PointOpEnd: 4, OpCount: 2, ByteCount: 160},
		},
		BackendChunks: []FlushSpanRunBackendChunk{
			{PointOpStart: 0, PointOpEnd: 3, ByteCount: 256},
			{ChunkIndex: 1, PointOpStart: 3, PointOpEnd: 4, ByteCount: 64},
		},
	}
	if err := ValidateFlushSpanRunMetadata(meta); err != nil {
		t.Fatalf("valid metadata rejected: %v", err)
	}

	badShadow := meta
	badShadow.ShadowedPointOps = 3
	if err := ValidateFlushSpanRunMetadata(badShadow); err == nil {
		t.Fatalf("metadata with inconsistent shadow count passed validation")
	}

	badZeroSource := meta
	badZeroSource.SourcePointOps = 0
	badZeroSource.PlannedPointOps = 1
	badZeroSource.ShadowedPointOps = 0
	badZeroSource.TargetLeafSpans = nil
	badZeroSource.BackendChunks = nil
	if err := ValidateFlushSpanRunMetadata(badZeroSource); err == nil {
		t.Fatalf("metadata with planned ops but zero source ops passed validation")
	}

	badMissingSpans := meta
	badMissingSpans.TargetLeafSpans = nil
	if err := ValidateFlushSpanRunMetadata(badMissingSpans); err == nil {
		t.Fatalf("metadata with planned ops but no target leaf spans passed validation")
	}

	badSpan := meta
	badSpan.TargetLeafSpans = append([]FlushSpanRunTargetLeafSpan(nil), meta.TargetLeafSpans...)
	badSpan.TargetLeafSpans[1].PointOpStart = 1
	if err := ValidateFlushSpanRunMetadata(badSpan); err == nil {
		t.Fatalf("metadata with overlapping target leaf spans passed validation")
	}

	badSpanGap := meta
	badSpanGap.TargetLeafSpans = append([]FlushSpanRunTargetLeafSpan(nil), meta.TargetLeafSpans...)
	badSpanGap.TargetLeafSpans[1].PointOpStart = 3
	if err := ValidateFlushSpanRunMetadata(badSpanGap); err == nil {
		t.Fatalf("metadata with gap between target leaf spans passed validation")
	}

	badSpanTailGap := meta
	badSpanTailGap.TargetLeafSpans = append([]FlushSpanRunTargetLeafSpan(nil), meta.TargetLeafSpans[:1]...)
	if err := ValidateFlushSpanRunMetadata(badSpanTailGap); err == nil {
		t.Fatalf("metadata with uncovered planned point ops passed validation")
	}

	badDeleteRange := meta
	badDeleteRange.TargetLeafSpans = append([]FlushSpanRunTargetLeafSpan(nil), meta.TargetLeafSpans...)
	badDeleteRange.TargetLeafSpans[0].DeleteRangeStart = 0
	badDeleteRange.TargetLeafSpans[0].DeleteRangeEnd = 2
	if err := ValidateFlushSpanRunMetadata(badDeleteRange); err == nil {
		t.Fatalf("metadata with out-of-bounds delete range indexes passed validation")
	}

	badSpanIndex := meta
	badSpanIndex.TargetLeafSpans = append([]FlushSpanRunTargetLeafSpan(nil), meta.TargetLeafSpans...)
	badSpanIndex.TargetLeafSpans[1].SpanIndex = 0
	if err := ValidateFlushSpanRunMetadata(badSpanIndex); err == nil {
		t.Fatalf("metadata with duplicate span index passed validation")
	}

	badChunk := meta
	badChunk.BackendChunks = append([]FlushSpanRunBackendChunk(nil), meta.BackendChunks...)
	badChunk.BackendChunks[1].PointOpStart = 2
	if err := ValidateFlushSpanRunMetadata(badChunk); err == nil {
		t.Fatalf("metadata with overlapping backend chunks passed validation")
	}

	badChunkIndex := meta
	badChunkIndex.BackendChunks = append([]FlushSpanRunBackendChunk(nil), meta.BackendChunks...)
	badChunkIndex.BackendChunks[1].ChunkIndex = 0
	if err := ValidateFlushSpanRunMetadata(badChunkIndex); err == nil {
		t.Fatalf("metadata with duplicate chunk index passed validation")
	}
}

func TestPlanFlushSpanRunCapturesTargetLeafMetadata(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()
	seed := d.NewBatch()
	for i := 0; i < 512; i++ {
		if err := seed.Set([]byte(fmt.Sprintf("key-%06d", i)), []byte("seed")); err != nil {
			t.Fatalf("seed set %d: %v", i, err)
		}
	}
	if err := seed.WriteSync(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	if err := seed.Close(); err != nil {
		t.Fatalf("seed close: %v", err)
	}

	ops := []batch.Entry{
		{Type: batch.OpPut, Key: []byte("key-000010"), Value: []byte("new")},
		{Type: batch.OpDelete, Key: []byte("key-000300")},
	}
	meta, err := d.PlanFlushSpanRun(FlushSpanRunPlanRequest{
		SourceMemtables:  2,
		SourcePointOps:   3,
		PlannedPointOps:  len(ops),
		ShadowedPointOps: 1,
		PointOps:         ops,
	})
	if err != nil {
		t.Fatalf("PlanFlushSpanRun: %v", err)
	}
	if meta.BaseRoot.CapturedRootID == 0 || !meta.BaseRoot.Matched {
		t.Fatalf("base root not captured: %+v", meta.BaseRoot)
	}
	if got := len(meta.TargetLeafSpans); got == 0 {
		t.Fatalf("target leaf spans empty: %+v", meta)
	}
	if err := ValidateFlushSpanRunMetadata(meta); err != nil {
		t.Fatalf("metadata validation: %v", err)
	}
	sawPointSpan := false
	for _, span := range meta.TargetLeafSpans {
		if span.PointOpEnd <= span.PointOpStart {
			continue
		}
		sawPointSpan = true
		if len(span.FirstOpKey) == 0 || len(span.LastOpKey) == 0 {
			t.Fatalf("exact PlanFlushSpanRun omitted point key bounds: %+v", span)
		}
	}
	if !sawPointSpan {
		t.Fatalf("PlanFlushSpanRun produced no point spans: %+v", meta.TargetLeafSpans)
	}

	chunkPlan, err := d.PlanFlushSpanRunChunks(FlushSpanRunPlanRequest{
		SourceMemtables:  2,
		SourcePointOps:   3,
		PlannedPointOps:  len(ops),
		ShadowedPointOps: 1,
		PointOps:         ops,
	}, 1)
	if err != nil {
		t.Fatalf("PlanFlushSpanRunChunks: %v", err)
	}
	if chunkPlan.TargetLeafSpans == 0 || chunkPlan.SpanOps == 0 || len(chunkPlan.BackendChunks) == 0 {
		t.Fatalf("empty chunk plan: %+v", chunkPlan)
	}
}

func TestFlushSpanRunChunkSplitFixtureEntryChunksSplitTargetLeaf(t *testing.T) {
	// This deterministic fixture is the M8 proof case: entry-count chunking at
	// four entries would split the first target leaf's ten point ops across three
	// backend chunks. A later leaf-aware chunker should make this counter zero.
	spans := []FlushSpanRunTargetLeafSpan{
		{PointOpStart: 0, PointOpEnd: 10, OpCount: 10, ByteCount: 1000},
		{SpanIndex: 1, PointOpStart: 10, PointOpEnd: 14, OpCount: 4, ByteCount: 400},
	}
	entryCountChunks := []FlushSpanRunBackendChunk{
		{PointOpStart: 0, PointOpEnd: 4},
		{ChunkIndex: 1, PointOpStart: 4, PointOpEnd: 8},
		{ChunkIndex: 2, PointOpStart: 8, PointOpEnd: 12},
		{ChunkIndex: 3, PointOpStart: 12, PointOpEnd: 14},
	}
	summary := SummarizeFlushSpanRunChunkSplits(spans, entryCountChunks)
	if summary.BackendChunks != 4 || summary.TargetLeafSpans != 2 {
		t.Fatalf("summary identity mismatch: %+v", summary)
	}
	if got, want := summary.TargetLeavesSplitAcrossChunks, 2; got != want {
		t.Fatalf("split target leaves=%d want %d (summary=%+v)", got, want, summary)
	}
	if got, want := summary.MaxChunksPerTargetLeaf, 3; got != want {
		t.Fatalf("max chunks per target leaf=%d want %d", got, want)
	}

	leafAwareChunks := []FlushSpanRunBackendChunk{
		{PointOpStart: 0, PointOpEnd: 10},
		{ChunkIndex: 1, PointOpStart: 10, PointOpEnd: 14},
	}
	leafAware := SummarizeFlushSpanRunChunkSplits(spans, leafAwareChunks)
	if leafAware.TargetLeavesSplitAcrossChunks != 0 {
		t.Fatalf("leaf-aware chunking split target leaves: %+v", leafAware)
	}
}
