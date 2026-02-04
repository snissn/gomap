package template

import (
	"fmt"
	"sync"
	"sync/atomic"
)

const (
	reasonSkipSmall            = "tmpl_skip_small"
	reasonSkipNoFPs            = "tmpl_skip_no_fps"
	reasonSkipCold             = "tmpl_skip_cold"
	reasonFPLookupErr          = "tmpl_fp_lookup_err"
	reasonNoCandidates         = "tmpl_no_candidates"
	reasonTemplateFetchErr     = "tmpl_template_fetch_err"
	reasonMatchExpectedSavings = "tmpl_match_fail_expected_savings"
	reasonMatchOpsCap          = "tmpl_match_fail_ops_cap"
	reasonMatchMissingAnchor   = "tmpl_match_fail_missing_anchor"
	reasonMatchOverlap         = "tmpl_match_fail_overlap"
	reasonKeepNoSavings        = "tmpl_keep_fail_no_savings"
	reasonKeepBounds           = "tmpl_keep_fail_bounds"
)

// TemplateStats tracks template compression outcomes.
type TemplateStats struct {
	Attempted                    atomic.Uint64
	Matched                      atomic.Uint64
	Kept                         atomic.Uint64
	BytesSaved                   atomic.Uint64
	CandidateFPReads             atomic.Uint64
	CandidateTemplatesConsidered atomic.Uint64
	TemplateFetches              atomic.Uint64
	TemplatesPublished           atomic.Uint64
	MaskSparseUsed               atomic.Uint64
	MaskFullUsed                 atomic.Uint64

	TrainEnqueueAttempts  atomic.Uint64
	TrainEnqueued         atomic.Uint64
	TrainDroppedQueueFull atomic.Uint64
	TrainDroppedTooLarge  atomic.Uint64
	TrainRouted           atomic.Uint64
	TrainDroppedShardFull atomic.Uint64
	TrainProcessed        atomic.Uint64
	PublishBatches        atomic.Uint64
	PublishDefs           atomic.Uint64
	PublishErrors         atomic.Uint64

	reasonsMu sync.Mutex
	reasons   map[string]uint64
}

func (s *TemplateStats) addReason(code string) {
	if code == "" {
		return
	}
	s.reasonsMu.Lock()
	if s.reasons == nil {
		s.reasons = make(map[string]uint64)
	}
	s.reasons[code]++
	s.reasonsMu.Unlock()
}

// Snapshot returns a copy of stats suitable for reporting.
func (s *TemplateStats) Snapshot() map[string]string {
	out := map[string]string{
		"attempted":                            fmt.Sprintf("%d", s.Attempted.Load()),
		"matched":                              fmt.Sprintf("%d", s.Matched.Load()),
		"kept":                                 fmt.Sprintf("%d", s.Kept.Load()),
		"bytes_saved_total":                    fmt.Sprintf("%d", s.BytesSaved.Load()),
		"candidate_fp_reads_total":             fmt.Sprintf("%d", s.CandidateFPReads.Load()),
		"candidate_templates_considered_total": fmt.Sprintf("%d", s.CandidateTemplatesConsidered.Load()),
		"template_fetches_total":               fmt.Sprintf("%d", s.TemplateFetches.Load()),
		"templates_published_total":            fmt.Sprintf("%d", s.TemplatesPublished.Load()),
		"mask_sparse_used_total":               fmt.Sprintf("%d", s.MaskSparseUsed.Load()),
		"mask_full_used_total":                 fmt.Sprintf("%d", s.MaskFullUsed.Load()),
		"train_enqueue_attempts_total":         fmt.Sprintf("%d", s.TrainEnqueueAttempts.Load()),
		"train_enqueued_total":                 fmt.Sprintf("%d", s.TrainEnqueued.Load()),
		"train_dropped_queue_full_total":       fmt.Sprintf("%d", s.TrainDroppedQueueFull.Load()),
		"train_dropped_too_large_total":        fmt.Sprintf("%d", s.TrainDroppedTooLarge.Load()),
		"train_routed_total":                   fmt.Sprintf("%d", s.TrainRouted.Load()),
		"train_dropped_shard_full_total":       fmt.Sprintf("%d", s.TrainDroppedShardFull.Load()),
		"train_processed_total":                fmt.Sprintf("%d", s.TrainProcessed.Load()),
		"publish_batches_total":                fmt.Sprintf("%d", s.PublishBatches.Load()),
		"publish_defs_total":                   fmt.Sprintf("%d", s.PublishDefs.Load()),
		"publish_errors_total":                 fmt.Sprintf("%d", s.PublishErrors.Load()),
	}
	s.reasonsMu.Lock()
	for k, v := range s.reasons {
		out["reason."+k] = fmt.Sprintf("%d", v)
	}
	s.reasonsMu.Unlock()
	return out
}
