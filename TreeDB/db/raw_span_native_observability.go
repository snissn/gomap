package db

import (
	"fmt"
	"sync/atomic"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/zipper"
)

// RawSpanNativeRoute is the stable support route label for raw TreeDB write
// batches reaching the backend apply boundary.
type RawSpanNativeRoute string

const (
	RawSpanNativeRoutePointPut               RawSpanNativeRoute = "point_put"
	RawSpanNativeRoutePointDelete            RawSpanNativeRoute = "point_delete"
	RawSpanNativeRouteMixedPoint             RawSpanNativeRoute = "mixed_point"
	RawSpanNativeRouteRangeDelete            RawSpanNativeRoute = "range_delete"
	RawSpanNativeRouteMixedRangeDelete       RawSpanNativeRoute = "mixed_range_delete"
	RawSpanNativeRouteEmptyBatch             RawSpanNativeRoute = "empty_batch"
	RawSpanNativeRouteCloseOrCheckpointDrain RawSpanNativeRoute = "close_or_checkpoint_drain"
)

const rawSpanNativeRouteCount = 7

var rawSpanNativeRouteList = [...]RawSpanNativeRoute{
	RawSpanNativeRoutePointPut,
	RawSpanNativeRoutePointDelete,
	RawSpanNativeRouteMixedPoint,
	RawSpanNativeRouteRangeDelete,
	RawSpanNativeRouteMixedRangeDelete,
	RawSpanNativeRouteEmptyBatch,
	RawSpanNativeRouteCloseOrCheckpointDrain,
}

type rawSpanNativeRouteCounters struct {
	observations         atomic.Uint64
	candidateOps         atomic.Uint64
	candidateSpans       atomic.Uint64
	eligibleOps          atomic.Uint64
	eligibleSpans        atomic.Uint64
	usedOps              atomic.Uint64
	usedSpans            atomic.Uint64
	ineligibleOps        atomic.Uint64
	ineligibleSpans      atomic.Uint64
	fallbacks            atomic.Uint64
	fallbackReasonCounts [FlushSpanRunFallbackReasonCount]atomic.Uint64
	fallbackOps          [FlushSpanRunFallbackReasonCount]atomic.Uint64
	fallbackSpans        [FlushSpanRunFallbackReasonCount]atomic.Uint64
}

type rawSpanNativeBatchPlan struct {
	route RawSpanNativeRoute
	ops   int
}

type rawSpanNativeEligibilityRequest struct {
	route                         RawSpanNativeRoute
	summary                       zipper.ReadOnlyLeafSpanSummary
	deltaOps                      int
	readOnlyPrepareRequested      bool
	readOnlyPrepareFailed         bool
	readOnlyPrepareValidationFail bool
	err                           error
	explicitFallbackReason        string
	spanNativeEligible            bool
	spanNativeUsed                bool
	applyOptionsUsed              bool
	spanNativeRequested           bool
}

type rawSpanNativeObservation struct {
	route          RawSpanNativeRoute
	candidate      bool
	eligible       bool
	used           bool
	ops            uint64
	spans          uint64
	fallbackReason FlushSpanRunFallbackReason
}

func rawSpanNativeRoutes() []RawSpanNativeRoute {
	return rawSpanNativeRouteList[:]
}

func rawSpanNativeRouteIndex(route RawSpanNativeRoute) int {
	switch route {
	case RawSpanNativeRoutePointPut:
		return 0
	case RawSpanNativeRoutePointDelete:
		return 1
	case RawSpanNativeRouteMixedPoint:
		return 2
	case RawSpanNativeRouteRangeDelete:
		return 3
	case RawSpanNativeRouteMixedRangeDelete:
		return 4
	case RawSpanNativeRouteEmptyBatch:
		return 5
	case RawSpanNativeRouteCloseOrCheckpointDrain:
		return 6
	default:
		return -1
	}
}

func (db *DB) rawSpanNativeRouteCountersFor(route RawSpanNativeRoute) *rawSpanNativeRouteCounters {
	if db == nil {
		return nil
	}
	idx := rawSpanNativeRouteIndex(route)
	if idx < 0 || idx >= len(db.rawSpanNativeRouteCounters) {
		return nil
	}
	return &db.rawSpanNativeRouteCounters[idx]
}

func (b *Batch) rawSpanNativeBatchPlan() rawSpanNativeBatchPlan {
	if b == nil || b.batch == nil {
		return rawSpanNativeBatchPlan{route: RawSpanNativeRouteEmptyBatch}
	}
	entries := b.batch.OrderedEntries()
	rangeOps := 0
	puts, deletes := 0, 0
	for _, entry := range entries {
		switch entry.Type {
		case batch.OpDelete:
			deletes++
		case batch.OpDeleteRange:
			rangeOps++
		default:
			puts++
		}
	}
	ops := puts + deletes + rangeOps
	if ops == 0 {
		return rawSpanNativeBatchPlan{route: RawSpanNativeRouteEmptyBatch}
	}
	if b.flushApplySpanNativeFallbackReason == FlushSpanRunFallbackCloseOrCheckpoint {
		return rawSpanNativeBatchPlan{route: RawSpanNativeRouteCloseOrCheckpointDrain, ops: ops}
	}
	switch {
	case rangeOps > 0 && puts+deletes > 0:
		return rawSpanNativeBatchPlan{route: RawSpanNativeRouteMixedRangeDelete, ops: ops}
	case rangeOps > 0:
		return rawSpanNativeBatchPlan{route: RawSpanNativeRouteRangeDelete, ops: ops}
	case puts > 0 && deletes > 0:
		return rawSpanNativeBatchPlan{route: RawSpanNativeRouteMixedPoint, ops: ops}
	case deletes > 0:
		return rawSpanNativeBatchPlan{route: RawSpanNativeRoutePointDelete, ops: ops}
	default:
		return rawSpanNativeBatchPlan{route: RawSpanNativeRoutePointPut, ops: ops}
	}
}

func (db *DB) observeRawSpanNativeApplyResult(plan rawSpanNativeBatchPlan, result zipper.ApplyResult, err error, applyOptionsUsed bool, spanNativeRequested bool) {
	if db == nil {
		return
	}
	if rawSpanNativeRouteIndex(plan.route) < 0 {
		return
	}
	observation := db.rawSpanNativeEligibility(rawSpanNativeEligibilityRequest{
		route:                         plan.route,
		summary:                       result.ReadOnlyPrepare.LeafSpanSummary(),
		deltaOps:                      plan.ops,
		readOnlyPrepareRequested:      result.ReadOnlyPrepareRequested,
		readOnlyPrepareFailed:         result.ReadOnlyPrepareFailed,
		readOnlyPrepareValidationFail: result.ReadOnlyPrepareValidationFailed,
		err:                           err,
		explicitFallbackReason:        result.SpanNativeFallbackReason,
		spanNativeEligible:            result.SpanNativeEligible,
		spanNativeUsed:                result.SpanNativeUsed,
		applyOptionsUsed:              applyOptionsUsed,
		spanNativeRequested:           spanNativeRequested,
	})
	db.observeRawSpanNativeObservation(observation)
}

func (db *DB) observeRawSpanNativePublishFallback(plan rawSpanNativeBatchPlan, snapshot flushApplySpanNativePublishSnapshot, reason FlushSpanRunFallbackReason) {
	if db == nil || !reason.Valid() || reason == FlushSpanRunFallbackUnknown {
		return
	}
	routeCounters := db.rawSpanNativeRouteCountersFor(plan.route)
	if routeCounters == nil {
		return
	}
	if !snapshot.preparedSpanNativeCandidate() {
		return
	}
	ops, spans := rawSpanNativeOpsAndSpans(snapshot.summary, plan.ops)
	db.rawSpanNativeFallbacks.Add(1)
	db.rawSpanNativeFallbackReasonCounts[reason].Add(1)
	db.rawSpanNativeFallbackOps[reason].Add(ops)
	db.rawSpanNativeFallbackSpans[reason].Add(spans)
	routeCounters.fallbacks.Add(1)
	routeCounters.fallbackReasonCounts[reason].Add(1)
	routeCounters.fallbackOps[reason].Add(ops)
	routeCounters.fallbackSpans[reason].Add(spans)
}

func (db *DB) observeRawBatchSpanNativePublishFallback(plan rawSpanNativeBatchPlan, snapshot flushApplySpanNativePublishSnapshot, reason FlushSpanRunFallbackReason) {
	db.observeFlushApplySpanNativePublishFallback(snapshot, reason)
	db.observeRawSpanNativePublishFallback(plan, snapshot, reason)
}

func (db *DB) rawSpanNativeEligibility(req rawSpanNativeEligibilityRequest) rawSpanNativeObservation {
	ops, spans := rawSpanNativeOpsAndSpans(req.summary, req.deltaOps)
	observation := rawSpanNativeObservation{
		route: req.route,
		ops:   ops,
		spans: spans,
	}
	if req.spanNativeUsed {
		observation.candidate = true
		observation.eligible = true
		observation.used = true
		return observation
	}
	reason, hasExplicitReason := parseFlushApplySpanNativeFallbackReason(req.explicitFallbackReason)
	if req.spanNativeRequested && req.applyOptionsUsed && req.readOnlyPrepareRequested && ops > 0 {
		observation.candidate = true
	}
	if req.spanNativeEligible {
		observation.candidate = true
		observation.eligible = true
	}
	if !hasExplicitReason {
		reason = db.classifyRawSpanNativeFallback(req, ops)
	}
	if !reason.Valid() {
		reason = FlushSpanRunFallbackUnknown
	}
	observation.fallbackReason = reason
	return observation
}

func rawSpanNativeOpsAndSpans(summary zipper.ReadOnlyLeafSpanSummary, deltaOps int) (uint64, uint64) {
	ops := summary.Ops
	if ops == 0 {
		ops = summary.PointOps + summary.DeleteRanges
	}
	if ops == 0 && deltaOps > 0 {
		ops = deltaOps
	}
	spans := summary.Spans
	if ops < 0 {
		ops = 0
	}
	if spans < 0 {
		spans = 0
	}
	return uint64(ops), uint64(spans)
}

func (db *DB) classifyRawSpanNativeFallback(req rawSpanNativeEligibilityRequest, ops uint64) FlushSpanRunFallbackReason {
	summary := req.summary
	switch {
	case req.readOnlyPrepareValidationFail:
		return FlushSpanRunFallbackValidationFailed
	case req.readOnlyPrepareRequested && req.readOnlyPrepareFailed:
		return FlushSpanRunFallbackPrepareError
	case ops == 0:
		return FlushSpanRunFallbackBelowThreshold
	}
	admission := FlushAdmissionDecision{Policy: FlushAdmissionPolicyAuto}.withStatsDefaults()
	if db != nil {
		admission = db.flushAdmission.withStatsDefaults()
	}
	if admission.Policy == FlushAdmissionPolicyOff {
		return FlushSpanRunFallbackDisabled
	}
	if !admission.Admitted {
		if admission.Policy != FlushAdmissionPolicyAuto {
			return FlushSpanRunFallbackDisabled
		}
		return FlushSpanRunFallbackAdmissionPolicyDecline
	}
	switch {
	case !req.applyOptionsUsed || !req.readOnlyPrepareRequested:
		return FlushSpanRunFallbackDisabled
	case !req.spanNativeRequested:
		return FlushSpanRunFallbackDisabled
	case req.route == RawSpanNativeRouteCloseOrCheckpointDrain:
		return FlushSpanRunFallbackCloseOrCheckpoint
	case summary.DeleteRanges > 0 || req.route == RawSpanNativeRouteRangeDelete || req.route == RawSpanNativeRouteMixedRangeDelete:
		return FlushSpanRunFallbackRangeDeleteBarrier
	case summary.Maintenance:
		return FlushSpanRunFallbackMaintenance
	case summary.ColdBuild:
		return FlushSpanRunFallbackColdBuild
	case summary.Spans > 0 && !summary.ExactLeafSpans:
		return FlushSpanRunFallbackInexactLeafSpans
	}
	return FlushSpanRunFallbackSpanNativeNotImplemented
}

func (db *DB) observeRawSpanNativeObservation(observation rawSpanNativeObservation) {
	if db == nil {
		return
	}
	routeCounters := db.rawSpanNativeRouteCountersFor(observation.route)
	if routeCounters == nil {
		return
	}
	routeCounters.observations.Add(1)
	if observation.candidate {
		db.rawSpanNativeCandidateOps.Add(observation.ops)
		db.rawSpanNativeCandidateSpans.Add(observation.spans)
		routeCounters.candidateOps.Add(observation.ops)
		routeCounters.candidateSpans.Add(observation.spans)
	}
	if observation.eligible {
		db.rawSpanNativeEligibleOps.Add(observation.ops)
		db.rawSpanNativeEligibleSpans.Add(observation.spans)
		routeCounters.eligibleOps.Add(observation.ops)
		routeCounters.eligibleSpans.Add(observation.spans)
	}
	if observation.used {
		db.rawSpanNativeUsedOps.Add(observation.ops)
		db.rawSpanNativeUsedSpans.Add(observation.spans)
		routeCounters.usedOps.Add(observation.ops)
		routeCounters.usedSpans.Add(observation.spans)
		return
	}
	if !observation.eligible {
		db.rawSpanNativeIneligibleOps.Add(observation.ops)
		db.rawSpanNativeIneligibleSpans.Add(observation.spans)
		routeCounters.ineligibleOps.Add(observation.ops)
		routeCounters.ineligibleSpans.Add(observation.spans)
	}
	reason := observation.fallbackReason
	if !reason.Valid() {
		reason = FlushSpanRunFallbackUnknown
	}
	db.rawSpanNativeFallbacks.Add(1)
	db.rawSpanNativeFallbackReasonCounts[reason].Add(1)
	db.rawSpanNativeFallbackOps[reason].Add(observation.ops)
	db.rawSpanNativeFallbackSpans[reason].Add(observation.spans)
	routeCounters.fallbacks.Add(1)
	routeCounters.fallbackReasonCounts[reason].Add(1)
	routeCounters.fallbackOps[reason].Add(observation.ops)
	routeCounters.fallbackSpans[reason].Add(observation.spans)
}

func (db *DB) appendRawSpanNativeStats(stats map[string]string) {
	if db == nil || stats == nil {
		return
	}
	prefix := "treedb.raw.span_native."
	stats[prefix+"candidate_ops_total"] = fmt.Sprintf("%d", db.rawSpanNativeCandidateOps.Load())
	stats[prefix+"candidate_spans_total"] = fmt.Sprintf("%d", db.rawSpanNativeCandidateSpans.Load())
	stats[prefix+"eligible_ops_total"] = fmt.Sprintf("%d", db.rawSpanNativeEligibleOps.Load())
	stats[prefix+"eligible_spans_total"] = fmt.Sprintf("%d", db.rawSpanNativeEligibleSpans.Load())
	stats[prefix+"used_ops_total"] = fmt.Sprintf("%d", db.rawSpanNativeUsedOps.Load())
	stats[prefix+"used_spans_total"] = fmt.Sprintf("%d", db.rawSpanNativeUsedSpans.Load())
	stats[prefix+"ineligible_ops_total"] = fmt.Sprintf("%d", db.rawSpanNativeIneligibleOps.Load())
	stats[prefix+"ineligible_spans_total"] = fmt.Sprintf("%d", db.rawSpanNativeIneligibleSpans.Load())
	stats[prefix+"fallbacks_total"] = fmt.Sprintf("%d", db.rawSpanNativeFallbacks.Load())
	for _, reason := range FlushSpanRunFallbackReasons() {
		name := reason.String()
		stats[prefix+"fallback.reason."+name+".count_total"] = fmt.Sprintf("%d", db.rawSpanNativeFallbackReasonCounts[reason].Load())
		stats[prefix+"fallback.reason."+name+".ops_total"] = fmt.Sprintf("%d", db.rawSpanNativeFallbackOps[reason].Load())
		stats[prefix+"fallback.reason."+name+".spans_total"] = fmt.Sprintf("%d", db.rawSpanNativeFallbackSpans[reason].Load())
	}
	for _, route := range rawSpanNativeRoutes() {
		routeCounters := db.rawSpanNativeRouteCountersFor(route)
		if routeCounters == nil {
			continue
		}
		routePrefix := prefix + "route." + string(route) + "."
		stats[routePrefix+"observations_total"] = fmt.Sprintf("%d", routeCounters.observations.Load())
		stats[routePrefix+"candidate_ops_total"] = fmt.Sprintf("%d", routeCounters.candidateOps.Load())
		stats[routePrefix+"candidate_spans_total"] = fmt.Sprintf("%d", routeCounters.candidateSpans.Load())
		stats[routePrefix+"eligible_ops_total"] = fmt.Sprintf("%d", routeCounters.eligibleOps.Load())
		stats[routePrefix+"eligible_spans_total"] = fmt.Sprintf("%d", routeCounters.eligibleSpans.Load())
		stats[routePrefix+"used_ops_total"] = fmt.Sprintf("%d", routeCounters.usedOps.Load())
		stats[routePrefix+"used_spans_total"] = fmt.Sprintf("%d", routeCounters.usedSpans.Load())
		stats[routePrefix+"ineligible_ops_total"] = fmt.Sprintf("%d", routeCounters.ineligibleOps.Load())
		stats[routePrefix+"ineligible_spans_total"] = fmt.Sprintf("%d", routeCounters.ineligibleSpans.Load())
		stats[routePrefix+"fallbacks_total"] = fmt.Sprintf("%d", routeCounters.fallbacks.Load())
		for _, reason := range FlushSpanRunFallbackReasons() {
			name := reason.String()
			stats[routePrefix+"fallback.reason."+name+".count_total"] = fmt.Sprintf("%d", routeCounters.fallbackReasonCounts[reason].Load())
			stats[routePrefix+"fallback.reason."+name+".ops_total"] = fmt.Sprintf("%d", routeCounters.fallbackOps[reason].Load())
			stats[routePrefix+"fallback.reason."+name+".spans_total"] = fmt.Sprintf("%d", routeCounters.fallbackSpans[reason].Load())
		}
	}
}
