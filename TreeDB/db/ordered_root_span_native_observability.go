package db

import (
	"fmt"
	"sync/atomic"

	"github.com/snissn/gomap/TreeDB/zipper"
)

// OrderedRootSpanNativeRoute is the stable support route label for ordered-root
// span-native eligibility. These labels are emitted through Stats and should be
// changed only at an explicit translation boundary.
type OrderedRootSpanNativeRoute string

const (
	OrderedRootSpanNativeRouteDirectPublish             OrderedRootSpanNativeRoute = "direct_publish"
	OrderedRootSpanNativeRouteGroupedPublish            OrderedRootSpanNativeRoute = "grouped_publish"
	OrderedRootSpanNativeRouteSystemDeltaBuilderPublish OrderedRootSpanNativeRoute = "system_delta_builder_publish"
	OrderedRootSpanNativeRouteCommandWALPublish         OrderedRootSpanNativeRoute = "command_wal_publish"
	OrderedRootSpanNativeRouteCollectionBufferedRoots   OrderedRootSpanNativeRoute = "collection_buffered_roots"
	OrderedRootSpanNativeRouteOverlayColdBuild          OrderedRootSpanNativeRoute = "overlay_cold_build"
	OrderedRootSpanNativeRouteMultiIndexGroupPublish    OrderedRootSpanNativeRoute = "multi_index_group_publish"
	OrderedRootSpanNativeRouteDeltaBatchPublish         OrderedRootSpanNativeRoute = "delta_batch_publish"
	OrderedRootSpanNativeRouteReadOnlyPrepare           OrderedRootSpanNativeRoute = "read_only_prepare"
)

const orderedRootSpanNativeRouteCount = 9

type orderedRootSpanNativeRouteCounters struct {
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

// OrderedRootSpanNativeStatus is the terminal support status for a route or
// observed publish attempt.
type OrderedRootSpanNativeStatus string

const (
	OrderedRootSpanNativeStatusIneligible OrderedRootSpanNativeStatus = "ineligible"
	OrderedRootSpanNativeStatusCandidate  OrderedRootSpanNativeStatus = "candidate"
	OrderedRootSpanNativeStatusEligible   OrderedRootSpanNativeStatus = "eligible"
	OrderedRootSpanNativeStatusUsed       OrderedRootSpanNativeStatus = "used"
	OrderedRootSpanNativeStatusFallback   OrderedRootSpanNativeStatus = "fallback"
)

// OrderedRootSpanNativeFallbackClass groups stable fallback reasons into the
// support buckets operators need during triage.
type OrderedRootSpanNativeFallbackClass string

const (
	OrderedRootSpanNativeFallbackClassNone           OrderedRootSpanNativeFallbackClass = "none"
	OrderedRootSpanNativeFallbackClassPolicy         OrderedRootSpanNativeFallbackClass = "policy"
	OrderedRootSpanNativeFallbackClassRoute          OrderedRootSpanNativeFallbackClass = "route"
	OrderedRootSpanNativeFallbackClassValidation     OrderedRootSpanNativeFallbackClass = "validation"
	OrderedRootSpanNativeFallbackClassReducerStorage OrderedRootSpanNativeFallbackClass = "reducer_storage"
	OrderedRootSpanNativeFallbackClassUnknown        OrderedRootSpanNativeFallbackClass = "unknown"
)

// OrderedRootSpanNativeTriageRow is a route-level support snapshot. It is
// intentionally independent of raw flush-apply counters.
type OrderedRootSpanNativeTriageRow struct {
	Route              OrderedRootSpanNativeRoute
	Context            string
	Status             OrderedRootSpanNativeStatus
	Candidate          bool
	Eligible           bool
	Used               bool
	Ops                uint64
	Spans              uint64
	FallbackReason     string
	FallbackClass      OrderedRootSpanNativeFallbackClass
	AdmissionPolicy    string
	AdmissionAdmitted  bool
	AdmissionReason    string
	SelectedWorkers    int
	RouteSupportDetail string
}

type orderedRootSpanNativeEligibilityRequest struct {
	Route                         OrderedRootSpanNativeRoute
	Context                       string
	Summary                       zipper.ReadOnlyLeafSpanSummary
	DeltaOps                      int
	ReadOnlyPrepareValidationFail bool
	ReadOnlyPrepareFailed         bool
	Err                           error
	ExplicitFallbackReason        string
	SpanNativeEligible            bool
	SpanNativeUsed                bool
	ForceColdBuild                bool
}

// OrderedRootSpanNativeTriageSnapshot returns the ordered-root-specific route
// support matrix for the current DB admission decision. It is a support surface:
// benchmark/report plumbing should consume these rows instead of inferring
// ordered-root proof from raw flush_apply span-native counters.
func (db *DB) OrderedRootSpanNativeTriageSnapshot() []OrderedRootSpanNativeTriageRow {
	routes := []struct {
		route     OrderedRootSpanNativeRoute
		context   string
		candidate bool
		eligible  bool
		reason    FlushSpanRunFallbackReason
		detail    string
	}{
		{
			route:   OrderedRootSpanNativeRouteDirectPublish,
			context: "full ordered-root iterator publish",
			reason:  FlushSpanRunFallbackRouteIneligible,
			detail:  "direct full-root publish is not a span-native delta route",
		},
		{
			route:   OrderedRootSpanNativeRouteGroupedPublish,
			context: "full ordered-root group publish",
			reason:  FlushSpanRunFallbackRouteIneligible,
			detail:  "grouped full-root publish is not a span-native delta route",
		},
		{
			route:     OrderedRootSpanNativeRouteSystemDeltaBuilderPublish,
			context:   "ordered-root delta group with system delta builder",
			candidate: true,
			eligible:  true,
			detail:    "warm ordered-root system delta groups can use span-native apply when admitted; unsupported rows fail closed with route-specific fallback reasons",
		},
		{
			route:     OrderedRootSpanNativeRouteCommandWALPublish,
			context:   "command-WAL covered ordered-root delta publish",
			candidate: true,
			eligible:  true,
			detail:    "command-WAL ordered-root deltas can use span-native apply after the command frame is appended and before the covered commit finalizes",
		},
		{
			route:     OrderedRootSpanNativeRouteCollectionBufferedRoots,
			context:   "collection buffered root delta publish",
			candidate: true,
			eligible:  true,
			detail:    "collection buffered roots publish through ordered-root delta routes and inherit the ordered-root span-native correctness contract",
		},
		{
			route:   OrderedRootSpanNativeRouteOverlayColdBuild,
			context: "overlay zero-base cold-build publish",
			reason:  FlushSpanRunFallbackColdBuild,
			detail:  "zero-base overlay/cold-build routes do not have existing leaf spans to replace",
		},
		{
			route:     OrderedRootSpanNativeRouteMultiIndexGroupPublish,
			context:   "multi-index ordered-root group publish",
			candidate: true,
			eligible:  true,
			detail:    "multi-index warm ordered-root groups can use span-native apply when admitted; cold and maintenance rows remain deterministic fallbacks",
		},
		{
			route:     OrderedRootSpanNativeRouteDeltaBatchPublish,
			context:   "ordered-root delta batch root apply",
			candidate: true,
			eligible:  true,
			detail:    "warm ordered-root delta batches are the runtime span-native candidate surface",
		},
		{
			route:     OrderedRootSpanNativeRouteReadOnlyPrepare,
			context:   "ordered-root read-only prepare proof",
			candidate: true,
			eligible:  true,
			detail:    "read-only prepare validates leaf-span planning for admitted ordered-root span-native apply",
		},
	}
	out := make([]OrderedRootSpanNativeTriageRow, 0, len(routes))
	for _, route := range routes {
		out = append(out, db.orderedRootSpanNativeRouteTriage(route.route, route.context, route.candidate, route.eligible, route.reason, route.detail))
	}
	return out
}

func (db *DB) orderedRootSpanNativeRouteTriage(route OrderedRootSpanNativeRoute, context string, candidate bool, eligible bool, fallback FlushSpanRunFallbackReason, detail string) OrderedRootSpanNativeTriageRow {
	admission := FlushAdmissionDecision{Policy: FlushAdmissionPolicyAuto}.withStatsDefaults()
	if db != nil {
		admission = db.flushAdmission.withStatsDefaults()
	}
	if !fallback.Valid() || fallback == FlushSpanRunFallbackUnknown {
		fallback = FlushSpanRunFallbackRouteIneligible
	}
	row := OrderedRootSpanNativeTriageRow{
		Route:              route,
		Context:            context,
		Candidate:          candidate,
		Status:             OrderedRootSpanNativeStatusIneligible,
		FallbackReason:     fallback.String(),
		FallbackClass:      orderedRootSpanNativeFallbackClass(fallback),
		AdmissionPolicy:    admission.Policy.String(),
		AdmissionAdmitted:  admission.Admitted,
		AdmissionReason:    admission.Reason,
		SelectedWorkers:    admission.FlushApplyConcurrency,
		RouteSupportDetail: detail,
	}
	if !candidate {
		return row
	}
	row.Status = OrderedRootSpanNativeStatusFallback
	if admission.Policy == FlushAdmissionPolicyOff {
		row.FallbackReason = FlushSpanRunFallbackDisabled.String()
		row.FallbackClass = orderedRootSpanNativeFallbackClass(FlushSpanRunFallbackDisabled)
		return row
	}
	if !admission.Admitted {
		row.FallbackReason = FlushSpanRunFallbackAdmissionPolicyDecline.String()
		row.FallbackClass = orderedRootSpanNativeFallbackClass(FlushSpanRunFallbackAdmissionPolicyDecline)
		return row
	}
	if !admission.FlushApplySpanNative {
		row.FallbackReason = FlushSpanRunFallbackDisabled.String()
		row.FallbackClass = orderedRootSpanNativeFallbackClass(FlushSpanRunFallbackDisabled)
		return row
	}
	if eligible {
		row.Eligible = true
		row.Status = OrderedRootSpanNativeStatusEligible
		row.FallbackReason = ""
		row.FallbackClass = OrderedRootSpanNativeFallbackClassNone
	}
	return row
}

func (db *DB) orderedRootSpanNativeEligibility(req orderedRootSpanNativeEligibilityRequest) OrderedRootSpanNativeTriageRow {
	if req.Route == "" {
		req.Route = OrderedRootSpanNativeRouteDeltaBatchPublish
	}
	admission := FlushAdmissionDecision{Policy: FlushAdmissionPolicyAuto}.withStatsDefaults()
	if db != nil {
		admission = db.flushAdmission.withStatsDefaults()
	}
	ops, spans := orderedRootSpanNativeOpsAndSpans(req.Summary, req.DeltaOps)
	row := OrderedRootSpanNativeTriageRow{
		Route:             req.Route,
		Context:           req.Context,
		Status:            OrderedRootSpanNativeStatusIneligible,
		Ops:               ops,
		Spans:             spans,
		AdmissionPolicy:   admission.Policy.String(),
		AdmissionAdmitted: admission.Admitted,
		AdmissionReason:   admission.Reason,
		SelectedWorkers:   admission.FlushApplyConcurrency,
	}
	if req.SpanNativeUsed {
		row.Candidate = true
		row.Eligible = true
		row.Used = true
		row.Status = OrderedRootSpanNativeStatusUsed
		row.FallbackClass = OrderedRootSpanNativeFallbackClassNone
		return row
	}

	reason, hasExplicitReason := parseFlushApplySpanNativeFallbackReason(req.ExplicitFallbackReason)
	if !hasExplicitReason || reason == FlushSpanRunFallbackSpanNativeNotImplemented {
		classifiedReason := db.classifyOrderedRootSpanNativeFallback(req, ops)
		if !hasExplicitReason {
			reason = classifiedReason
		} else if classifiedReason.Valid() && classifiedReason != FlushSpanRunFallbackSpanNativeNotImplemented && (classifiedReason != FlushSpanRunFallbackUnknown || req.Err != nil) {
			reason = classifiedReason
			hasExplicitReason = false
		}
	}
	if !reason.Valid() {
		reason = FlushSpanRunFallbackUnknown
	}

	row.Candidate = orderedRootSpanNativeCandidate(req.Route, req.Summary, ops)
	if row.Candidate {
		row.Status = OrderedRootSpanNativeStatusCandidate
	}
	if row.Candidate && req.SpanNativeEligible && reason == FlushSpanRunFallbackUnknown {
		row.Eligible = true
		row.Status = OrderedRootSpanNativeStatusEligible
		row.FallbackClass = OrderedRootSpanNativeFallbackClassNone
		return row
	}
	if row.Candidate && req.SpanNativeEligible && hasExplicitReason {
		row.Eligible = true
	}

	row.Status = OrderedRootSpanNativeStatusFallback
	if !row.Candidate {
		row.Status = OrderedRootSpanNativeStatusIneligible
	}
	row.FallbackReason = reason.String()
	row.FallbackClass = orderedRootSpanNativeFallbackClass(reason)
	return row
}

func (db *DB) classifyOrderedRootSpanNativeFallback(req orderedRootSpanNativeEligibilityRequest, ops uint64) FlushSpanRunFallbackReason {
	summary := req.Summary
	switch {
	case req.ReadOnlyPrepareValidationFail:
		return FlushSpanRunFallbackValidationFailed
	case req.Err != nil && req.ReadOnlyPrepareFailed:
		return FlushSpanRunFallbackPrepareError
	case req.ForceColdBuild || summary.ColdBuild:
		return FlushSpanRunFallbackColdBuild
	case summary.Maintenance && summary.PointOps <= 0:
		return FlushSpanRunFallbackMaintenance
	case summary.DeleteRanges > 0:
		return FlushSpanRunFallbackRangeDeleteBarrier
	case summary.Spans > 0 && !summary.ExactLeafSpans:
		return FlushSpanRunFallbackInexactLeafSpans
	case ops == 0:
		return FlushSpanRunFallbackBelowThreshold
	case !orderedRootSpanNativeRouteCanBeCandidate(req.Route):
		return FlushSpanRunFallbackRouteIneligible
	}
	admission := FlushAdmissionDecision{Policy: FlushAdmissionPolicyAuto}.withStatsDefaults()
	if db != nil {
		admission = db.flushAdmission.withStatsDefaults()
	}
	if admission.Policy == FlushAdmissionPolicyOff {
		return FlushSpanRunFallbackDisabled
	}
	if !admission.Admitted {
		return FlushSpanRunFallbackAdmissionPolicyDecline
	}
	if !admission.FlushApplySpanNative {
		return FlushSpanRunFallbackDisabled
	}
	return FlushSpanRunFallbackUnknown
}

func orderedRootSpanNativeCandidate(route OrderedRootSpanNativeRoute, summary zipper.ReadOnlyLeafSpanSummary, ops uint64) bool {
	if !orderedRootSpanNativeRouteCanBeCandidate(route) {
		return false
	}
	return ops > 0 &&
		summary.Spans > 0 &&
		summary.ExactLeafSpans &&
		!summary.ColdBuild &&
		(!summary.Maintenance || summary.PointOps > 0) &&
		summary.DeleteRanges == 0
}

func orderedRootSpanNativeRouteCanBeCandidate(route OrderedRootSpanNativeRoute) bool {
	switch route {
	case OrderedRootSpanNativeRouteSystemDeltaBuilderPublish,
		OrderedRootSpanNativeRouteCommandWALPublish,
		OrderedRootSpanNativeRouteCollectionBufferedRoots,
		OrderedRootSpanNativeRouteMultiIndexGroupPublish,
		OrderedRootSpanNativeRouteDeltaBatchPublish,
		OrderedRootSpanNativeRouteReadOnlyPrepare:
		return true
	default:
		return false
	}
}

func orderedRootSpanNativeRoutes() []OrderedRootSpanNativeRoute {
	return []OrderedRootSpanNativeRoute{
		OrderedRootSpanNativeRouteDirectPublish,
		OrderedRootSpanNativeRouteGroupedPublish,
		OrderedRootSpanNativeRouteSystemDeltaBuilderPublish,
		OrderedRootSpanNativeRouteCommandWALPublish,
		OrderedRootSpanNativeRouteCollectionBufferedRoots,
		OrderedRootSpanNativeRouteOverlayColdBuild,
		OrderedRootSpanNativeRouteMultiIndexGroupPublish,
		OrderedRootSpanNativeRouteDeltaBatchPublish,
		OrderedRootSpanNativeRouteReadOnlyPrepare,
	}
}

func orderedRootSpanNativeRouteIndex(route OrderedRootSpanNativeRoute) (int, bool) {
	switch route {
	case OrderedRootSpanNativeRouteDirectPublish:
		return 0, true
	case OrderedRootSpanNativeRouteGroupedPublish:
		return 1, true
	case OrderedRootSpanNativeRouteSystemDeltaBuilderPublish:
		return 2, true
	case OrderedRootSpanNativeRouteCommandWALPublish:
		return 3, true
	case OrderedRootSpanNativeRouteCollectionBufferedRoots:
		return 4, true
	case OrderedRootSpanNativeRouteOverlayColdBuild:
		return 5, true
	case OrderedRootSpanNativeRouteMultiIndexGroupPublish:
		return 6, true
	case OrderedRootSpanNativeRouteDeltaBatchPublish:
		return 7, true
	case OrderedRootSpanNativeRouteReadOnlyPrepare:
		return 8, true
	default:
		return 0, false
	}
}

func orderedRootSpanNativeOpsAndSpans(summary zipper.ReadOnlyLeafSpanSummary, deltaOps int) (uint64, uint64) {
	ops := summary.Ops
	if ops <= 0 {
		ops = summary.SpanOps
	}
	if ops <= 0 {
		ops = deltaOps
	}
	if ops < 0 {
		ops = 0
	}
	spans := summary.Spans
	if spans < 0 {
		spans = 0
	}
	return uint64(ops), uint64(spans)
}

func orderedRootSpanNativeFallbackClass(reason FlushSpanRunFallbackReason) OrderedRootSpanNativeFallbackClass {
	switch reason {
	case FlushSpanRunFallbackUnknown:
		return OrderedRootSpanNativeFallbackClassUnknown
	case FlushSpanRunFallbackDisabled,
		FlushSpanRunFallbackAdmissionPolicyDecline,
		FlushSpanRunFallbackMemoryEmergencyCap:
		return OrderedRootSpanNativeFallbackClassPolicy
	case FlushSpanRunFallbackValidationFailed,
		FlushSpanRunFallbackPrepareError:
		return OrderedRootSpanNativeFallbackClassValidation
	case FlushSpanRunFallbackRootMismatch,
		FlushSpanRunFallbackOutputOwnershipFailure,
		FlushSpanRunFallbackReducerValidationFailed:
		return OrderedRootSpanNativeFallbackClassReducerStorage
	default:
		return OrderedRootSpanNativeFallbackClassRoute
	}
}

func (db *DB) observeOrderedRootSpanNativeEligibility(row OrderedRootSpanNativeTriageRow) {
	if db == nil {
		return
	}
	routeCounters := db.orderedRootSpanNativeRouteCountersFor(row.Route)
	if routeCounters != nil {
		routeCounters.observations.Add(1)
	}
	if row.Candidate {
		if row.Ops > 0 {
			db.orderedRootSpanNativeCandidateOps.Add(row.Ops)
			if routeCounters != nil {
				routeCounters.candidateOps.Add(row.Ops)
			}
		}
		if row.Spans > 0 {
			db.orderedRootSpanNativeCandidateSpans.Add(row.Spans)
			if routeCounters != nil {
				routeCounters.candidateSpans.Add(row.Spans)
			}
		}
	}
	if row.Eligible {
		if row.Ops > 0 {
			db.orderedRootSpanNativeEligibleOps.Add(row.Ops)
			if routeCounters != nil {
				routeCounters.eligibleOps.Add(row.Ops)
			}
		}
		if row.Spans > 0 {
			db.orderedRootSpanNativeEligibleSpans.Add(row.Spans)
			if routeCounters != nil {
				routeCounters.eligibleSpans.Add(row.Spans)
			}
		}
	}
	if row.Used {
		if row.Ops > 0 {
			db.orderedRootSpanNativeUsedOps.Add(row.Ops)
			if routeCounters != nil {
				routeCounters.usedOps.Add(row.Ops)
			}
		}
		if row.Spans > 0 {
			db.orderedRootSpanNativeUsedSpans.Add(row.Spans)
			if routeCounters != nil {
				routeCounters.usedSpans.Add(row.Spans)
			}
		}
		return
	}
	if row.Status == OrderedRootSpanNativeStatusEligible {
		return
	}
	if !row.Eligible {
		if row.Ops > 0 {
			db.orderedRootSpanNativeIneligibleOps.Add(row.Ops)
			if routeCounters != nil {
				routeCounters.ineligibleOps.Add(row.Ops)
			}
		}
		if row.Spans > 0 {
			db.orderedRootSpanNativeIneligibleSpans.Add(row.Spans)
			if routeCounters != nil {
				routeCounters.ineligibleSpans.Add(row.Spans)
			}
		}
	}
	if row.Eligible && row.FallbackReason == "" {
		return
	}
	reason, ok := ParseFlushSpanRunFallbackReason(row.FallbackReason)
	if !ok || !reason.Valid() {
		reason = FlushSpanRunFallbackUnknown
	}
	db.orderedRootSpanNativeFallbacks.Add(1)
	db.orderedRootSpanNativeFallbackReasonCounts[reason].Add(1)
	if routeCounters != nil {
		routeCounters.fallbacks.Add(1)
		routeCounters.fallbackReasonCounts[reason].Add(1)
	}
	if row.Ops > 0 {
		db.orderedRootSpanNativeFallbackOps[reason].Add(row.Ops)
		if routeCounters != nil {
			routeCounters.fallbackOps[reason].Add(row.Ops)
		}
	}
	if row.Spans > 0 {
		db.orderedRootSpanNativeFallbackSpans[reason].Add(row.Spans)
		if routeCounters != nil {
			routeCounters.fallbackSpans[reason].Add(row.Spans)
		}
	}
}

func (db *DB) orderedRootSpanNativeRouteCountersFor(route OrderedRootSpanNativeRoute) *orderedRootSpanNativeRouteCounters {
	idx, ok := orderedRootSpanNativeRouteIndex(route)
	if db == nil || !ok {
		return nil
	}
	return &db.orderedRootSpanNativeRouteCounters[idx]
}

func (db *DB) observeOrderedRootSpanNativeApplyResult(route OrderedRootSpanNativeRoute, context string, result zipper.ApplyResult, err error, fallbackReason string) {
	if db == nil || !result.ReadOnlyPrepareRequested {
		return
	}
	summary := result.ReadOnlyPrepare.LeafSpanSummary()
	explicitFallbackReason := result.SpanNativeFallbackReason
	if explicitFallbackReason == "" && !orderedRootSpanNativeApplyPrepareFailed(result) && orderedRootSpanNativeApplyPreparedCandidate(route, summary) {
		explicitFallbackReason = fallbackReason
	}
	row := db.orderedRootSpanNativeEligibility(orderedRootSpanNativeEligibilityRequest{
		Route:                         route,
		Context:                       context,
		Summary:                       summary,
		ReadOnlyPrepareValidationFail: result.ReadOnlyPrepareValidationFailed,
		ReadOnlyPrepareFailed:         result.ReadOnlyPrepareFailed,
		Err:                           err,
		ExplicitFallbackReason:        explicitFallbackReason,
		SpanNativeEligible:            result.SpanNativeEligible,
		SpanNativeUsed:                result.SpanNativeUsed,
	})
	db.observeOrderedRootSpanNativeEligibility(row)
}

func orderedRootSpanNativeApplyPrepareFailed(result zipper.ApplyResult) bool {
	return result.ReadOnlyPrepareValidationFailed || result.ReadOnlyPrepareFailed
}

func orderedRootSpanNativeApplyPreparedCandidate(route OrderedRootSpanNativeRoute, summary zipper.ReadOnlyLeafSpanSummary) bool {
	ops, _ := orderedRootSpanNativeOpsAndSpans(summary, 0)
	return orderedRootSpanNativeCandidate(route, summary, ops)
}

func (db *DB) observeOrderedRootSpanNativeReadOnlyPrepare(summary zipper.ReadOnlyLeafSpanSummary, deltaOps int, err error, validationFailed bool, opts orderedRootPublishOptions) {
	if db == nil {
		return
	}
	applyOpts := db.orderedRootDeltaBatchApplyOptions(opts)
	spanNativeEligible := err == nil && !validationFailed && applyOpts.SpanNativeApply
	explicitFallbackReason := ""
	if err == nil && !validationFailed && !spanNativeEligible {
		explicitFallbackReason = FlushSpanRunFallbackSpanNativeNotImplemented.String()
	}
	row := db.orderedRootSpanNativeEligibility(orderedRootSpanNativeEligibilityRequest{
		Route:                         OrderedRootSpanNativeRouteReadOnlyPrepare,
		Context:                       "ordered-root read-only prepare proof",
		Summary:                       summary,
		DeltaOps:                      deltaOps,
		ReadOnlyPrepareValidationFail: validationFailed,
		ReadOnlyPrepareFailed:         err != nil,
		Err:                           err,
		ExplicitFallbackReason:        explicitFallbackReason,
		SpanNativeEligible:            spanNativeEligible,
	})
	db.observeOrderedRootSpanNativeEligibility(row)
}

func (db *DB) appendOrderedRootSpanNativeStats(stats map[string]string) {
	if db == nil || stats == nil {
		return
	}
	prefix := "treedb.publish.ordered_root_delta_group.span_native."
	stats[prefix+"candidate_ops_total"] = fmt.Sprintf("%d", db.orderedRootSpanNativeCandidateOps.Load())
	stats[prefix+"candidate_spans_total"] = fmt.Sprintf("%d", db.orderedRootSpanNativeCandidateSpans.Load())
	stats[prefix+"eligible_ops_total"] = fmt.Sprintf("%d", db.orderedRootSpanNativeEligibleOps.Load())
	stats[prefix+"eligible_spans_total"] = fmt.Sprintf("%d", db.orderedRootSpanNativeEligibleSpans.Load())
	stats[prefix+"used_ops_total"] = fmt.Sprintf("%d", db.orderedRootSpanNativeUsedOps.Load())
	stats[prefix+"used_spans_total"] = fmt.Sprintf("%d", db.orderedRootSpanNativeUsedSpans.Load())
	stats[prefix+"ineligible_ops_total"] = fmt.Sprintf("%d", db.orderedRootSpanNativeIneligibleOps.Load())
	stats[prefix+"ineligible_spans_total"] = fmt.Sprintf("%d", db.orderedRootSpanNativeIneligibleSpans.Load())
	stats[prefix+"fallbacks_total"] = fmt.Sprintf("%d", db.orderedRootSpanNativeFallbacks.Load())
	for _, reason := range FlushSpanRunFallbackReasons() {
		name := reason.String()
		stats[prefix+"fallback.reason."+name+".count_total"] = fmt.Sprintf("%d", db.orderedRootSpanNativeFallbackReasonCounts[reason].Load())
		stats[prefix+"fallback.reason."+name+".ops_total"] = fmt.Sprintf("%d", db.orderedRootSpanNativeFallbackOps[reason].Load())
		stats[prefix+"fallback.reason."+name+".spans_total"] = fmt.Sprintf("%d", db.orderedRootSpanNativeFallbackSpans[reason].Load())
	}
	for _, route := range orderedRootSpanNativeRoutes() {
		routeCounters := db.orderedRootSpanNativeRouteCountersFor(route)
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
	for _, row := range db.OrderedRootSpanNativeTriageSnapshot() {
		routePrefix := prefix + "triage.route." + string(row.Route) + "."
		stats[routePrefix+"context"] = row.Context
		stats[routePrefix+"status"] = string(row.Status)
		stats[routePrefix+"candidate"] = fmt.Sprintf("%t", row.Candidate)
		stats[routePrefix+"eligible"] = fmt.Sprintf("%t", row.Eligible)
		stats[routePrefix+"used"] = fmt.Sprintf("%t", row.Used)
		stats[routePrefix+"fallback_reason"] = row.FallbackReason
		stats[routePrefix+"fallback_class"] = string(row.FallbackClass)
		stats[routePrefix+"admission_policy"] = row.AdmissionPolicy
		stats[routePrefix+"admission_admitted"] = fmt.Sprintf("%t", row.AdmissionAdmitted)
		stats[routePrefix+"admission_reason"] = row.AdmissionReason
		stats[routePrefix+"selected_workers"] = fmt.Sprintf("%d", row.SelectedWorkers)
		stats[routePrefix+"detail"] = row.RouteSupportDetail
	}
}
