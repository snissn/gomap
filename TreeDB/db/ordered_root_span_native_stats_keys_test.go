package db

import (
	"fmt"
	"math"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
)

// The reference below is the exact emitter from 1cb7142a9, before key
// precomputation. Keep it independent of the precomputed key tables.
func appendOrderedRootSpanNativeStatsReference(db *DB, stats map[string]string) {
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

func orderedRootSpanNativeStatCounterPointers(d *DB) []*atomic.Uint64 {
	counters := []*atomic.Uint64{
		&d.orderedRootSpanNativeCandidateOps, &d.orderedRootSpanNativeCandidateSpans,
		&d.orderedRootSpanNativeEligibleOps, &d.orderedRootSpanNativeEligibleSpans,
		&d.orderedRootSpanNativeUsedOps, &d.orderedRootSpanNativeUsedSpans,
		&d.orderedRootSpanNativeIneligibleOps, &d.orderedRootSpanNativeIneligibleSpans,
		&d.orderedRootSpanNativeFallbacks,
	}
	for _, reason := range FlushSpanRunFallbackReasons() {
		counters = append(counters, &d.orderedRootSpanNativeFallbackReasonCounts[reason],
			&d.orderedRootSpanNativeFallbackOps[reason], &d.orderedRootSpanNativeFallbackSpans[reason])
	}
	for _, route := range orderedRootSpanNativeRoutes() {
		c := d.orderedRootSpanNativeRouteCountersFor(route)
		counters = append(counters, &c.observations, &c.candidateOps, &c.candidateSpans,
			&c.eligibleOps, &c.eligibleSpans, &c.usedOps, &c.usedSpans,
			&c.ineligibleOps, &c.ineligibleSpans, &c.fallbacks)
		for _, reason := range FlushSpanRunFallbackReasons() {
			counters = append(counters, &c.fallbackReasonCounts[reason], &c.fallbackOps[reason], &c.fallbackSpans[reason])
		}
	}
	return counters
}

func assertOrderedRootSpanNativeStatsParity(t *testing.T, d *DB) {
	t.Helper()
	want := map[string]string{"unrelated": "preserved"}
	got := map[string]string{"unrelated": "preserved"}
	appendOrderedRootSpanNativeStatsReference(d, want)
	d.appendOrderedRootSpanNativeStats(got)
	if !reflect.DeepEqual(got, want) {
		for key, value := range want {
			if actual, ok := got[key]; !ok || actual != value {
				t.Errorf("metric %q: got %q (present %t), want %q", key, actual, ok, value)
			}
		}
		for key := range got {
			if _, ok := want[key]; !ok {
				t.Errorf("unexpected metric %q", key)
			}
		}
		t.FailNow()
	}
	// All 9 routes, 21 fallback reasons (including unknown) and 12 triage
	// fields must remain visible even when their values are zero.
	if len(got) != 838 {
		t.Fatalf("metric count including sentinel = %d, want 838", len(got))
	}
}

func TestOrderedRootSpanNativeStatsPrecomputedKeysParity(t *testing.T) {
	if len(orderedRootSpanNativeRoutes()) != 9 || len(FlushSpanRunFallbackReasons()) != 21 {
		t.Fatal("update the independent frozen schema count when adding routes or reasons")
	}
	decisions := []struct {
		name      string
		admission FlushAdmissionDecision
	}{
		{"defaults", FlushAdmissionDecision{}},
		{"policy_off", FlushAdmissionDecision{Policy: FlushAdmissionPolicyOff}},
		{"declined", FlushAdmissionDecision{Policy: FlushAdmissionPolicyAuto, Reason: "fixture-decline"}},
		{"span_native_off", FlushAdmissionDecision{Policy: FlushAdmissionPolicyAuto, Admitted: true, FlushApplyConcurrency: 3}},
		{"span_native_on", FlushAdmissionDecision{Policy: FlushAdmissionPolicyAuto, Admitted: true, FlushApplySpanNative: true, FlushApplyConcurrency: 7, Reason: "fixture-admit"}},
	}
	for _, decision := range decisions {
		for _, pattern := range []string{"zero", "distinct", "max_uint64"} {
			t.Run(decision.name+"/"+pattern, func(t *testing.T) {
				d := &DB{flushAdmission: decision.admission}
				for index, counter := range orderedRootSpanNativeStatCounterPointers(d) {
					switch pattern {
					case "distinct":
						counter.Store(uint64(index + 1))
					case "max_uint64":
						counter.Store(math.MaxUint64 - uint64(index))
					}
				}
				assertOrderedRootSpanNativeStatsParity(t, d)
			})
		}
	}
}

func TestOrderedRootSpanNativeStatsPrecomputedKeysLiveAndOwned(t *testing.T) {
	d := &DB{}
	var absent *DB
	sentinel := map[string]string{"unrelated": "preserved"}
	absent.appendOrderedRootSpanNativeStats(sentinel)
	d.appendOrderedRootSpanNativeStats(nil)
	if !reflect.DeepEqual(sentinel, map[string]string{"unrelated": "preserved"}) {
		t.Fatal("nil DB changed map")
	}
	d.orderedRootSpanNativeCandidateOps.Store(11)
	first := make(map[string]string)
	d.appendOrderedRootSpanNativeStats(first)
	key := "treedb.publish.ordered_root_delta_group.span_native.candidate_ops_total"
	if first[key] != "11" {
		t.Fatalf("first live value %q", first[key])
	}
	first[key] = "caller mutation"
	first["caller-only"] = "owned"
	delete(first, "treedb.publish.ordered_root_delta_group.span_native.triage.route.direct_publish.status")
	d.orderedRootSpanNativeCandidateOps.Store(12)
	d.orderedRootSpanNativeFallbackOps[FlushSpanRunFallbackUnknown].Store(math.MaxUint64)
	for _, route := range orderedRootSpanNativeRoutes() {
		d.orderedRootSpanNativeRouteCountersFor(route).fallbackSpans[FlushSpanRunFallbackUnknown].Store(43)
	}
	second := make(map[string]string)
	d.appendOrderedRootSpanNativeStats(second)
	if second[key] != "12" || second["caller-only"] != "" || first[key] != "caller mutation" {
		t.Fatal("Stats cached values or shared caller-owned map state")
	}
	assertOrderedRootSpanNativeStatsParity(t, d)
	assertOrderedRootSpanNativeStatsParity(t, &DB{})
}

func TestOrderedRootSpanNativeStatsPrecomputedKeysConcurrent(t *testing.T) {
	d := &DB{flushAdmission: FlushAdmissionDecision{Policy: FlushAdmissionPolicyAuto, Admitted: true, FlushApplySpanNative: true}}
	counters := orderedRootSpanNativeStatCounterPointers(d)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for writer := 0; writer < 2; writer++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for round := 0; round < 100; round++ {
				for _, counter := range counters {
					counter.Add(1)
				}
			}
		}()
	}
	for reader := 0; reader < 4; reader++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for round := 0; round < 40; round++ {
				own := make(map[string]string)
				d.appendOrderedRootSpanNativeStats(own)
				if len(own) != 837 {
					t.Errorf("concurrent schema has %d metrics", len(own))
					return
				}
				own["caller-only"] = "local"
			}
		}()
	}
	close(start)
	wg.Wait()
	for index, counter := range counters {
		if got := counter.Load(); got != 200 {
			t.Fatalf("counter %d = %d, want 200", index, got)
		}
	}
	assertOrderedRootSpanNativeStatsParity(t, d)
}
