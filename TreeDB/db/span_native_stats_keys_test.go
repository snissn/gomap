package db

import (
	"crypto/sha256"
	"fmt"
	"sort"
	"sync"
	"testing"
)

// Digests characterize all keys and values from the original implementation.
func TestSpanNativeStatsStable(t *testing.T) {
	for _, populated := range []bool{false, true} {
		db := &DB{}
		if populated {
			db.rawSpanNativeCandidateOps.Store(1000)
			db.rawSpanNativeCandidateSpans.Store(1001)
			db.rawSpanNativeEligibleOps.Store(1002)
			db.rawSpanNativeEligibleSpans.Store(1003)
			db.rawSpanNativeUsedOps.Store(1004)
			db.rawSpanNativeUsedSpans.Store(1005)
			db.rawSpanNativeIneligibleOps.Store(1006)
			db.rawSpanNativeIneligibleSpans.Store(1007)
			db.rawSpanNativeFallbacks.Store(1008)
			db.orderedRootSpanNativeCandidateOps.Store(1000)
			db.orderedRootSpanNativeCandidateSpans.Store(1001)
			db.orderedRootSpanNativeEligibleOps.Store(1002)
			db.orderedRootSpanNativeEligibleSpans.Store(1003)
			db.orderedRootSpanNativeUsedOps.Store(1004)
			db.orderedRootSpanNativeUsedSpans.Store(1005)
			db.orderedRootSpanNativeIneligibleOps.Store(1006)
			db.orderedRootSpanNativeIneligibleSpans.Store(1007)
			db.orderedRootSpanNativeFallbacks.Store(1008)
			for _, reason := range FlushSpanRunFallbackReasons() {
				i := uint64(reason) + 1
				db.rawSpanNativeFallbackReasonCounts[reason].Store(i)
				db.rawSpanNativeFallbackOps[reason].Store(i + 100)
				db.rawSpanNativeFallbackSpans[reason].Store(i + 200)
				db.orderedRootSpanNativeFallbackReasonCounts[reason].Store(i + 300)
				db.orderedRootSpanNativeFallbackOps[reason].Store(i + 400)
				db.orderedRootSpanNativeFallbackSpans[reason].Store(i + 500)
			}
			for i := range db.rawSpanNativeRouteCounters {
				c := &db.rawSpanNativeRouteCounters[i]
				c.observations.Store(uint64(i + 1))
				c.candidateOps.Store(uint64(i + 11))
				c.candidateSpans.Store(uint64(i + 21))
				c.eligibleOps.Store(uint64(i + 31))
				c.eligibleSpans.Store(uint64(i + 41))
				c.usedOps.Store(uint64(i + 51))
				c.usedSpans.Store(uint64(i + 61))
				c.ineligibleOps.Store(uint64(i + 71))
				c.ineligibleSpans.Store(uint64(i + 81))
				c.fallbacks.Store(uint64(i + 91))
				for _, reason := range FlushSpanRunFallbackReasons() {
					c.fallbackReasonCounts[reason].Store(uint64(i)*100 + uint64(reason) + 1)
					c.fallbackOps[reason].Store(uint64(i)*100 + uint64(reason) + 101)
					c.fallbackSpans[reason].Store(uint64(i)*100 + uint64(reason) + 201)
				}
			}
			for i := range db.orderedRootSpanNativeRouteCounters {
				c := &db.orderedRootSpanNativeRouteCounters[i]
				c.observations.Store(uint64(i + 1))
				c.candidateOps.Store(uint64(i + 11))
				c.candidateSpans.Store(uint64(i + 21))
				c.eligibleOps.Store(uint64(i + 31))
				c.eligibleSpans.Store(uint64(i + 41))
				c.usedOps.Store(uint64(i + 51))
				c.usedSpans.Store(uint64(i + 61))
				c.ineligibleOps.Store(uint64(i + 71))
				c.ineligibleSpans.Store(uint64(i + 81))
				c.fallbacks.Store(uint64(i + 91))
				for _, reason := range FlushSpanRunFallbackReasons() {
					c.fallbackReasonCounts[reason].Store(uint64(i)*100 + uint64(reason) + 1)
					c.fallbackOps[reason].Store(uint64(i)*100 + uint64(reason) + 101)
					c.fallbackSpans[reason].Store(uint64(i)*100 + uint64(reason) + 201)
				}
			}
		}
		stats := map[string]string{}
		db.appendRawSpanNativeStats(stats)
		db.appendOrderedRootSpanNativeStats(stats)
		keys := make([]string, 0, len(stats))
		for k := range stats {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		h := sha256.New()
		for _, k := range keys {
			fmt.Fprintf(h, "%s\x00%s\x00", k, stats[k])
		}
		want := "67018334b2aace81c2a0e178145e28f3bd599359180cf1705059c63f1fe076c6"
		if populated {
			want = "963cac36e8f44b7508a7f632b986070116306efccf2ee484ab7ec69e4a8d7d53"
		}
		if got := fmt.Sprintf("%x", h.Sum(nil)); len(stats) != 1420 || got != want {
			t.Fatalf("populated=%v entries=%d digest=%s want 1420 and %s", populated, len(stats), got, want)
		}
	}
}

func TestSpanNativeStatsConcurrentAndIndependent(t *testing.T) {
	db := &DB{}
	const key = "treedb.raw.span_native.candidate_ops_total"
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				db.rawSpanNativeCandidateOps.Add(1)
				stats := map[string]string{}
				db.appendRawSpanNativeStats(stats)
				db.appendOrderedRootSpanNativeStats(stats)
				if stats[key] == "caller-owned" {
					t.Error("maps shared")
				}
			}
		}()
	}
	wg.Wait()
	first := map[string]string{}
	db.appendRawSpanNativeStats(first)
	first[key] = "caller-owned"
	second := map[string]string{}
	db.appendRawSpanNativeStats(second)
	if second[key] == "caller-owned" {
		t.Fatal("caller maps shared")
	}
	if first[key] != "caller-owned" {
		t.Fatal("caller map changed")
	}
}

func TestSpanNativeUnknownRouteKeys(t *testing.T) {
	const prefix = "treedb.test."
	const route = "future_route"
	names := newSpanNativeStatKeys(prefix, nil, true)
	keys := names.route(prefix, route)
	for i, suffix := range spanNativeTotalSuffixes {
		if got, want := keys.totals[i], prefix+"route."+route+"."+suffix; got != want {
			t.Errorf("total %d: got %q want %q", i, got, want)
		}
	}
	for _, reason := range FlushSpanRunFallbackReasons() {
		for i, suffix := range spanNativeReasonSuffixes {
			if got, want := keys.reasons[reason][i], prefix+"route."+route+".fallback.reason."+reason.String()+"."+suffix; got != want {
				t.Errorf("reason %s %d: got %q want %q", reason, i, got, want)
			}
		}
	}
	for i, suffix := range spanNativeTriageSuffixes {
		if got, want := keys.triage[i], prefix+"triage.route."+route+"."+suffix; got != want {
			t.Errorf("triage %d: got %q want %q", i, got, want)
		}
	}
}

func BenchmarkSpanNativeStatKeyInitialization(b *testing.B) {
	rawRoutes := make([]string, 0, len(rawSpanNativeRoutes()))
	for _, route := range rawSpanNativeRoutes() {
		rawRoutes = append(rawRoutes, string(route))
	}
	orderedRoutes := make([]string, 0, len(orderedRootSpanNativeRoutes()))
	for _, route := range orderedRootSpanNativeRoutes() {
		orderedRoutes = append(orderedRoutes, string(route))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		raw := newSpanNativeStatKeys("treedb.raw.span_native.", rawRoutes, false)
		ordered := newSpanNativeStatKeys("treedb.publish.ordered_root_delta_group.span_native.", orderedRoutes, true)
		if len(raw.routes)+len(ordered.routes) != len(rawRoutes)+len(orderedRoutes) {
			b.Fatal("missing routes")
		}
	}
}
