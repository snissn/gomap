package db

import "testing"

func TestHardwareAwareDefaultFlushApplyConcurrency(t *testing.T) {
	cases := []struct {
		name       string
		configured int
		gomax      int
		physical   int
		want       int
	}{
		{name: "default_uses_physical_under_gomax", gomax: 16, physical: 6, want: 6},
		{name: "default_caps_to_gomax", gomax: 4, physical: 6, want: 4},
		{name: "default_caps_to_upper_bound", gomax: 32, physical: 16, want: 8},
		{name: "default_unknown_physical_uses_existing_cap", gomax: 16, physical: 0, want: 8},
		{name: "configured_override_authoritative", configured: 16, gomax: 16, physical: 6, want: 16},
		{name: "configured_override_caps_to_gomax", configured: 16, gomax: 12, physical: 6, want: 12},
		{name: "configured_c1_disables_effective_pool", configured: 1, gomax: 16, physical: 6, want: 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := autoFlushApplyConcurrencyForHardware(tc.configured, tc.gomax, tc.physical); got != tc.want {
				t.Fatalf("autoFlushApplyConcurrencyForHardware(configured=%d,gomax=%d,physical=%d)=%d want %d", tc.configured, tc.gomax, tc.physical, got, tc.want)
			}
		})
	}
}

func TestResolveJournalLaneDefaultsCoalescingSafe(t *testing.T) {
	cases := []struct {
		name       string
		configured int
		gomax      int
		physical   int
		policy     ValueLogGenerationPolicy
		want       JournalLaneDefaultDecision
	}{
		{
			name:     "hot_warm_cold_six_physical_sixteen_gomax",
			gomax:    16,
			physical: 6,
			policy:   ValueLogGenerationHotWarmCold,
			want:     JournalLaneDefaultDecision{Effective: 3, Defaulted: true, HotLanes: 1, WarmLanes: 1, ColdLanes: 1},
		},
		{
			name:     "hot_warm_cold_unknown_physical_stays_safe",
			gomax:    16,
			physical: 0,
			policy:   ValueLogGenerationHotWarmCold,
			want:     JournalLaneDefaultDecision{Effective: 3, Defaulted: true, HotLanes: 1, WarmLanes: 1, ColdLanes: 1},
		},
		{
			name:     "generation_default_uses_hot_warm_cold_shape",
			gomax:    12,
			physical: 6,
			policy:   ValueLogGenerationDefault,
			want:     JournalLaneDefaultDecision{Effective: 3, Defaulted: true, HotLanes: 1, WarmLanes: 1, ColdLanes: 1},
		},
		{
			name:     "generation_off_defaults_single_hot_lane",
			gomax:    16,
			physical: 6,
			policy:   ValueLogGenerationOff,
			want:     JournalLaneDefaultDecision{Effective: 1, Defaulted: true, HotLanes: 1},
		},
		{
			name:       "explicit_journal_lanes_authoritative",
			configured: 6,
			gomax:      16,
			physical:   6,
			policy:     ValueLogGenerationHotWarmCold,
			want:       JournalLaneDefaultDecision{Configured: 6, Effective: 6, Defaulted: false, HotLanes: 4, WarmLanes: 1, ColdLanes: 1},
		},
		{
			name:       "explicit_low_lane_count_with_generation_remains_authoritative",
			configured: 1,
			gomax:      16,
			physical:   6,
			policy:     ValueLogGenerationHotWarmCold,
			want:       JournalLaneDefaultDecision{Configured: 1, Effective: 1, Defaulted: false, HotLanes: 1},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ResolveJournalLaneDefaults(tc.configured, tc.gomax, tc.physical, tc.policy)
			if got.Configured != tc.want.Configured || got.Effective != tc.want.Effective || got.Defaulted != tc.want.Defaulted || got.HotLanes != tc.want.HotLanes || got.WarmLanes != tc.want.WarmLanes || got.ColdLanes != tc.want.ColdLanes {
				t.Fatalf("ResolveJournalLaneDefaults=%+v want configured/effective/defaulted/hot/warm/cold=%d/%d/%t/%d/%d/%d", got, tc.want.Configured, tc.want.Effective, tc.want.Defaulted, tc.want.HotLanes, tc.want.WarmLanes, tc.want.ColdLanes)
			}
			if got.GOMAXPROCS != tc.gomax || got.PhysicalCores != tc.physical || got.GenerationPolicy != tc.policy {
				t.Fatalf("hardware/policy fields=%d/%d/%d want %d/%d/%d", got.GOMAXPROCS, got.PhysicalCores, got.GenerationPolicy, tc.gomax, tc.physical, tc.policy)
			}
		})
	}
}
