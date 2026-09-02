package main

import (
	"testing"
	"time"
)

func TestCheckpointCutoverDurationFromStats(t *testing.T) {
	tests := []struct {
		name     string
		stats    map[string]string
		want     time.Duration
		wantUsed bool
	}{
		{
			name: "missing stats",
		},
		{
			name: "last ms without evidence falls back",
			stats: map[string]string{
				"treedb.cache.checkpoint.cutover_last_ms": "0.000",
			},
		},
		{
			name: "zero samples and zero timestamp falls back",
			stats: map[string]string{
				"treedb.cache.checkpoint.cutover_last_ms":        "0.000",
				"treedb.cache.checkpoint.cutover_samples":        "0",
				"treedb.cache.checkpoint.cutover_last_unix_nano": "0",
			},
		},
		{
			name: "positive samples uses cutover last ms",
			stats: map[string]string{
				"treedb.cache.checkpoint.cutover_last_ms": "1.250",
				"treedb.cache.checkpoint.cutover_samples": "3",
			},
			want:     1250 * time.Microsecond,
			wantUsed: true,
		},
		{
			name: "last unix nano evidence uses cutover metric",
			stats: map[string]string{
				"treedb.cache.checkpoint.cutover_last_ms":        "2.500",
				"treedb.cache.checkpoint.cutover_last_unix_nano": "1738950000000000000",
			},
			want:     2500 * time.Microsecond,
			wantUsed: true,
		},
		{
			name: "sampled zero duration remains valid",
			stats: map[string]string{
				"treedb.cache.checkpoint.cutover_last_ms": "0.000",
				"treedb.cache.checkpoint.cutover_samples": "1",
			},
			want:     0,
			wantUsed: true,
		},
		{
			name: "negative cutover ignored",
			stats: map[string]string{
				"treedb.cache.checkpoint.cutover_last_ms": "-1.000",
				"treedb.cache.checkpoint.cutover_samples": "2",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, used := checkpointCutoverDurationFromStats(tt.stats)
			if used != tt.wantUsed {
				t.Fatalf("used = %v, want %v", used, tt.wantUsed)
			}
			if got != tt.want {
				t.Fatalf("duration = %v, want %v", got, tt.want)
			}
		})
	}
}
