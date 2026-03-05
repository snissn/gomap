package caching

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/compression"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func TestValueLogDictCollectBudget_ScalesForTinyValues(t *testing.T) {
	db := &DB{}
	// Mirror caching.Open defaults: auto/dict modes train by default unless
	// explicitly disabled.
	db.valueLogDictTrain.TrainBytes = compression.DefaultTrainBytes

	cases := []struct {
		name      string
		valueSize int
		records   int
		want      int
	}{
		// ceil(128KiB/16B)=8192; old cap=2048 would under-collect and delay the
		// first dict publication for large-batch tiny-value workloads.
		{name: "16B", valueSize: 16, records: 9000, want: 8192},
		// ceil(128KiB/8B)=16384 hits the hard cap (and still bootstraps).
		{name: "8B", valueSize: 8, records: 20000, want: 16384},
		// Smaller than 8B would exceed the cap; ensure we still return the cap.
		{name: "4B_capped", valueSize: 4, records: 20000, want: 16384},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			records := make([]valuelog.Record, tc.records)
			value := make([]byte, tc.valueSize)
			for i := range records {
				records[i].Value = value
			}
			got := db.valueLogDictCollectBudget(records, false)
			if got != tc.want {
				t.Fatalf("valueLogDictCollectBudget(valueSize=%d, records=%d)=%d want %d",
					tc.valueSize, tc.records, got, tc.want)
			}
		})
	}
}
