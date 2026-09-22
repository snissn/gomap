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
		// ceil(32KiB/16B)=2048 keeps bootstrap work bounded while still training
		// quickly for tiny-value streams.
		{name: "16B", valueSize: 16, records: 9000, want: 2048},
		{name: "8B", valueSize: 8, records: 20000, want: 4096},
		{name: "4B", valueSize: 4, records: 20000, want: 8192},
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
