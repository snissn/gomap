package main

import (
	"strings"
	"testing"
)

func TestRenderMongoFullSweepSkipsEmptyScalingForLoadOnlyIndexes(t *testing.T) {
	rows := []mongoSummaryRow{
		{Documents: 100000, SecondaryIndexes: 0, Phase: "load_insert_many", TreeDBOpsSec: 500000, MongoOpsSec: 300000},
		{Documents: 100000, SecondaryIndexes: 0, Phase: "id_find_one", TreeDBOpsSec: 25000, MongoOpsSec: 15000},
		{Documents: 100000, SecondaryIndexes: 2, Phase: "load_insert_many", TreeDBOpsSec: 400000, MongoOpsSec: 250000},
		{Documents: 100000, SecondaryIndexes: 2, Phase: "id_find_one", TreeDBOpsSec: 24000, MongoOpsSec: 14000},
		{Documents: 100000, SecondaryIndexes: 4, Phase: "load_insert_many", TreeDBOpsSec: 350000, MongoOpsSec: 180000},
		{Documents: 100000, SecondaryIndexes: 32, Phase: "load_insert_many", TreeDBOpsSec: 120000, MongoOpsSec: 40000},
	}
	var b strings.Builder
	renderMongoFullSweep(&b, rows)
	html := b.String()

	lowerHTML := strings.ToLower(html)
	// Overview charts & retention table include all index counts 0, 2, 4, 32.
	for _, want := range []string{
		"insert throughput vs secondary indexes",
		"storage footprint vs secondary indexes",
		"0 indexes: mongo api scaling",
		"2 indexes: mongo api scaling",
		">32<",
	} {
		if !strings.Contains(lowerHTML, want) {
			t.Fatalf("expected HTML to contain %q", want)
		}
	}
	// Load-only index counts (4, 32) must NOT emit empty Mongo API Scaling subsections.
	for _, unwanted := range []string{
		"4 indexes: mongo api scaling",
		"32 indexes: mongo api scaling",
	} {
		if strings.Contains(lowerHTML, unwanted) {
			t.Fatalf("unexpected empty scaling section %q in HTML", unwanted)
		}
	}
}
