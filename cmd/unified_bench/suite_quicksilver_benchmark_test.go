package main

import "testing"

// Runs against both historical source and the instrumented source with an
// allocation-free adapter, to disclose the cost added to the original harness.
func BenchmarkQuicksilverLegacyReadHarness(b *testing.B) {
	c := quicksilverSmokeConfig()
	c.Keys = 100000
	c.Reads = 100000
	c.Workers = 4
	c.ReadBatch = 1
	f := newQuicksilverFixture(c)
	d := &fixedNameDB{name: "misses"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p, e := quicksilverReadPhase(d, c, f, 1, nil, nil, nil)
		if e != nil {
			b.Fatal(e)
		}
		b.ReportMetric(p.Seconds*1e9/float64(p.Ops), "reader-ns/read")
		b.ReportMetric(p.BytesPerOp, "reader-B/read")
		b.ReportMetric(p.AllocsPerOp, "reader-allocs/read")
	}
}

func BenchmarkQuicksilverRealisticReadHarness(b *testing.B) {
	c := quicksilverRealisticSmokeConfig()
	c.Keys = 1000000
	c.Reads = 100000
	c.Workers = 4
	c.ReadBatch = 1
	f := newQuicksilverFixture(c)
	d := &fixedNameDB{name: "misses"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p, e := quicksilverReadPhase(d, c, f, 1, nil, nil, nil)
		if e != nil {
			b.Fatal(e)
		}
		b.ReportMetric(p.Seconds*1e9/float64(p.Ops), "reader-ns/read")
		b.ReportMetric(p.BytesPerOp, "reader-B/read")
		b.ReportMetric(p.AllocsPerOp, "reader-allocs/read")
	}
	b.ReportMetric(float64(((c.Keys*5+63)/64)*8*(c.Workers+1)+c.Keys+quicksilverSampleLimit*8), "fixture-B")
}
