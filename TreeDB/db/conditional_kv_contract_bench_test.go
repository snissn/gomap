package db

import "testing"

func BenchmarkGetVersioned(b *testing.B) {
	b.ReportAllocs()
	b.Skip("planned by #3424/#3425: replace with native versioned point-read benchmark once EntryRevision APIs land")
}

func BenchmarkConditionalTxnReadSet1(b *testing.B) {
	benchmarkConditionalTxnReadSetPlanned(b, 1)
}

func BenchmarkConditionalTxnReadSet10(b *testing.B) {
	benchmarkConditionalTxnReadSetPlanned(b, 10)
}

func BenchmarkConditionalTxnReadSet100(b *testing.B) {
	benchmarkConditionalTxnReadSetPlanned(b, 100)
}

func BenchmarkConditionalTxnReadSet10000(b *testing.B) {
	benchmarkConditionalTxnReadSetPlanned(b, 10000)
}

func benchmarkConditionalTxnReadSetPlanned(b *testing.B, readSet int) {
	b.Helper()
	b.ReportAllocs()
	b.Skipf("planned by #3424/#3425: replace with native conditional transaction benchmark for read set size %d", readSet)
}
