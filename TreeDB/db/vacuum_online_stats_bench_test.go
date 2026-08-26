package db

import "testing"

var benchmarkVacuumOnlineStatsSink VacuumOnlineStats

func BenchmarkVacuumOnlineStatsSnapshot(b *testing.B) {
	database := &DB{}
	database.vacuumOnlineLast.Store(&VacuumOnlineStats{})
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		benchmarkVacuumOnlineStatsSink = database.vacuumOnlineStatsSnapshot()
	}
}
