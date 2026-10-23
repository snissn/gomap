package caching

import (
	"fmt"
	"runtime"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// Measure the actual public Stats method as a guard for the more expensive full
// coherent field copy. Keep real clocks and both normal telemetry samplers.
func BenchmarkPoolPressurePublicStats(b *testing.B) {
	dir := b.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		b.Fatal(err)
	}
	defer backend.Close()
	for i := 0; i < 64; i++ {
		if err := backend.SetSync([]byte(fmt.Sprintf("stats/%03d", i)), []byte("inline-value")); err != nil {
			b.Fatal(err)
		}
	}
	cached, err := Open(dir, backend, Options{FlushThreshold: 64 << 20})
	if err != nil {
		b.Fatal(err)
	}
	defer cached.Close()
	if got := cached.Stats()["treedb.process.memory.pool_pressure_level"]; got == "" {
		b.Fatal("public Stats did not include the pressure snapshot")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runtime.KeepAlive(cached.Stats())
	}
	b.StopTimer() // Deferred cached/backend Close is outside the Stats boundary.
}
