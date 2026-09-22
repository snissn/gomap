package treedb

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func BenchmarkCachedOpenInsertClose(b *testing.B) {
	const insertsPerCycle = 5000
	const valueSize = 64

	keys := make([][]byte, insertsPerCycle)
	vals := make([][]byte, insertsPerCycle)
	for i := 0; i < insertsPerCycle; i++ {
		key := make([]byte, 8)
		val := make([]byte, valueSize)
		binary.LittleEndian.PutUint64(key, uint64(i))
		binary.LittleEndian.PutUint64(val, uint64(i))
		keys[i] = key
		vals[i] = val
	}

	baseDir, err := os.MkdirTemp("", "treedb-open-close-bench-")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(baseDir)

	var openDur time.Duration
	var insertDur time.Duration
	var closeDur time.Duration

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir := filepath.Join(baseDir, strconv.Itoa(i))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()

		start := time.Now()
		db, err := Open(Options{Dir: dir})
		openDur += time.Since(start)
		if err != nil {
			b.Fatal(err)
		}

		start = time.Now()
		for j := 0; j < insertsPerCycle; j++ {
			if err := db.Set(keys[j], vals[j]); err != nil {
				_ = db.Close()
				b.Fatal(err)
			}
		}
		insertDur += time.Since(start)

		start = time.Now()
		if err := db.Close(); err != nil {
			b.Fatal(err)
		}
		closeDur += time.Since(start)

		b.StopTimer()
		if err := os.RemoveAll(dir); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
	}

	b.ReportMetric(float64(openDur.Nanoseconds())/float64(b.N), "ns/open")
	b.ReportMetric(float64(insertDur.Nanoseconds())/float64(b.N), "ns/insert")
	b.ReportMetric(float64(closeDur.Nanoseconds())/float64(b.N), "ns/close")
}
