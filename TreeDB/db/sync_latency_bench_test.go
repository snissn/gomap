package db

import (
	"encoding/binary"
	"os"
	"testing"
)

func BenchmarkSyncLatency(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "treedb-bench-sync-backend-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	d, err := Open(Options{Dir: tmpDir})
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer d.Close()

	val := make([]byte, 128)
	var key [8]byte

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(key[:], uint64(i))
		if err := d.SetSync(key[:], val); err != nil {
			b.Fatalf("setsync: %v", err)
		}
	}
}
