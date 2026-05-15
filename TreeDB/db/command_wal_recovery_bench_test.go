package db

import "testing"

func BenchmarkCommandWALRawKVDirectWrite(b *testing.B) {
	for _, tc := range []struct {
		name       string
		commandWAL bool
	}{
		{name: "backend_no_command_wal", commandWAL: false},
		{name: "command_wal_raw_kv", commandWAL: true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			dir := b.TempDir()
			db, err := Open(Options{Dir: dir, CommandWAL: tc.commandWAL})
			if err != nil {
				b.Fatalf("Open: %v", err)
			}
			defer db.Close()
			key := []byte("bench-key")
			value := []byte("bench-value-0123456789-0123456789-0123456789")
			b.ReportAllocs()
			b.SetBytes(int64(len(key) + len(value)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				batch := db.NewBatch()
				if err := batch.Set(key, value); err != nil {
					b.Fatalf("Set: %v", err)
				}
				if err := batch.Write(); err != nil {
					b.Fatalf("Write: %v", err)
				}
				if err := batch.Close(); err != nil {
					b.Fatalf("Close batch: %v", err)
				}
			}
		})
	}
}
