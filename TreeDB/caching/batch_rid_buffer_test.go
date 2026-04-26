package caching

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func testBatchRIDKey(i int) []byte {
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], uint64(i+1))
	return key[:]
}

func TestBatchWriteRegularRIDBufferOnlyForValueLogPointers(t *testing.T) {
	tests := []struct {
		name          string
		ptrThreshold  int
		wantRIDBuffer bool
	}{
		{
			name:          "inline_wal_records",
			ptrThreshold:  1 << 20,
			wantRIDBuffer: false,
		},
		{
			name:          "value_log_pointer_records",
			ptrThreshold:  1,
			wantRIDBuffer: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := Open(t.TempDir(), NewMockBackend(), Options{
				AllowUnsafe:              true,
				RelaxedSync:              true,
				MemtableMode:             "btree",
				MemtableShards:           1,
				FlushThreshold:           1 << 30,
				ValueLogPointerThreshold: tt.ptrThreshold,
			})
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer db.Close()

			b := db.NewBatch()
			value := bytes.Repeat([]byte{0x42}, 128)
			for i := 0; i < 4; i++ {
				if err := b.Set(testBatchRIDKey(i), value); err != nil {
					t.Fatalf("set %d: %v", i, err)
				}
			}
			if err := b.Write(); err != nil {
				t.Fatalf("write: %v", err)
			}

			if got := cap(b.ridBuf) > 0; got != tt.wantRIDBuffer {
				t.Fatalf("rid buffer allocated=%t want %t", got, tt.wantRIDBuffer)
			}
			if tt.wantRIDBuffer {
				firstCap := cap(b.ridBuf)
				for i := 0; i < 4; i++ {
					if err := b.Set(testBatchRIDKey(16+i), value); err != nil {
						t.Fatalf("second set %d: %v", i, err)
					}
				}
				if err := b.Write(); err != nil {
					t.Fatalf("second write: %v", err)
				}
				if got := cap(b.ridBuf); got != firstCap {
					t.Fatalf("rid buffer cap after reuse=%d want %d", got, firstCap)
				}
			}
			if err := b.Close(); err != nil {
				t.Fatalf("close batch: %v", err)
			}
		})
	}
}
