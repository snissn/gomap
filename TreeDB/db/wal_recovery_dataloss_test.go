package db

import (
	"bytes"
	"fmt"
	"testing"
)

func TestWALRecovery_LargeVlog_NoDataLoss(t *testing.T) {
	dir := t.TempDir()

	count := 15000 // Greater than maxOpsPerBatch (10,000)
	val := bytes.Repeat([]byte{0xEE}, 500)

	// Phase 1: Write data using caching layer with Vlogs
	{
		opts := Options{
			Dir:                      dir,
			FlushThreshold:           4 * 1024 * 1024,
			Mode:                     ModeCached,
			ValueLogPointerThreshold: 32,
			ForceValuePointers:       true,
			AllowUnsafe:              true,
		}

		db, err := Open(opts)
		if err != nil {
			t.Fatal(err)
		}

		batch := db.NewBatch()
		for i := 0; i < count; i++ {
			if err := batch.Set([]byte(fmt.Sprintf("key-%06d", i)), val); err != nil {
				t.Fatal(err)
			}
		}
		if err := batch.Write(); err != nil {
			t.Fatal(err)
		}

		// Hard close without checkpoint to leave data in WAL/Vlog
		db.Close()
	}

	// Phase 2: Reopen and verify data
	{
		opts := Options{
			Dir:                      dir,
			FlushThreshold:           4 * 1024 * 1024,
			Mode:                     ModeCached,
			ValueLogPointerThreshold: 32,
			ForceValuePointers:       true,
			AllowUnsafe:              true,
		}

		db, err := Open(opts)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		for i := 0; i < count; i++ {
			key := []byte(fmt.Sprintf("key-%06d", i))
			got, err := db.Get(key)
			if err != nil {
				t.Fatalf("Key %s missing after recovery: %v", string(key), err)
			}
			if !bytes.Equal(got, val) {
				t.Fatalf("Data mismatch for key %s", string(key))
			}
		}
	}
}
