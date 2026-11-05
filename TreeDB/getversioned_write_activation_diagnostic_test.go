//go:build treedb_activation_diagnostic

package treedb

import (
	"bytes"
	"fmt"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// TestGetVersionedWriteActivationDiagnostic uses overlay-only counters. Their
// observations include background work inside the interval and make no timing
// or original-binary activation claim.
func TestGetVersionedWriteActivationDiagnostic(t *testing.T) {
	t.Run("durable_write_window", func(t *testing.T) {
		opts := commandWALDurabilityProofOptions(t.TempDir())
		opts.PublicBatchWriteSyncPhaseStats = true
		db, err := Open(opts)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		closed := false
		t.Cleanup(func() {
			if !closed {
				if err := db.Close(); err != nil {
					t.Errorf("cleanup Close: %v", err)
				}
			}
		})
		before := db.Stats()
		backenddb.ResetGetVersionedActivationDiagnostic()
		value := []byte("public-command-wal-value")
		for i := 0; i < 32; i++ {
			batch := db.NewBatchWithSize(1)
			if err := batch.Set([]byte(fmt.Sprintf("dirty/%d", i)), value); err != nil {
				if closeErr := batch.Close(); closeErr != nil {
					t.Errorf("batch Close after Set %d: %v", i, closeErr)
				}
				t.Fatalf("Set %d: %v", i, err)
			}
			if err := batch.WriteSync(); err != nil {
				if closeErr := batch.Close(); closeErr != nil {
					t.Errorf("batch Close after WriteSync %d: %v", i, closeErr)
				}
				t.Fatalf("WriteSync %d: %v", i, err)
			}
			if err := batch.Close(); err != nil {
				t.Fatalf("batch Close %d: %v", i, err)
			}
			acquire, reset := backenddb.ReadGetVersionedActivationDiagnostic()
			t.Logf("write=%d acquire=%d bound_reset=%d", i, acquire, reset)
		}
		after := db.Stats()
		for key, want := range publicCommandWALDurableShapeExpectedCounters("dirty_batch", false, DurabilityDurable) {
			requirePublicStatDelta(t, before, after, key, want*32)
		}
		requirePublicStatDelta(t, before, after, "treedb.public.checkpoint.calls_total", 0)
		requirePublicStatDelta(t, before, after, "treedb.cache.checkpoint.runs", 0)
		if err := db.Close(); err != nil {
			closed = true
			t.Fatalf("Close: %v", err)
		}
		closed = true
		reopen, err := Open(opts)
		if err != nil {
			t.Fatalf("reopen: %v", err)
		}
		t.Cleanup(func() {
			if err := reopen.Close(); err != nil {
				t.Errorf("reopen Close: %v", err)
			}
		})
		for i := 0; i < 32; i++ {
			got, revision, err := reopen.GetVersioned([]byte(fmt.Sprintf("dirty/%d", i)))
			if err != nil || !bytes.Equal(got, value) {
				t.Fatalf("reopen GetVersioned %d: value=%q revision=%v err=%v", i, got, revision, err)
			}
			again, againRevision, err := reopen.GetVersioned([]byte(fmt.Sprintf("dirty/%d", i)))
			if err != nil || !bytes.Equal(again, got) || againRevision != revision {
				t.Fatalf("repeat GetVersioned %d: value=%q revision=%v err=%v", i, again, againRevision, err)
			}
		}
	})
	t.Run("fresh_backend_positive", func(t *testing.T) {
		db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
		if err != nil {
			t.Fatalf("backend Open: %v", err)
		}
		t.Cleanup(func() {
			if err := db.Close(); err != nil {
				t.Errorf("backend Close: %v", err)
			}
		})
		backenddb.ResetGetVersionedActivationDiagnostic()
		value, _, err := db.GetVersioned([]byte("missing"))
		acquire, reset := backenddb.ReadGetVersionedActivationDiagnostic()
		t.Logf("positive acquire=%d bound_reset=%d", acquire, reset)
		if err != nil || value != nil || acquire == 0 || reset == 0 {
			t.Fatalf("positive value=%q err=%v acquire=%d reset=%d", value, err, acquire, reset)
		}
	})
}
