package db

import (
	"fmt"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
)

func TestVacuumRecorder_DoesNotRecordUncommittedWrites(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() {
		_ = d.Close()
	}()

	d.vacuum.Start()
	defer d.vacuum.Stop()

	// Block root reads/commit so a batch write can't complete. If the vacuum
	// recorder captures keys before commit, it can be drained while the write is
	// still in-flight, causing online vacuum to miss the eventual update.
	d.mu.Lock()

	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		b := d.NewBatch()
		_ = b.Set([]byte("k"), []byte("v"))
		done <- b.Write()
		_ = b.Close()
	}()

	<-started

	// Give the write goroutine time to start and (if buggy) record its keys, but
	// ensure the recorder stays empty while the write is blocked.
	deadline := time.Now().Add(250 * time.Millisecond)
	for time.Now().Before(deadline) {
		if keys := d.vacuum.Drain(); len(keys) > 0 {
			t.Fatalf("vacuum recorded keys before commit: %v", keys)
		}
		time.Sleep(2 * time.Millisecond)
	}

	d.mu.Unlock()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("write: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for write to complete")
	}

	keys := d.vacuum.Drain()
	if _, ok := keys["k"]; !ok {
		t.Fatalf("expected committed key to be recorded, got: %v", keys)
	}
}

func TestVacuumRecorder_RecordEntries_LastWriteWinsAndCopies(t *testing.T) {
	var r vacuumRecorder
	r.Start()
	defer r.Stop()

	key := []byte("k")
	v1 := []byte("v1")
	v2 := []byte("v2")
	entries := []batch.Entry{
		{Type: batch.OpPut, Key: key, Value: v1},
		{Type: batch.OpPut, Key: key, Value: v2},
	}
	r.RecordEntries(entries)

	// Mutate sources after recording; recorder must keep independent copies.
	key[0] = 'x'
	v1[0] = 'x'
	v2[0] = 'x'

	ops := r.Drain()
	if len(ops) != 1 {
		t.Fatalf("drain len=%d want=1 (ops=%v)", len(ops), ops)
	}
	got, ok := ops["k"]
	if !ok {
		t.Fatalf("missing key k in drained ops: %v", ops)
	}
	if got.Type != batch.OpPut || got.IsPtr {
		t.Fatalf("unexpected drained entry metadata: %+v", got)
	}
	if string(got.Key) != "k" {
		t.Fatalf("drained key=%q want=%q", string(got.Key), "k")
	}
	if string(got.Value) != "v2" {
		t.Fatalf("drained value=%q want=%q", string(got.Value), "v2")
	}
}

func BenchmarkVacuumRecorder_RecordPath(b *testing.B) {
	const n = 512
	entries := make([]batch.Entry, 0, n)
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		val := []byte(fmt.Sprintf("value-%04d", i))
		entries = append(entries, batch.Entry{
			Type:  batch.OpPut,
			Key:   key,
			Value: val,
		})
	}

	b.Run("OldPath_RecordOpsFromMap", func(b *testing.B) {
		var r vacuumRecorder
		r.Start()
		defer r.Stop()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ops := make(map[string]batch.Entry, len(entries))
			for j := range entries {
				ops[string(entries[j].Key)] = entries[j]
			}
			r.RecordOps(ops)
			_ = r.Drain()
		}
	})

	b.Run("NewPath_RecordEntries", func(b *testing.B) {
		var r vacuumRecorder
		r.Start()
		defer r.Stop()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			r.RecordEntries(entries)
			_ = r.Drain()
		}
	})
}
