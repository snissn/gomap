package caching

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

// Observe the actual physical producers, rather than their shared logical lane.
func hotReconcileInventory(t *testing.T, db *DB) []leafReconcileFile {
	t.Helper()
	var result []leafReconcileFile
	for _, l := range db.nativeRootValueLogAppendLanesSnapshot() {
		l.vlogMu.Lock()
		id, err := valuelog.EncodeFileID(uint32(l.id), uint32(l.vlogSeq))
		path, rotations := l.vlogPath, l.vlogRotateTotal.Load()
		l.vlogMu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		identity, identityErr := rootpublication.StableIdentityFromFile(f)
		info, statErr := f.Stat()
		closeErr := f.Close()
		if err := errors.Join(identityErr, statErr, closeErr); err != nil {
			t.Fatal(err)
		}
		result = append(result, leafReconcileFile{path, id, identity, info.Size(), rotations})
	}
	return result
}

func openHotReconcileDB(t *testing.T) (*DB, *backenddb.DB) {
	t.Helper()
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir, Durability: backenddb.DurabilityDurable})
	if err != nil {
		t.Fatal(err)
	}
	db, err := Open(dir, backend, Options{JournalLanes: 3, ValueLogGenerationPolicy: uint8(backenddb.ValueLogGenerationHotWarmCold)})
	if err != nil {
		_ = backend.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
		if err := backend.Close(); err != nil {
			t.Error(err)
		}
	})
	return db, backend
}

func TestHotReconcilePreservesHealthyPhysicalWritersAfterNoWorkAndFailure(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("exact relative namespace inspection unsupported")
	}
	db, backend := openHotReconcileDB(t)
	lanes := db.nativeRootValueLogAppendLanesSnapshot()
	if len(lanes) < 2 {
		t.Fatal("real public owner did not install independent physical hot writers")
	}
	values := make([][]byte, len(lanes)*8)
	for i := range values {
		values[i] = bytes.Repeat([]byte{byte(i + 1)}, 1024+i)
	}
	ptrs, err := backend.AppendValueLogValues(values)
	if err != nil {
		t.Fatal(err)
	}
	for i, p := range ptrs {
		got, e := db.ReadValueLogRecord(p)
		if e != nil || !bytes.Equal(got, values[i]) {
			t.Fatalf("initial value %d: %v", i, e)
		}
	}
	before := hotReconcileInventory(t, db)
	failed := errors.New("backend maintenance refused")
	for _, fnErr := range []error{nil, failed, nil} {
		err := db.runWithBackendMaintenanceOptions(backendMaintenanceOptions{skipCheckpoint: true}, func() error { return fnErr })
		if !errors.Is(err, fnErr) {
			t.Fatalf("maintenance error=%v want %v", err, fnErr)
		}
		after := hotReconcileInventory(t, db)
		if len(after) != len(before) {
			t.Fatal("maintenance changed hot physical inventory")
		}
		for i := range before {
			if before[i] != after[i] {
				t.Fatalf("healthy hot physical writer %d changed after maintenance: before=%+v after=%+v", i, before[i], after[i])
			}
		}
		for i, p := range ptrs {
			got, e := db.ReadValueLogRecord(p)
			if e != nil || !bytes.Equal(got, values[i]) {
				t.Fatalf("old value %d after maintenance: %v", i, e)
			}
		}
	}
	// Continuing append must use the same actual writer streams.
	next, err := backend.AppendValueLogValues(values)
	if err != nil {
		t.Fatal(err)
	}
	if len(next) != len(ptrs) {
		t.Fatal("appender lost values")
	}
	for i, p := range next {
		if p.FileID != ptrs[i].FileID {
			t.Fatalf("continued value %d moved from %d to %d", i, ptrs[i].FileID, p.FileID)
		}
		got, e := db.ReadValueLogRecord(p)
		if e != nil || !bytes.Equal(got, values[i]) {
			t.Fatalf("continued value %d: %v", i, e)
		}
	}
	for i, l := range lanes {
		if l.vlogSeq > int(db.nativeRootValueLogAppendSeq.Load()) {
			t.Fatal(fmt.Sprintf("writer %d escaped sequence authority", i))
		}
	}
	// The explicit retirement operation must still replace each installed stream.
	floor := int(db.nativeRootValueLogAppendSeq.Load())
	for _, l := range lanes {
		if err := db.advanceValueLogWriterPastObservedSeq(l, floor+1); err != nil {
			t.Fatal(err)
		}
	}
	retired := hotReconcileInventory(t, db)
	for i := range retired {
		if retired[i].id == before[i].id {
			t.Fatalf("explicit retirement kept writer %d", i)
		}
	}
	for i, p := range ptrs {
		got, e := db.ReadValueLogRecord(p)
		if e != nil || !bytes.Equal(got, values[i]) {
			t.Fatalf("old value %d after retirement: %v", i, e)
		}
	}
	for i, p := range next {
		got, e := db.ReadValueLogRecord(p)
		if e != nil || !bytes.Equal(got, values[i]) {
			t.Fatalf("continued value %d after retirement: %v", i, e)
		}
	}
}

func TestHotReconcileRefusesMissingReplacedAndReboundInstalledWriters(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("exact relative namespace inspection unsupported")
	}
	for _, failure := range []string{"missing", "replaced", "rebound-parent", "cache-demoted", "backend-demoted", "reservation-authority"} {
		t.Run(failure, func(t *testing.T) {
			db, backend := openHotReconcileDB(t)
			lanes := db.nativeRootValueLogAppendLanesSnapshot()
			values := make([][]byte, len(lanes)*8)
			for i := range values {
				values[i] = bytes.Repeat([]byte("hot-value"), 128)
			}
			ptrs, err := backend.AppendValueLogValues(values)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.ReadValueLogRecord(ptrs[0]); err != nil {
				t.Fatal(err)
			}
			before := hotReconcileInventory(t, db)
			seg := before[0]
			l := db.nativeRootValueLogAppendLanesSnapshot()[0]
			switch failure {
			case "missing", "replaced":
				saved := seg.path + ".retained-test-owner"
				if err := os.Rename(seg.path, saved); err != nil {
					t.Fatal(err)
				}
				defer func() {
					_ = os.Remove(seg.path)
					if err := os.Rename(saved, seg.path); err != nil {
						t.Error(err)
					}
				}()
				if failure == "replaced" {
					if err := os.WriteFile(seg.path, []byte("unrelated child"), 0600); err != nil {
						t.Fatal(err)
					}
				}
			case "rebound-parent":
				parent := filepath.Dir(seg.path)
				saved := parent + ".retained-test-owner"
				if err := os.Rename(parent, saved); err != nil {
					t.Fatal(err)
				}
				defer func() {
					_ = os.RemoveAll(parent)
					if err := os.Rename(saved, parent); err != nil {
						t.Error(err)
					}
				}()
				if err := os.Mkdir(parent, 0700); err != nil {
					t.Fatal(err)
				}
				for _, s := range before {
					if err := os.Link(filepath.Join(saved, filepath.Base(s.path)), s.path); err != nil {
						t.Fatal(err)
					}
				}
			case "cache-demoted":
				db.valueLogReader.DemoteCurrentWritable(seg.id)
			case "backend-demoted":
				next := int(db.nativeRootValueLogAppendSeq.Load()) + 1
				id, err := valuelog.EncodeFileID(uint32(l.id), uint32(next))
				if err != nil {
					t.Fatal(err)
				}
				path := filepath.Join(filepath.Dir(seg.path), valueLogName(l.id, next))
				w, err := valuelog.NewWriterWithStableResourcePinRegistry(path, id, db.valueLogIdentityPins)
				if err != nil {
					t.Fatal(err)
				}
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}
				if err := backend.RegisterValueLogSegmentReplacing(path, id, seg.id); err != nil {
					t.Fatal(err)
				}
			case "reservation-authority":
				original := db.nativeRootValueLogAppendSeq.Load()
				db.nativeRootValueLogAppendSeq.Store(0)
				defer db.nativeRootValueLogAppendSeq.Store(original)
			}
			// Use the same reconciliation operation without refreshing an intentionally
			// corrupted reservation authority from the directory first.
			if err := db.reconcileValueLogAppendWriterAfterBackendMaintenance(l, int(db.nativeRootValueLogAppendSeq.Load())); err == nil {
				t.Fatal("invalid installed ownership silently preserved or repaired")
			}
			l.vlogMu.Lock()
			path, rotations := l.vlogPath, l.vlogRotateTotal.Load()
			l.vlogMu.Unlock()
			if path != seg.path || rotations != seg.rotations {
				t.Fatal("refusal replaced the installed writer")
			}
		})
	}
}
