package caching

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

// Use the real backend: the capturing BackendDB wrapper intentionally does
// not expose the concrete shared-registry/registration capability.
func openLeafReconcileDB(t *testing.T) (*DB, *backenddb.DB, *cachingLeafPageLogGroup) {
	t.Helper()
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatal(err)
	}
	db, err := Open(dir, backend, Options{
		IndexOuterLeavesInValueLog: true, FlushApplyConcurrency: 2,
		RelaxedSync: true, AllowUnsafe: true,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return db, backend, &cachingLeafPageLogGroup{db: db}
}

type leafReconcileFile struct {
	path      string
	id        uint32
	identity  rootpublication.StableIdentity
	size      int64
	rotations uint64
}

func leafReconcileInventory(t *testing.T, db *DB) []leafReconcileFile {
	t.Helper()
	var inventory []leafReconcileFile
	for _, l := range db.leafLogAppendLanesSnapshot() {
		if l == nil {
			continue
		}
		l.vlogMu.Lock()
		if l.vlog == nil {
			l.vlogMu.Unlock()
			continue
		}
		id, err := valuelog.EncodeFileID(uint32(l.id), uint32(l.vlogSeq))
		path, rotations := l.vlogPath, l.vlogRotateTotal.Load()
		l.vlogMu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
		file, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		identity, err := rootpublication.StableIdentityFromFile(file)
		info, statErr := file.Stat()
		closeErr := file.Close()
		if err != nil || statErr != nil || closeErr != nil {
			t.Fatal(errors.Join(err, statErr, closeErr))
		}
		inventory = append(inventory, leafReconcileFile{path, id, identity, info.Size(), rotations})
	}
	return inventory
}

func TestLeafReconcilePreservesHealthyPhysicalWritersAfterNoWorkAndFailure(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("exact relative namespace inspection unsupported")
	}
	db, backend, group := openLeafReconcileDB(t)
	var ptrs []page.LeafLogPtr
	var payloads [][]byte
	for worker := 0; worker < 4; worker++ {
		log, ok := group.LeafPageLogLane(worker)
		if !ok {
			t.Fatalf("worker %d missing", worker)
		}
		payload := testLeafPageBytes(fmt.Sprintf("preserved-worker-%d", worker))
		ptr, err := log.AppendLeafPage(payload)
		if err != nil {
			t.Fatal(err)
		}
		if err := log.Flush(); err != nil {
			t.Fatal(err)
		}
		ptrs, payloads = append(ptrs, ptr), append(payloads, payload)
	}
	before := leafReconcileInventory(t, db)
	if len(before) != 4 {
		t.Fatalf("physical writers=%d want4", len(before))
	}
	// A backend-created, higher-generation file raises future allocation only.
	floor := int(db.leafLogAppendSeq.Load()) + 20
	fileID, err := valuelog.EncodeFileID(uint32(leafLogLaneID), uint32(floor))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(db.leafLogDir, valueLogName(leafLogLaneID, floor))
	w, err := valuelog.NewWriterWithStableResourcePinRegistry(path, fileID, db.valueLogIdentityPins)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append(0, nil, 1, []byte("backend-created-generation")); err != nil {
		_ = w.Close()
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := backend.RegisterValueLogSegment(path, fileID); err != nil {
		t.Fatal(err)
	}
	failed := errors.New("backend maintenance refused")
	for _, fnErr := range []error{nil, failed, nil} {
		err := db.runWithBackendMaintenanceOptions(backendMaintenanceOptions{skipCheckpoint: true}, func() error { return fnErr })
		if !errors.Is(err, fnErr) {
			t.Fatalf("maintenance error=%v want%v", err, fnErr)
		}
		after := leafReconcileInventory(t, db)
		if len(after) != len(before) {
			t.Fatal("maintenance changed worker inventory")
		}
		for i := range before {
			if after[i] != before[i] {
				t.Fatalf("healthy worker changed: before=%+v after=%+v", before[i], after[i])
			}
			if err := backend.ValidateCurrentWritableValueLogBinding(after[i].path, after[i].id, after[i].identity, db.valueLogIdentityPins); err != nil {
				t.Fatal(err)
			}
		}
	}
	if db.leafLogAppendSeq.Load() < uint32(floor) {
		t.Fatal("future reservation floor did not observe backend generation")
	}
	log, _ := group.LeafPageLogLane(5)
	ptr, err := log.AppendLeafPage(testLeafPageBytes("future-worker"))
	if err != nil {
		t.Fatal(err)
	}
	_, seq := valuelog.DecodeFileID(ptr.ValuePtr().FileID)
	if int(seq) <= floor {
		t.Fatalf("new worker sequence=%d floor=%d", seq, floor)
	}
	for i, ptr := range ptrs {
		got, err := db.ReadValueLogRecord(ptr.ValuePtr())
		if err != nil || !bytes.Equal(got, payloads[i]) {
			t.Fatalf("preserved pointer%d err=%v", i, err)
		}
	}
	// Explicit retirement still moves all dirty physical writers, including the
	// healthy peers preserved by the generic maintenance path above.
	if err := backend.AdvanceLeafPageLogGeneration(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, after := range leafReconcileInventory(t, db) {
		_, seq := valuelog.DecodeFileID(after.id)
		if int(seq) <= floor {
			t.Fatalf("explicit handoff kept old writer%+v", after)
		}
	}
}

func TestLeafReconcileRefusesMissingReplacedAndReboundInstalledWriters(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("exact relative namespace inspection unsupported")
	}
	for _, failure := range []string{"missing", "replaced", "rebound-parent", "cache-demoted", "backend-demoted"} {
		t.Run(failure, func(t *testing.T) {
			db, backend, group := openLeafReconcileDB(t)
			if _, err := group.AppendLeafPage(testLeafPageBytes("installed-owner")); err != nil {
				t.Fatal(err)
			}
			if err := group.Flush(); err != nil {
				t.Fatal(err)
			}
			before := leafReconcileInventory(t, db)
			if len(before) != 1 {
				t.Fatalf("inventory%v", before)
			}
			seg := before[0]
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
				saved := db.leafLogDir + ".retained-test-owner"
				if err := os.Rename(db.leafLogDir, saved); err != nil {
					t.Fatal(err)
				}
				defer func() {
					_ = os.RemoveAll(db.leafLogDir)
					if err := os.Rename(saved, db.leafLogDir); err != nil {
						t.Error(err)
					}
				}()
				if err := os.Mkdir(db.leafLogDir, 0700); err != nil {
					t.Fatal(err)
				}
				// Preserve the same physical child under a different parent inode.
				if err := os.Link(filepath.Join(saved, filepath.Base(seg.path)), seg.path); err != nil {
					t.Fatal(err)
				}
			case "cache-demoted":
				db.valueLogReader.DemoteCurrentWritable(seg.id)
			case "backend-demoted":
				// Existing replacement registration demotes the actual old file.
				next := int(db.leafLogAppendSeq.Load()) + 1
				id, err := valuelog.EncodeFileID(uint32(leafLogLaneID), uint32(next))
				if err != nil {
					t.Fatal(err)
				}
				path := filepath.Join(db.leafLogDir, valueLogName(leafLogLaneID, next))
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
			}
			if err := db.ReconcileAfterBackendMaintenance(); err == nil {
				t.Fatal("invalid installed ownership silently preserved or repaired")
			}
			l := db.leafLogAppendLaneForWorkerIndex(0)
			l.vlogMu.Lock()
			path, rotations := l.vlogPath, l.vlogRotateTotal.Load()
			l.vlogMu.Unlock()
			if path != seg.path || rotations != seg.rotations {
				t.Fatal("refusal replaced the installed writer")
			}
		})
	}
}
