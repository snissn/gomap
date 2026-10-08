package valuelog

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func openCurrentBindingFixture(t *testing.T) (*Writer, *Manager, string, uint32, *rootpublication.IdentityPinRegistry) {
	t.Helper()
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("exact relative namespace inspection unsupported")
	}
	dir := t.TempDir()
	pins := rootpublication.NewIdentityPinRegistry()
	id, err := EncodeFileID(ReservedLeafLogLaneID, 1)
	if err != nil {
		t.Fatal(err)
	}
	path := SegmentPath(dir, id)
	w, err := NewWriterWithStableResourcePinRegistry(path, id, pins)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := w.Close(); err != nil {
			t.Error(err)
		}
	})
	m, err := NewManagerWithStableResourcePinRegistry(dir, pins)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := m.Close(); err != nil {
			t.Error(err)
		}
	})
	m.SetMultiCurrentWritableLane(ReservedLeafLogLaneID, true)
	if err := m.PromoteCurrentWritable(id); err != nil {
		t.Fatal(err)
	}
	return w, m, path, id, pins
}

func TestCurrentBindingObservationPreservesBufferedWriterAndRegistration(t *testing.T) {
	w, m, path, id, pins := openCurrentBindingFixture(t)
	if _, err := w.Append(0, nil, 1, []byte("not flushed by inspection")); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	size := w.Size()
	scans := m.RefreshScanCount()
	syncs, dirs := w.fileSyncCalls.Load(), w.directorySyncCalls.Load()
	for n := 0; n < 8; n++ {
		identity, err := w.ValidateCurrentBinding(path, id, pins)
		if err != nil {
			t.Fatal(err)
		}
		if err := m.ValidateCurrentWritableBinding(path, id, identity, pins); err != nil {
			t.Fatal(err)
		}
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if before.Size() != after.Size() || w.Size() != size || m.RefreshScanCount() != scans ||
		w.fileSyncCalls.Load() != syncs || w.directorySyncCalls.Load() != dirs {
		t.Fatal("ownership observation performed producer work")
	}
	if ids := m.CurrentWritableFileIDs(); len(ids) != 1 || ids[0] != id {
		t.Fatalf("registration changed%v", ids)
	}
	identity, err := w.ValidateCurrentBinding(path, id, pins)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.ValidateCurrentWritableBinding(path, id, identity, rootpublication.NewIdentityPinRegistry()); !errors.Is(err, rootpublication.ErrResourceConflict) {
		t.Fatalf("foreign registry=%v", err)
	}
	w.pendingStableSuccessor = &pendingValueLogSuccessor{}
	_, err = w.ValidateCurrentBinding(path, id, pins)
	w.pendingStableSuccessor = nil
	if !errors.Is(err, rootpublication.ErrResourceConflict) {
		t.Fatalf("pending successor accepted%v", err)
	}
	m.DemoteCurrentWritable(id)
	if err := m.ValidateCurrentWritableBinding(path, id, identity, pins); !errors.Is(err, rootpublication.ErrResourceConflict) {
		t.Fatalf("demoted manager binding accepted%v", err)
	}
}

func TestCurrentBindingManagerRejectsChildAndParentRebinding(t *testing.T) {
	for _, failure := range []string{"missing", "replaced", "rebound-parent"} {
		t.Run(failure, func(t *testing.T) {
			w, m, path, id, pins := openCurrentBindingFixture(t)
			identity, err := w.ValidateCurrentBinding(path, id, pins)
			if err != nil {
				t.Fatal(err)
			}
			if failure == "rebound-parent" {
				dir := filepath.Dir(path)
				saved := dir + ".retained-owner"
				if err := os.Rename(dir, saved); err != nil {
					t.Fatal(err)
				}
				defer func() {
					_ = os.RemoveAll(dir)
					if err := os.Rename(saved, dir); err != nil {
						t.Error(err)
					}
				}()
				if err := os.Mkdir(dir, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.Link(filepath.Join(saved, filepath.Base(path)), path); err != nil {
					t.Fatal(err)
				}
			} else {
				saved := path + ".retained-owner"
				if err := os.Rename(path, saved); err != nil {
					t.Fatal(err)
				}
				defer func() {
					_ = os.Remove(path)
					if err := os.Rename(saved, path); err != nil {
						t.Error(err)
					}
				}()
				if failure == "replaced" {
					if err := os.WriteFile(path, []byte("foreign file"), 0600); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := m.ValidateCurrentWritableBinding(path, id, identity, pins); !errors.Is(err, rootpublication.ErrResourceConflict) {
				t.Fatalf("manager accepted rebound namespace%v", err)
			}
			if _, err := w.ValidateCurrentBinding(path, id, pins); !errors.Is(err, rootpublication.ErrResourceConflict) {
				t.Fatalf("writer accepted rebound namespace%v", err)
			}
		})
	}
}

func TestCurrentBindingInspectionsRaceManagerClose(t *testing.T) {
	w, m, path, id, pins := openCurrentBindingFixture(t)
	identity, err := w.ValidateCurrentBinding(path, id, pins)
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	errorsSeen := make(chan error, 32)
	var workers sync.WaitGroup
	for n := 0; n < 32; n++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			for n := 0; n < 8; n++ {
				if err := m.ValidateCurrentWritableBinding(path, id, identity, pins); err != nil && !errors.Is(err, rootpublication.ErrResourceConflict) {
					errorsSeen <- err
					return
				}
			}
		}()
	}
	closed := make(chan error, 1)
	go func() { <-start; closed <- m.Close() }()
	close(start)
	workers.Wait()
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	close(errorsSeen)
	for err := range errorsSeen {
		t.Fatalf("Close broke admitted file inspection:%v", err)
	}
	if err := m.ValidateCurrentWritableBinding(path, id, identity, pins); !errors.Is(err, rootpublication.ErrResourceConflict) {
		t.Fatalf("closed manager accepted inspection:%v", err)
	}
}
