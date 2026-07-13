package valuelog

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func writeIdentityPinTestSegment(t *testing.T, dir string) (uint32, string) {
	t.Helper()
	fileID, err := EncodeFileID(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	path := SegmentPath(dir, fileID)
	writer, err := NewWriter(path, fileID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Append(0, nil, 1, []byte("stable value")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return fileID, path
}

func TestManagerZombieDeleteWaitsForStableIdentityPin(t *testing.T) {
	dir := t.TempDir()
	fileID, path := writeIdentityPinTestSegment(t, dir)
	manager, err := NewManagerWithStableResourcePinRegistry(dir, rootpublication.NewIdentityPinRegistry())
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	token := stableManagerToken(t, manager, fileID)
	set := manager.CurrentSetNoRefresh()
	if err := manager.MarkZombie(fileID); err != nil {
		t.Fatal(err)
	}
	if err := manager.Release(set); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("zombie removed while stable-pinned: %v", err)
	}
	token.Release()
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err := os.Stat(path)
		if errors.Is(err, os.ErrNotExist) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatal("zombie was not removed after stable pin release")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestManagerRetryZombieDeleteClosesOwnHandleBeforeUnlink(t *testing.T) {
	dir := t.TempDir()
	fileID, path := writeIdentityPinTestSegment(t, dir)
	manager, err := NewManagerWithStableResourcePinRegistry(dir, rootpublication.NewIdentityPinRegistry())
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	if err := manager.MarkZombie(fileID); err != nil {
		t.Fatal(err)
	}
	manager.mu.RLock()
	file := manager.files[fileID]
	manager.mu.RUnlock()
	if file == nil {
		t.Fatal("manager lost zombie before retry")
	}

	originalRemove := removeSegmentPath
	removeSegmentPath = func(removePath string) error {
		if _, statErr := file.File.Stat(); statErr == nil {
			return errors.New("unlink attempted before closing manager handle")
		}
		return os.Remove(removePath)
	}
	t.Cleanup(func() { removeSegmentPath = originalRemove })

	file.retryDeletePending.Store(true)
	manager.retryZombieDelete(file)
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("zombie remains after close-first retry: %v", err)
	}
	if manager.HasSegment(fileID) {
		t.Fatal("manager retained deleted zombie")
	}
}

func TestManagerZombieDeleteCommitsSuccessfulUnlinkAfterCloseError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value-l1-000001.log")
	handle, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := rootpublication.StableIdentityFromFile(handle)
	if err != nil {
		handle.Close()
		t.Fatal(err)
	}
	namespace, err := stableDeleteNamespace(path)
	if err != nil {
		handle.Close()
		t.Fatal(err)
	}
	if err := handle.Close(); err != nil {
		t.Fatal(err)
	}
	registry := rootpublication.NewIdentityPinRegistry()
	if err := registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	file := &File{
		ID: 1, Path: path, File: handle,
		stableIdentity: identity, stableNamespace: namespace, stableObserved: true,
	}
	file.IsZombie.Store(true)
	manager := &Manager{
		files: map[uint32]*File{file.ID: file}, stableResourcePins: registry,
	}
	if err := manager.deleteZombieFile(file); err == nil {
		t.Fatal("deleteZombieFile returned nil, want the already-closed handle error")
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("successfully unlinked zombie remains: %v", err)
	}
	if manager.HasSegment(file.ID) {
		t.Fatal("manager retained zombie after successful unlink")
	}
	if got := registry.ActiveIdentities(); got != 0 {
		t.Fatalf("identity registry retained %d entries after successful unlink", got)
	}
}

func stableManagerToken(t *testing.T, manager *Manager, fileID uint32) *rootpublication.StableResourceToken {
	t.Helper()
	token, err := manager.StableResourceToken(fileID, StableResourceRegistration{
		Kind: rootpublication.ResourceValueLog, LogicalLane: "main", Generation: 1,
		DiagnosticPath: "value_vlog/" + filepath.Base(manager.SegmentPath(fileID)),
		Digest:         [32]byte{1}, Reachability: rootpublication.ReachabilityValueLogPointer,
	})
	if err != nil {
		t.Fatal(err)
	}
	return token
}

func TestManagerStableIdentityPinBlocksSecondManagerDelete(t *testing.T) {
	dir := t.TempDir()
	fileID, path := writeIdentityPinTestSegment(t, dir)
	registry := rootpublication.NewIdentityPinRegistry()
	first, err := NewManagerWithStableResourcePinRegistry(dir, registry)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewManagerWithStableResourcePinRegistry(dir, registry)
	if err != nil {
		first.Close()
		t.Fatal(err)
	}
	defer second.Close()
	if first.StableResourcePinRegistry() != second.StableResourcePinRegistry() {
		t.Fatal("managers do not share one identity registry")
	}
	token := stableManagerToken(t, first, fileID)
	removed, err := second.RemoveSegmentIfUnpinned(fileID)
	if err != nil {
		t.Fatalf("RemoveSegmentIfUnpinned while stable-pinned: %v", err)
	}
	if removed {
		t.Fatal("stable-pinned segment was removed")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("stable-pinned segment disappeared: %v", err)
	}
	token.Release()
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	removed, err = second.RemoveSegmentIfUnpinned(fileID)
	if err != nil || !removed {
		t.Fatalf("RemoveSegmentIfUnpinned after release = (%v, %v), want (true, nil)", removed, err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("segment remains after delete: %v", err)
	}
}

func TestManagerStableIdentityPinBlocksExplicitDeleteModes(t *testing.T) {
	for _, mode := range []string{"remove", "force"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			fileID, path := writeIdentityPinTestSegment(t, dir)
			registry := rootpublication.NewIdentityPinRegistry()
			producer, err := NewManagerWithStableResourcePinRegistry(dir, registry)
			if err != nil {
				t.Fatal(err)
			}
			deleter, err := NewManagerWithStableResourcePinRegistry(dir, registry)
			if err != nil {
				producer.Close()
				t.Fatal(err)
			}
			defer deleter.Close()
			token := stableManagerToken(t, producer, fileID)
			remove := deleter.RemoveSegment
			if mode == "force" {
				remove = deleter.RemoveSegmentForce
			}
			if err := remove(fileID); !errors.Is(err, ErrFilePinned) {
				t.Fatalf("delete while stable-pinned = %v, want ErrFilePinned", err)
			}
			if _, err := os.Stat(path); err != nil {
				t.Fatalf("stable-pinned segment disappeared: %v", err)
			}
			token.Release()
			if err := producer.Close(); err != nil {
				t.Fatal(err)
			}
			if err := remove(fileID); err != nil {
				t.Fatalf("delete after release: %v", err)
			}
			if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("segment remains after delete: %v", err)
			}
		})
	}
}

func TestManagerStableDeleteRejectsReplacedPath(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows does not permit replacing an open segment path")
	}
	dir := t.TempDir()
	fileID, path := writeIdentityPinTestSegment(t, dir)
	manager, err := NewManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	if err := manager.SetStableResourcePinRegistry(rootpublication.NewIdentityPinRegistry()); err != nil {
		t.Fatal(err)
	}
	oldPath := path + ".old"
	if err := os.Rename(path, oldPath); err != nil {
		t.Fatal(err)
	}
	const replacement = "replacement must survive"
	if err := os.WriteFile(path, []byte(replacement), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := manager.RemoveSegment(fileID); !errors.Is(err, rootpublication.ErrResourceConflict) {
		t.Fatalf("RemoveSegment replaced path = %v, want ErrResourceConflict", err)
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != replacement {
		t.Fatalf("replacement changed: got=%q err=%v", got, err)
	}
}

func TestManagerRegisterSegmentRejectsSameIDReboundPath(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows does not permit replacing an open segment path")
	}
	dir := t.TempDir()
	fileID, path := writeIdentityPinTestSegment(t, dir)
	manager, err := NewManagerWithStableResourcePinRegistry(dir, rootpublication.NewIdentityPinRegistry())
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	if err := os.Rename(path, path+".old"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("replacement"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := manager.RegisterSegment(path, fileID); !errors.Is(err, rootpublication.ErrResourceConflict) {
		t.Fatalf("RegisterSegment rebound path = %v, want ErrResourceConflict", err)
	}
}
