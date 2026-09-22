package valuelog

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	templ "github.com/snissn/gomap/TreeDB/template"
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

func identityPinTestIdentity(t *testing.T, path string) rootpublication.StableIdentity {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	identity, err := rootpublication.StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	return identity
}

func TestManagerStartupCompletesMatchingStableDeleteQuarantine(t *testing.T) {
	dir := t.TempDir()
	fileID, path := writeIdentityPinTestSegment(t, dir)
	identity := identityPinTestIdentity(t, path)
	quarantineDir, quarantinePath, err := stableDeleteQuarantinePaths(path, identity)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(quarantineDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path, quarantinePath); err != nil {
		t.Fatal(err)
	}

	manager, err := NewManagerWithStableResourcePinRegistry(dir, rootpublication.NewIdentityPinRegistry())
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	if manager.HasSegment(fileID) {
		t.Fatal("manager registered a segment whose stable deletion had committed")
	}
	if _, err := os.Stat(quarantineDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("matching quarantine remains after startup recovery: %v", err)
	}
}

func TestReadOnlyManagerRejectsStableDeleteQuarantineWithoutMutation(t *testing.T) {
	dir := t.TempDir()
	_, path := writeIdentityPinTestSegment(t, dir)
	identity := identityPinTestIdentity(t, path)
	quarantineDir, quarantinePath, err := stableDeleteQuarantinePaths(path, identity)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(quarantineDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path, quarantinePath); err != nil {
		t.Fatal(err)
	}

	manager, err := NewReadOnlyManagerWithStableResourcePinRegistry(dir, rootpublication.NewIdentityPinRegistry())
	if manager != nil || !errors.Is(err, ErrStableDeleteRecoveryRequired) || !errors.Is(err, rootpublication.ErrRecoveryRequired) {
		t.Fatalf("NewReadOnlyManager=(%v, %v), want specific and shared recovery errors", manager, err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("read-only open restored canonical path: %v", err)
	}
	if _, err := os.Stat(quarantinePath); err != nil {
		t.Fatalf("read-only open changed quarantine inode: %v", err)
	}
	if _, err := os.Stat(quarantineDir); err != nil {
		t.Fatalf("read-only open changed quarantine directory: %v", err)
	}
}

func TestReadOnlyManagerRefreshRejectsNewStableDeleteQuarantineWithoutMutation(t *testing.T) {
	dir := t.TempDir()
	manager, err := NewReadOnlyManagerWithStableResourcePinRegistry(dir, rootpublication.NewIdentityPinRegistry())
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()

	_, path := writeIdentityPinTestSegment(t, dir)
	identity := identityPinTestIdentity(t, path)
	quarantineDir, quarantinePath, err := stableDeleteQuarantinePaths(path, identity)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(quarantineDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path, quarantinePath); err != nil {
		t.Fatal(err)
	}

	err = manager.Refresh()
	if !errors.Is(err, ErrStableDeleteRecoveryRequired) || !errors.Is(err, rootpublication.ErrRecoveryRequired) {
		t.Fatalf("Refresh() error = %v, want specific and shared recovery errors", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("read-only refresh restored canonical path: %v", err)
	}
	if _, err := os.Stat(quarantinePath); err != nil {
		t.Fatalf("read-only refresh changed quarantine inode: %v", err)
	}
	if _, err := os.Stat(quarantineDir); err != nil {
		t.Fatalf("read-only refresh changed quarantine directory: %v", err)
	}
}

func TestManagerStartupRestoresUnexpectedStableDeleteQuarantineIdentity(t *testing.T) {
	dir := t.TempDir()
	fileID, path := writeIdentityPinTestSegment(t, dir)
	intendedIdentity := identityPinTestIdentity(t, path)
	replacementDir := filepath.Join(dir, "replacement")
	if err := os.Mkdir(replacementDir, 0o700); err != nil {
		t.Fatal(err)
	}
	_, replacementPath := writeIdentityPinTestSegment(t, replacementDir)
	replacementIdentity := identityPinTestIdentity(t, replacementPath)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if rootpublication.SamePhysicalIdentity(intendedIdentity, replacementIdentity) {
		t.Fatal("replacement unexpectedly reused the intended physical identity")
	}
	quarantineDir, quarantinePath, err := stableDeleteQuarantinePaths(path, intendedIdentity)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(quarantineDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacementPath, quarantinePath); err != nil {
		t.Fatal(err)
	}

	manager, err := NewManagerWithStableResourcePinRegistry(dir, rootpublication.NewIdentityPinRegistry())
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	if !manager.HasSegment(fileID) {
		t.Fatal("manager did not register the conservatively restored segment")
	}
	gotIdentity := identityPinTestIdentity(t, path)
	if !rootpublication.SamePhysicalIdentity(gotIdentity, replacementIdentity) {
		t.Fatal("startup recovery restored the wrong physical identity")
	}
	if _, err := os.Stat(quarantineDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unexpected-identity quarantine remains after restore: %v", err)
	}
}

func TestManagerStartupRollsBackPartialStableDeleteRestore(t *testing.T) {
	dir := t.TempDir()
	fileID, path := writeIdentityPinTestSegment(t, dir)
	identity := identityPinTestIdentity(t, path)
	quarantineDir, quarantinePath, err := stableDeleteQuarantinePaths(path, identity)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(quarantineDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(path, quarantinePath); err != nil {
		t.Fatal(err)
	}

	manager, err := NewManagerWithStableResourcePinRegistry(dir, rootpublication.NewIdentityPinRegistry())
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	if !manager.HasSegment(fileID) {
		t.Fatal("manager lost the canonical segment after partial restore recovery")
	}
	if _, err := os.Stat(quarantineDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("partial-restore quarantine remains after startup recovery: %v", err)
	}
}

func TestManagerStartupCompletesQuarantineAndPreservesReplacement(t *testing.T) {
	dir := t.TempDir()
	fileID, path := writeIdentityPinTestSegment(t, dir)
	identity := identityPinTestIdentity(t, path)
	quarantineDir, quarantinePath, err := stableDeleteQuarantinePaths(path, identity)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(quarantineDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path, quarantinePath); err != nil {
		t.Fatal(err)
	}
	replacementDir := filepath.Join(dir, "replacement")
	if err := os.Mkdir(replacementDir, 0o700); err != nil {
		t.Fatal(err)
	}
	_, replacementPath := writeIdentityPinTestSegment(t, replacementDir)
	replacementIdentity := identityPinTestIdentity(t, replacementPath)
	if err := os.Rename(replacementPath, path); err != nil {
		t.Fatal(err)
	}

	manager, err := NewManagerWithStableResourcePinRegistry(dir, rootpublication.NewIdentityPinRegistry())
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	if !manager.HasSegment(fileID) {
		t.Fatal("manager did not register the replacement segment")
	}
	if got := identityPinTestIdentity(t, path); !rootpublication.SamePhysicalIdentity(got, replacementIdentity) {
		t.Fatal("startup recovery changed the replacement identity")
	}
	if _, err := os.Stat(quarantineDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("matching quarantine remains after replacement recovery: %v", err)
	}
}

func TestManagerStartupRejectsAmbiguousStableDeleteQuarantine(t *testing.T) {
	dir := t.TempDir()
	_, path := writeIdentityPinTestSegment(t, dir)
	intendedIdentity := identityPinTestIdentity(t, path)
	quarantineDir, quarantinePath, err := stableDeleteQuarantinePaths(path, intendedIdentity)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(quarantineDir, 0o700); err != nil {
		t.Fatal(err)
	}
	replacementDir := filepath.Join(dir, "replacement")
	if err := os.Mkdir(replacementDir, 0o700); err != nil {
		t.Fatal(err)
	}
	_, replacementPath := writeIdentityPinTestSegment(t, replacementDir)
	if err := os.Rename(replacementPath, quarantinePath); err != nil {
		t.Fatal(err)
	}

	manager, err := NewManagerWithStableResourcePinRegistry(dir, rootpublication.NewIdentityPinRegistry())
	if manager != nil || !errors.Is(err, rootpublication.ErrResourceConflict) {
		t.Fatalf("NewManager ambiguous quarantine=(%v, %v), want (nil, ErrResourceConflict)", manager, err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("canonical segment changed after fail-closed recovery: %v", err)
	}
	if _, err := os.Stat(quarantinePath); err != nil {
		t.Fatalf("ambiguous quarantine changed after fail-closed recovery: %v", err)
	}
}

func TestStableDeleteRetryPreservesReplacementBesideCommittedQuarantine(t *testing.T) {
	dir := t.TempDir()
	_, path := writeIdentityPinTestSegment(t, dir)
	identity := identityPinTestIdentity(t, path)
	quarantineDir, quarantinePath, err := stableDeleteQuarantinePaths(path, identity)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(quarantineDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path, quarantinePath); err != nil {
		t.Fatal(err)
	}
	const replacement = "replacement after committed canonical unlink"
	if err := os.WriteFile(path, []byte(replacement), 0o600); err != nil {
		t.Fatal(err)
	}

	removed, err := removeStableSegmentFileOnce(path, identity)
	if err != nil || !removed {
		t.Fatalf("stable delete retry = (%v, %v), want (true, nil)", removed, err)
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != replacement {
		t.Fatalf("replacement changed during stable delete retry: got=%q err=%v", got, err)
	}
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

func TestManagerStableDeletePreservesReplacementCreatedAtUnlink(t *testing.T) {
	dir := t.TempDir()
	fileID, path := writeIdentityPinTestSegment(t, dir)
	manager, err := NewManagerWithStableResourcePinRegistry(dir, rootpublication.NewIdentityPinRegistry())
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()

	const replacement = "replacement created after quarantine rename"
	originalRemove := removeSegmentPath
	removeSegmentPath = func(quarantinePath string) error {
		if filepath.Clean(quarantinePath) == filepath.Clean(path) {
			t.Fatal("stable delete passed the original pathname to unlink")
		}
		if err := os.WriteFile(path, []byte(replacement), 0o600); err != nil {
			return err
		}
		return os.Remove(quarantinePath)
	}
	t.Cleanup(func() { removeSegmentPath = originalRemove })

	if err := manager.RemoveSegment(fileID); err != nil {
		t.Fatalf("RemoveSegment: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != replacement {
		t.Fatalf("replacement changed: got=%q err=%v", got, err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			t.Fatalf("stable delete left quarantine directory %q", entry.Name())
		}
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

func TestManagerStableDeleteEmitsCanonicalUnlinkCutBeforeQuarantineUnlink(t *testing.T) {
	dir := t.TempDir()
	fileID, path := writeIdentityPinTestSegment(t, dir)
	registry := rootpublication.NewIdentityPinRegistry()
	producer, err := NewManagerWithStableResourcePinRegistry(dir, registry)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	deleter, err := NewManagerWithStableResourcePinRegistry(dir, registry)
	if err != nil {
		t.Fatal(err)
	}
	defer deleter.Close()

	originalRemove := removeSegmentPath
	quarantineUnlinked := false
	removeSegmentPath = func(removePath string) error {
		quarantineUnlinked = true
		return originalRemove(removePath)
	}
	t.Cleanup(func() { removeSegmentPath = originalRemove })

	wantErr := errors.New("injected post-unlink cut")
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Namespace == durabilitycut.NamespaceUnlink &&
			event.Resource == durabilitycut.ResourceValueLog &&
			filepath.Clean(event.OldPath) == filepath.Clean(path) {
			return wantErr
		}
		return nil
	})
	err = deleter.RemoveSegment(fileID)
	restore()
	if !errors.Is(err, wantErr) {
		t.Fatalf("RemoveSegment error=%v, want post-unlink cut", err)
	}
	if quarantineUnlinked {
		t.Fatal("stable delete unlinked the quarantined inode before emitting the canonical-name cut")
	}
	if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("post-unlink cut path stat=%v, want removed", statErr)
	}

	token, pinErr := producer.StableResourceToken(fileID, StableResourceRegistration{
		Kind: rootpublication.ResourceValueLog, LogicalLane: "main", Generation: 2,
		DiagnosticPath: "value_vlog/" + filepath.Base(path),
		Digest:         [32]byte{2}, Reachability: rootpublication.ReachabilityValueLogPointer,
	})
	if token != nil {
		token.Release()
	}
	if !errors.Is(pinErr, rootpublication.ErrResourceConflict) {
		t.Fatalf("late pin after committed unlink error=%v, want ErrResourceConflict", pinErr)
	}
}

func TestManagerRejectsRegistryInstallationAfterAnotherManagerCommitsDelete(t *testing.T) {
	dir := t.TempDir()
	fileID, path := writeIdentityPinTestSegment(t, dir)
	late, err := NewManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer late.Close()
	registry := rootpublication.NewIdentityPinRegistry()
	deleter, err := NewManagerWithStableResourcePinRegistry(dir, registry)
	if err != nil {
		t.Fatal(err)
	}
	if err := deleter.RemoveSegment(fileID); err != nil {
		_ = deleter.Close()
		t.Fatal(err)
	}
	if err := deleter.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("committed delete left path: %v", err)
	}
	if got := registry.ActiveIdentities(); got != 0 {
		t.Fatalf("registry retained %d identities after committed delete", got)
	}
	if err := late.SetStableResourcePinRegistry(registry); !errors.Is(err, rootpublication.ErrUnresolvedResource) {
		t.Fatalf("late registry installation error=%v want ErrUnresolvedResource", err)
	}
	token, err := late.StableResourceToken(fileID, StableResourceRegistration{
		Kind: rootpublication.ResourceValueLog, LogicalLane: "main", Generation: 1,
		DiagnosticPath: "value_vlog/" + filepath.Base(path),
		Reachability:   rootpublication.ReachabilityValueLogPointer,
	})
	if token != nil {
		token.Release()
		t.Fatal("unregistered manager returned a stable resource token")
	}
	if !errors.Is(err, rootpublication.ErrUnresolvedResource) {
		t.Fatalf("unregistered manager token error=%v want ErrUnresolvedResource", err)
	}
	if got := registry.ActiveIdentities(); got != 0 {
		t.Fatalf("late installation resurrected %d identities", got)
	}
}

func TestManagerStableDeleteRestoreCompensatesCanonicalUnlink(t *testing.T) {
	dir := t.TempDir()
	fileID, path := writeIdentityPinTestSegment(t, dir)
	manager, err := NewManagerWithStableResourcePinRegistry(dir, rootpublication.NewIdentityPinRegistry())
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()

	wantErr := errors.New("injected quarantine unlink failure")
	originalRemove := removeSegmentPath
	removeSegmentPath = func(string) error { return wantErr }
	t.Cleanup(func() { removeSegmentPath = originalRemove })

	var operations []durabilitycut.NamespaceOperation
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if filepath.Clean(event.OldPath) == filepath.Clean(path) || filepath.Clean(event.NewPath) == filepath.Clean(path) {
			operations = append(operations, event.Namespace)
		}
		return nil
	})
	err = manager.RemoveSegment(fileID)
	restore()
	if !errors.Is(err, wantErr) {
		t.Fatalf("RemoveSegment error=%v, want quarantine unlink failure", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("restored segment stat: %v", err)
	}
	if len(operations) != 2 || operations[0] != durabilitycut.NamespaceUnlink || operations[1] != durabilitycut.NamespaceCreate {
		t.Fatalf("namespace operations=%v, want [unlink create]", operations)
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
	manager, err := NewManagerWithStableResourcePinRegistry(dir, rootpublication.NewIdentityPinRegistry())
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
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

func TestManagerRemoveSegmentExpectedIdentityRejectsRegisteredReplacement(t *testing.T) {
	dir := t.TempDir()
	fileID, path := writeIdentityPinTestSegment(t, dir)
	expected := identityPinTestIdentity(t, path)
	if err := os.Rename(path, path+".old"); err != nil {
		t.Fatal(err)
	}
	_, replacementPath := writeIdentityPinTestSegment(t, dir)
	replacement := identityPinTestIdentity(t, replacementPath)
	if rootpublication.SamePhysicalIdentity(expected, replacement) {
		t.Fatal("replacement unexpectedly reused the original physical identity")
	}

	manager, err := NewManagerWithStableResourcePinRegistry(dir, rootpublication.NewIdentityPinRegistry())
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	if err := manager.RemoveSegmentExpectedIdentity(fileID, expected); !errors.Is(err, rootpublication.ErrResourceConflict) {
		t.Fatalf("RemoveSegmentExpectedIdentity replacement error = %v, want ErrResourceConflict", err)
	}
	got := identityPinTestIdentity(t, replacementPath)
	if !rootpublication.SamePhysicalIdentity(got, replacement) {
		t.Fatal("expected-identity removal changed the registered replacement")
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

func TestNewManagerRefreshFailureReleasesObservedIdentities(t *testing.T) {
	dir := t.TempDir()
	for seq := uint32(1); seq <= 2; seq++ {
		fileID, err := EncodeFileID(1, seq)
		if err != nil {
			t.Fatal(err)
		}
		writer, err := NewWriter(SegmentPath(dir, fileID), fileID)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Append(0, nil, uint64(seq), []byte("constructor cleanup")); err != nil {
			writer.Close()
			t.Fatal(err)
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
	}

	originalOpen := currentOpenSegmentFile()
	openCalls := 0
	var firstOpened *File
	wantErr := errors.New("injected second segment open failure")
	swapOpenSegmentFileForTest(func(path string, id uint32, dictLookup DictLookup, templateLookup TemplateLookup, templateOpts templ.DecodeOptions, templateCache *templateDefCache) (*File, error) {
		openCalls++
		if openCalls == 2 {
			return nil, wantErr
		}
		file, err := originalOpen(path, id, dictLookup, templateLookup, templateOpts, templateCache)
		if err == nil {
			firstOpened = file
		}
		return file, err
	})
	t.Cleanup(func() { swapOpenSegmentFileForTest(originalOpen) })

	registry := rootpublication.NewIdentityPinRegistry()
	manager, err := NewManagerWithStableResourcePinRegistry(dir, registry)
	if manager != nil || !errors.Is(err, wantErr) {
		t.Fatalf("NewManager = (%v, %v), want (nil, injected error)", manager, err)
	}
	if firstOpened == nil {
		t.Fatal("constructor did not open the first segment")
	}
	if _, err := firstOpened.File.Stat(); err == nil {
		t.Fatal("partially initialized manager left its first handle open")
	}
	if got := registry.ActiveIdentities(); got != 0 {
		t.Fatalf("registry retained %d constructor observers", got)
	}
}
