package valuelog

import (
	"os"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func retirementIdentityAssertCount(t *testing.T, manager *Manager, want int) {
	t.Helper()
	manager.mu.RLock()
	count := manager.retiredCount
	actual := 0
	for _, file := range manager.files {
		if file.IsZombie.Load() {
			actual++
		}
	}
	manager.mu.RUnlock()
	if count != want || count != actual {
		t.Fatalf("tracked retirement count=%d actual zombies=%d want=%d", count, actual, want)
	}
}

func TestManagerRetirementIdentityAccounting(t *testing.T) {
	for _, mode := range []string{"Release", "EvictSegment", "RemoveSegment", "RemoveSegmentExpectedIdentity", "RemoveSegmentIfUnpinned", "RemoveSegmentForce", "Close"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			id := writeTestSegment(t, dir, 0, 1, 1, []byte("counted retirement owner"))
			manager, err := NewManager(dir)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = manager.Close() })
			manager.mu.RLock()
			file := manager.files[id]
			manager.mu.RUnlock()
			identity := retirementIdentityAtPath(t, file.Path)
			var set *Set
			if mode == "Release" {
				set = manager.CurrentSetNoRefresh()
			}
			retirementIdentityAssertCount(t, manager, 0)
			for range 3 {
				if err := manager.MarkZombie(id); err != nil {
					t.Fatal(err)
				}
			}
			if tracked, newlyMarked, err := manager.MarkZombieIfTracked(id); err != nil || !tracked || newlyMarked {
				t.Fatalf("repeated zombie transition=%t/%t error=%v", tracked, newlyMarked, err)
			}
			retirementIdentityAssertCount(t, manager, 1)
			manager.mu.RLock()
			retired := file.retirementIdentity
			manager.mu.RUnlock()
			if !rootpublication.SamePhysicalIdentity(identity, retired) {
				t.Fatal("retirement identity does not name the original open handle")
			}
			switch mode {
			case "Release":
				err = manager.Release(set)
			case "EvictSegment":
				err = manager.EvictSegment(id)
			case "Close":
				err = manager.Close()
			default:
				err = retirementIdentityRemove(manager, file, identity, mode)
			}
			if err != nil {
				t.Fatalf("%s retirement completion: %v", mode, err)
			}
			retirementIdentityAssertCount(t, manager, 0)
			if tracked, newlyMarked, err := manager.MarkZombieIfTracked(id); err != nil || tracked || newlyMarked {
				t.Fatalf("forgotten owner transition=%t/%t error=%v", tracked, newlyMarked, err)
			}
			retirementIdentityAssertCount(t, manager, 0)
		})
	}
}

func TestManagerRetirementIdentityCaptureFailureAccounting(t *testing.T) {
	for _, mode := range []string{"MarkZombie", "MarkZombieIfTracked", "RemoveSegment", "RemoveSegmentExpectedIdentity", "RemoveSegmentIfUnpinned", "RemoveSegmentForce"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			id := writeTestSegment(t, dir, 0, 1, 1, []byte("uncaptured count owner"))
			manager, err := NewManager(dir)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = manager.Close() })
			manager.mu.RLock()
			file := manager.files[id]
			manager.mu.RUnlock()
			identity := retirementIdentityAtPath(t, file.Path)
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "MarkZombie":
				err = manager.MarkZombie(id)
			case "MarkZombieIfTracked":
				_, _, err = manager.MarkZombieIfTracked(id)
			default:
				err = retirementIdentityRemove(manager, file, identity, mode)
			}
			if err == nil {
				t.Fatalf("%s accepted unavailable original identity", mode)
			}
			retirementIdentityAssertCount(t, manager, 0)
			manager.mu.RLock()
			retired := file.retirementIdentity
			tracked := manager.files[id]
			manager.mu.RUnlock()
			if retired != (rootpublication.StableIdentity{}) || tracked != file {
				t.Fatal("capture refusal published identity or forgot original owner")
			}
		})
	}
}

func TestManagerRetirementIdentityRegisteredIdentityConcurrent(t *testing.T) {
	dir := t.TempDir()
	id := writeTestSegment(t, dir, 0, 1, 1, []byte("pinned immutable identity"))
	manager, err := NewManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	set := manager.CurrentSetNoRefresh()
	file := set.Files[id]
	before, hadBefore := file.RegisteredStableIdentity()
	if hadBefore || before != (rootpublication.StableIdentity{}) {
		t.Fatal("nil registry unexpectedly stamped public registered identity")
	}
	ready := make(chan struct{})
	start := make(chan struct{})
	stop := make(chan struct{})
	done := make(chan bool)
	var reader sync.WaitGroup
	reader.Add(1)
	go func() {
		defer reader.Done()
		close(ready)
		<-start
		unchanged := true
		for {
			identity, ok := file.RegisteredStableIdentity()
			unchanged = unchanged && identity == before && ok == hadBefore
			select {
			case <-stop:
				done <- unchanged
				return
			default:
			}
		}
	}()
	<-ready
	close(start)
	var markErr error
	for range 1000 {
		if err := manager.MarkZombie(id); err != nil {
			markErr = err
			break
		}
	}
	close(stop)
	unchanged := <-done
	reader.Wait()
	if markErr != nil || !unchanged {
		t.Fatalf("retirement mutated registered reader identity: error=%v unchanged=%t", markErr, unchanged)
	}
	retirementIdentityAssertCount(t, manager, 1)
	if err := manager.Release(set); err != nil {
		t.Fatal(err)
	}
	retirementIdentityAssertCount(t, manager, 0)
}

func TestManagerRetirementIdentityRemovedParent(t *testing.T) {
	for _, mode := range []string{"Release", "RemoveSegment", "RemoveSegmentExpectedIdentity", "RemoveSegmentIfUnpinned", "RemoveSegmentForce"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			id := writeTestSegment(t, dir, 0, 1, 1, []byte("absent parent owner"))
			manager, err := NewManager(dir)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = manager.Close() })
			manager.mu.RLock()
			file := manager.files[id]
			manager.mu.RUnlock()
			identity := retirementIdentityAtPath(t, file.Path)
			var set *Set
			if mode == "Release" {
				set = manager.CurrentSetNoRefresh()
			}
			if err := manager.MarkZombie(id); err != nil {
				t.Fatal(err)
			}
			retirementIdentityAssertCount(t, manager, 1)
			// Retirement already owns an exact saved identity. Close the handle
			// before external unlink on every platform, including Windows.
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(file.Path); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(dir); err != nil {
				t.Fatal(err)
			}
			if mode == "Release" {
				err = manager.Release(set)
			} else {
				err = retirementIdentityRemove(manager, file, identity, mode)
			}
			if err != nil {
				t.Fatalf("%s refused an already-absent file and parent: %v", mode, err)
			}
			retirementIdentityAssertCount(t, manager, 0)
			manager.mu.RLock()
			tracked := manager.files[id]
			manager.mu.RUnlock()
			if tracked != nil {
				t.Fatal("already-absent file retained a retirement owner")
			}
		})
	}
}
