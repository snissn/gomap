package valuelog

import (
	"os"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestManagerCloseJoinsPinnedZombieRetry5066(t *testing.T) {
	dir := t.TempDir()
	id, path := writeIdentityPinTestSegment(t, dir)
	manager, err := NewManagerWithStableResourcePinRegistry(dir, rootpublication.NewIdentityPinRegistry())
	if err != nil {
		t.Fatal(err)
	}
	token := stableManagerToken(t, manager, id)
	defer token.Release()
	entered, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	manager.retryWaitHook = func() { once.Do(func() { close(entered); <-resume }) }
	set := manager.CurrentSetNoRefresh()
	manager.mu.RLock()
	file := manager.files[id]
	manager.mu.RUnlock()
	if err := manager.MarkZombie(id); err != nil {
		t.Fatal(err)
	}
	if err := manager.Release(set); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("retry never reached pin-blocked wait")
	}
	closeResult := make(chan error, 1)
	go func() { closeResult <- manager.Close() }()
	select {
	case err := <-closeResult:
		close(resume)
		t.Fatalf("Close returned while owned retry was still blocked (pending=%t, err=%v)", file.retryDeletePending.Load(), err)
	case <-time.After(100 * time.Millisecond):
	}
	close(resume)
	select {
	case err := <-closeResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not join canceled retry")
	}
	if file.retryDeletePending.Load() {
		t.Fatal("retry still pending after Close")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("pinned file removed: %v", err)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
}
