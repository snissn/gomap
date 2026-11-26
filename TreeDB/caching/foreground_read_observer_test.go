package caching

import (
	"sync"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestForegroundReadObserverHooksConcurrentSwapAndUnregisterBalanced(t *testing.T) {
	backend, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer func() {
		if err := backend.Close(); err != nil {
			t.Errorf("close backend: %v", err)
		}
	}()
	if err := backend.SetSync([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("SetSync: %v", err)
	}

	cdb1, err := Open(t.TempDir(), backend, Options{FlushThreshold: 1 << 20})
	if err != nil {
		t.Fatalf("open caching db 1: %v", err)
	}
	defer func() {
		if err := cdb1.Close(); err != nil {
			t.Errorf("close caching db 1: %v", err)
		}
	}()
	cdb2 := &DB{}

	stop := make(chan struct{})
	var swapWG sync.WaitGroup
	swapWG.Add(1)
	go func() {
		defer swapWG.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			var unreg func()
			if i%2 == 0 {
				unreg = backend.RegisterForegroundReadObserverHooks(cdb1.noteRead, cdb1.beginForegroundRead, cdb1.endForegroundRead)
			} else {
				unreg = backend.RegisterForegroundReadObserverHooks(cdb2.noteRead, cdb2.beginForegroundRead, cdb2.endForegroundRead)
			}
			if i%3 == 0 {
				unreg()
			}
		}
	}()

	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			dst := make([]byte, 0, 32)
			for i := 0; i < 400; i++ {
				switch (id + i) % 4 {
				case 0:
					snap := backend.AcquireSnapshot()
					if snap == nil {
						t.Errorf("AcquireSnapshot=nil")
						return
					}
					snap.MarkForegroundRead()
					if err := snap.Close(); err != nil {
						t.Errorf("Snapshot.Close: %v", err)
						return
					}
				case 1:
					snap := backend.AcquireSnapshot()
					if snap == nil {
						t.Errorf("AcquireSnapshot=nil")
						return
					}
					snap.MarkForegroundRead()
					snap.DetachForegroundRead()
					if err := snap.Close(); err != nil {
						t.Errorf("Snapshot.Close: %v", err)
						return
					}
				case 2:
					end := backend.BeginForegroundRead()
					end()
					end()
				case 3:
					if err := backend.WithOneShotRead(func(view backenddb.OneShotReadView) error {
						state, ok := view.StateToken()
						if !ok {
							return backenddb.ErrClosed
						}
						var readErr error
						dst, _, readErr = view.GetAppendAtRoot(state.RootPageID, []byte("k"), dst[:0])
						return readErr
					}); err != nil {
						t.Errorf("WithOneShotRead: %v", err)
						return
					}
				}
			}
		}(g)
	}

	wg.Wait()
	close(stop)
	swapWG.Wait()

	if got := cdb1.activeForegroundIterators.Load(); got != 0 {
		t.Fatalf("cdb1.activeForegroundIterators=%d, want 0", got)
	}
	if got := cdb2.activeForegroundIterators.Load(); got != 0 {
		t.Fatalf("cdb2.activeForegroundIterators=%d, want 0", got)
	}
}
