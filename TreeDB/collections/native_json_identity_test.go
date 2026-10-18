package collections

import (
	"fmt"
	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"sync"
	"testing"
)

func TestNativeJSONSharedCreateConcurrentIdentityAndNoWALReopen(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			opts := treedb.OptionsFor(profile, t.TempDir())
			opts.MemtableMode = "cow_btree"
			opts.BackgroundCheckpointInterval = -1
			database, cleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
			if err != nil {
				t.Fatal(err)
			}
			closed := false
			t.Cleanup(func() {
				if !closed {
					if err := cleanup(); err != nil {
						t.Errorf("cleanup: %v", err)
					}
				}
			})
			manager := NewCollectionManager(database)
			const count = 8
			start := make(chan struct{})
			failures := make(chan error, count)
			var wg sync.WaitGroup
			for i := 0; i < count; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					<-start
					_, err := manager.CreateCollection(&CollectionMeta{Name: fmt.Sprintf("identity-%d", i)})
					failures <- err
				}(i)
			}
			close(start)
			wg.Wait()
			close(failures)
			for err := range failures {
				if err != nil {
					t.Fatal(err)
				}
			}
			check := func(database *backenddb.DB) {
				snapshot := database.AcquireSnapshot()
				if snapshot == nil {
					t.Fatal("nil snapshot")
				}
				seen := make(map[uint64]bool, count)
				for i := 0; i < count; i++ {
					raw, present, err := getSystemValue(snapshot, nativeCatalogIncarnationPrefix+fmt.Sprintf("identity-%d", i))
					if err != nil || !present {
						t.Fatalf("identity absent: %v", err)
					}
					id, err := nativeCatalogIdentityValue(raw, false)
					if err != nil || seen[id] || id > count {
						t.Fatalf("identity %d: %v", id, err)
					}
					seen[id] = true
				}
				raw, present, err := getSystemValue(snapshot, nativeCatalogNextIncarnationKey)
				if err != nil || !present {
					t.Fatalf("counter absent: %v", err)
				}
				next, err := nativeCatalogIdentityValue(raw, true)
				if err != nil || next != count+1 {
					t.Fatalf("counter %d: %v", next, err)
				}
				if err := snapshot.Close(); err != nil {
					t.Fatal(err)
				}
			}
			check(database)
			if err := manager.Close(); err != nil {
				t.Fatal(err)
			}
			closed = true
			if err := cleanup(); err != nil {
				t.Fatal(err)
			}
			reopened, closeReopened, err := treedb.OpenBackendWithCachedLeafLog(opts)
			if err != nil {
				t.Fatal(err)
			}
			check(reopened)
			if err := closeReopened(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
