package db

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
)

type recordingDurableRootSinkV1 struct {
	events *[]string
}

func (sink recordingDurableRootSinkV1) WritePage(uint64, []byte) error {
	*sink.events = append(*sink.events, "meta-write")
	return nil
}

func testDurableMetaV1(t testing.TB) page.DurableMetaV1 {
	t.Helper()
	var digest [32]byte
	digest[0] = 1
	meta, err := page.NewDurableMetaV1(1, 1, 2, digest)
	if err != nil {
		t.Fatalf("new durable meta: %v", err)
	}
	return meta
}

func TestExecuteDurableRootStorageTransactionOrdersIndexBeforeMeta(t *testing.T) {
	events := make([]string, 0, 4)
	mutated, err := executeDurableRootStorageTransactionV1(durableRootStorageTransactionV1{
		materialize: func() error {
			events = append(events, "materialize")
			return nil
		},
		syncIndex: func() error {
			events = append(events, "index-sync")
			return nil
		},
		sink:   recordingDurableRootSinkV1{events: &events},
		target: MetaPage0ID,
		meta:   testDurableMetaV1(t),
		syncMeta: func() error {
			events = append(events, "meta-sync")
			return nil
		},
	})
	if err != nil {
		t.Fatalf("execute transaction: %v", err)
	}
	if !mutated {
		t.Fatal("successful transaction did not report meta mutation")
	}
	want := []string{"materialize", "index-sync", "meta-write", "meta-sync"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events=%v want=%v", events, want)
	}
}

func TestExecuteDurableRootStorageTransactionObservesExactDependencySyncBoundary(t *testing.T) {
	root := t.TempDir()
	dependencyPath := filepath.Join(root, "value_vlog", "value-l0-000001.log")
	if err := os.MkdirAll(filepath.Dir(dependencyPath), 0o755); err != nil {
		t.Fatal(err)
	}
	dependency, err := os.OpenFile(dependencyPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer dependency.Close()
	if _, err := dependency.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}

	events := make([]string, 0, 7)
	token, err := rootpublication.NewStableResourceToken(rootpublication.StableResourceSpec{
		Kind: rootpublication.ResourceValueLog, LogicalLane: "main", ResourceID: "1", Generation: 1,
		DiagnosticPath: "value_vlog/value-l0-000001.log", File: dependency,
		Frontier: rootpublication.DurableFrontier{Bytes: 1}, Reachability: rootpublication.ReachabilityValueLogPointer,
		SyncThrough: func(file *os.File, _ rootpublication.DurableFrontier) error {
			events = append(events, "resource-sync")
			return file.Sync()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	builder := rootpublication.NewStableResourceSetBuilder(rootpublication.ReachabilityValueLogPointer)
	if err := builder.Add(token); err != nil {
		token.Release()
		t.Fatal(err)
	}
	resources, err := builder.Freeze()
	if err != nil {
		builder.Abandon()
		t.Fatal(err)
	}
	defer resources.Release()

	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		switch event.Point {
		case durabilitycut.BeforeDependencyFileSync:
			events = append(events, "before-dependency-sync")
		case durabilitycut.AfterDependencyFileSync:
			events = append(events, "after-dependency-sync")
		default:
			return nil
		}
		if !reflect.DeepEqual(event.Paths, []string{dependencyPath}) {
			t.Fatalf("dependency observation paths=%v want exact retained-handle path %q", event.Paths, dependencyPath)
		}
		return nil
	})
	defer restore()

	mutated, err := executeDurableRootStorageTransactionV1(durableRootStorageTransactionV1{
		resources: resources,
		materialize: func() error {
			events = append(events, "materialize")
			return nil
		},
		syncIndex: func() error {
			events = append(events, "index-sync")
			return nil
		},
		sink: recordingDurableRootSinkV1{events: &events}, target: MetaPage0ID, meta: testDurableMetaV1(t),
		syncMeta: func() error {
			events = append(events, "meta-sync")
			return nil
		},
		dir: root, indexPath: filepath.Join(root, "index.db"),
	})
	if err != nil {
		t.Fatalf("execute transaction: %v", err)
	}
	if !mutated {
		t.Fatal("successful transaction did not report meta mutation")
	}
	want := []string{"before-dependency-sync", "resource-sync", "after-dependency-sync", "materialize", "index-sync", "meta-write", "meta-sync"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events=%v want=%v", events, want)
	}
}

func TestExecuteDurableRootStorageTransactionClassifiesMetaSyncFailureAmbiguous(t *testing.T) {
	wantErr := errors.New("meta sync failed")
	events := make([]string, 0, 2)
	mutated, err := executeDurableRootStorageTransactionV1(durableRootStorageTransactionV1{
		syncIndex: func() error { return nil },
		sink:      recordingDurableRootSinkV1{events: &events},
		target:    MetaPage0ID,
		meta:      testDurableMetaV1(t),
		syncMeta:  func() error { return wantErr },
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("error=%v want %v", err, wantErr)
	}
	if !mutated {
		t.Fatal("post-meta sync failure was classified retryable")
	}
}

func TestExecuteDurableRootStorageTransactionClassifiesIndexSyncFailureRetryable(t *testing.T) {
	wantErr := errors.New("index sync failed")
	events := make([]string, 0, 1)
	mutated, err := executeDurableRootStorageTransactionV1(durableRootStorageTransactionV1{
		syncIndex: func() error { return wantErr },
		sink:      recordingDurableRootSinkV1{events: &events},
		target:    MetaPage0ID,
		meta:      testDurableMetaV1(t),
		syncMeta:  func() error { return nil },
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("error=%v want %v", err, wantErr)
	}
	if mutated {
		t.Fatal("pre-meta index sync failure was classified ambiguous")
	}
	if len(events) != 0 {
		t.Fatalf("meta write occurred after index sync failure: %v", events)
	}
}
