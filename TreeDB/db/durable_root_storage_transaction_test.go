package db

import (
	"errors"
	"reflect"
	"testing"

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
