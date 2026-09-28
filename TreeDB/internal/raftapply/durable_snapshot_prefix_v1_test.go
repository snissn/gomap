package raftapply

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

func TestDurableSnapshotPrefixV1RetainsExactCutThroughAppendAndClose(t *testing.T) {
	root := t.TempDir()
	database := openApplyHarnessDB(t, filepath.Join(root, "db"))
	defer database.Close()
	progress, results := openDurableApplyStoresForTest(t, filepath.Join(root, "apply"), DurableApplyStoreOptions{DisableSync: true})
	defer closeDurableApplyStoresForTest(t, progress, results)
	apply := func(index uint64, name string) {
		version := uint64(testCatalogVersionStart) + index - 1
		raw := deterministicCreateCollectionEntryWithCatalogVersion(t, name, "prefix:"+name, version, testCreateCollectionMetaOptions{})
		if result, err := ApplyCommittedEntryV1(database, raw, applyMetaWithCatalogVersion(1, index, version), Options{ProgressStore: progress, ResultStore: results}); err != nil {
			t.Fatalf("apply: %+v %v", result, err)
		}
	}
	apply(1, "first")
	p, err := progress.CaptureSnapshotPrefixV1()
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	r, err := results.CaptureSnapshotPrefixV1()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	wantProgress, err := os.ReadFile(progress.path)
	if err != nil {
		t.Fatal(err)
	}
	wantResult, err := os.ReadFile(results.path)
	if err != nil {
		t.Fatal(err)
	}
	apply(2, "second")
	closeDurableApplyStoresForTest(t, progress, results)
	for _, test := range []struct {
		name string
		cut  *DurableSnapshotPrefixV1
		want []byte
	}{{"progress", p, wantProgress}, {"result", r, wantResult}} {
		t.Run(test.name, func(t *testing.T) {
			var got bytes.Buffer
			if err := test.cut.WriteToContext(context.Background(), &got); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got.Bytes(), test.want) || test.cut.SizeBytes() != int64(len(test.want)) {
				t.Fatal("metadata prefix changed after later apply/store close")
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if err := test.cut.WriteToContext(ctx, io.Discard); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if err := test.cut.Close(); err != nil {
				t.Fatal(err)
			}
			if err := test.cut.Close(); err != nil {
				t.Fatal(err)
			}
			if err := test.cut.WriteToContext(context.Background(), io.Discard); !errors.Is(err, os.ErrClosed) {
				t.Fatal(err)
			}
		})
	}
}

func TestDurableSnapshotPrefixV1RefusesPoisonedAppend(t *testing.T) {
	progress, err := OpenDurableApplyProgressStore(t.TempDir(), DurableApplyStoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer progress.Close()
	if err := progress.file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := progress.RecordApplied(ApplyProgressRecordV1{EntryID: raftentry.ApplyEntryID{Term: 1, Index: 1}, CommandDigest: testDurableDigest(1), AppliedCommandLSN: 1}); err == nil {
		t.Fatal("append unexpectedly succeeded")
	}
	if prefix, err := progress.CaptureSnapshotPrefixV1(); err == nil {
		_ = prefix.Close()
		t.Fatal("captured poisoned append")
	}
}
