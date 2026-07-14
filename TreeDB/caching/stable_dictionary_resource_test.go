//go:build !windows

package caching

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/dictdb"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

type failingStableDictionaryStore struct {
	dictID       uint64
	dict         []byte
	captureErr   error
	captureCalls int
}

func TestStableValueLogAppendMergesReusedDictionaryClosure(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	cached, err := Open(dir, backend, Options{
		IndexOuterLeavesInValueLog:          true,
		ValueLogCompression:                 uint8(vlogCompressionDict),
		ValueLogDictIncompressibleHoldBytes: -1,
		RelaxedSync:                         true,
		AllowUnsafe:                         true,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("open cache: %v", err)
	}
	defer cached.Close()

	dictionaryStore, err := dictdb.Open(dir+"/dictdb", backenddb.Options{ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("open dictionary store: %v", err)
	}
	defer dictionaryStore.Close()
	samples := [][]byte{
		queueTestValue('s', 20<<10, 1),
		queueTestValue('s', 20<<10, 2),
		queueTestValue('s', 20<<10, 3),
		queueTestValue('s', 20<<10, 4),
	}
	dictionary := buildQueueTestDict(t, 7, samples)
	dictID, err := dictionaryStore.PutDictBytes(context.Background(), dictionary)
	if err != nil {
		t.Fatalf("put dictionary: %v", err)
	}
	reusedID, err := dictionaryStore.PutDictBytes(context.Background(), dictionary)
	if err != nil || reusedID != dictID {
		t.Fatalf("reuse dictionary id=%d err=%v want %d", reusedID, err, dictID)
	}
	cached.SetDictStore(dictionaryStore)

	ptrs, resources, err := cached.appendValueLogWithStableResources(
		&cached.leafLog,
		dictID,
		dictionary,
		[]valuelog.Record{{RID: 1, Value: samples[0]}},
		journalDurabilityNone,
	)
	if err != nil {
		t.Fatalf("stable append: %v", err)
	}
	if len(ptrs) != 1 || resources == nil {
		t.Fatalf("stable append ptrs=%v resources=%v", ptrs, resources)
	}
	defer resources.Release()
	var hasDictionary, hasOuterLeaf bool
	for _, descriptor := range resources.Descriptors() {
		for _, field := range descriptor.ReachabilityFields() {
			switch field {
			case rootpublication.ReachabilityDictionaryGeneration:
				hasDictionary = true
			case rootpublication.ReachabilityOuterLeafRawPointer:
				hasOuterLeaf = true
			}
		}
	}
	if !hasDictionary || !hasOuterLeaf {
		t.Fatalf("merged stable closure dictionary=%v outer-leaf=%v", hasDictionary, hasOuterLeaf)
	}
}

func TestStableValueLogAppendRejectsDictionaryBytesOutsideCapturedClosure(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	cached, err := Open(dir, backend, Options{
		IndexOuterLeavesInValueLog:          true,
		ValueLogCompression:                 uint8(vlogCompressionDict),
		ValueLogDictIncompressibleHoldBytes: -1,
		RelaxedSync:                         true,
		AllowUnsafe:                         true,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("open cache: %v", err)
	}
	defer cached.Close()

	dictionaryStore, err := dictdb.Open(dir+"/dictdb", backenddb.Options{ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("open dictionary store: %v", err)
	}
	defer dictionaryStore.Close()
	dictionary := bytes.Repeat([]byte("captured-dictionary|"), 128)
	dictID, err := dictionaryStore.PutDictBytes(context.Background(), dictionary)
	if err != nil {
		t.Fatalf("put dictionary: %v", err)
	}
	cached.SetDictStore(dictionaryStore)
	before, err := os.ReadDir(cached.leafLogDir)
	if err != nil {
		t.Fatalf("read leaf-log dir before append: %v", err)
	}
	ptrs, resources, err := cached.appendValueLogWithStableResources(
		&cached.leafLog,
		dictID,
		bytes.Repeat([]byte("different-dictionary|"), 128),
		[]valuelog.Record{{RID: 1, Value: bytes.Repeat([]byte("payload|"), 1024)}},
		journalDurabilityNone,
	)
	if !errors.Is(err, rootpublication.ErrResourceConflict) {
		t.Fatalf("stable append error=%v want resource conflict", err)
	}
	if ptrs != nil || resources != nil {
		t.Fatalf("failed stable append ptrs=%v resources=%v want nil", ptrs, resources)
	}
	after, err := os.ReadDir(cached.leafLogDir)
	if err != nil {
		t.Fatalf("read leaf-log dir after append: %v", err)
	}
	if len(after) != len(before) {
		t.Fatalf("dictionary mismatch mutated writer namespace: before=%d after=%d", len(before), len(after))
	}
}

func (store *failingStableDictionaryStore) GetCurrent(context.Context) (uint64, error) {
	return store.dictID, nil
}

func (store *failingStableDictionaryStore) GetDictBytes(context.Context, uint64) ([]byte, error) {
	return append([]byte(nil), store.dict...), nil
}

func (store *failingStableDictionaryStore) CaptureDictionaryResources(context.Context, uint64) (*rootpublication.StableResourceSet, error) {
	store.captureCalls++
	return nil, store.captureErr
}

func TestStableValueLogAppendCapturesSelectedDictionaryBeforeWriterMutation(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	cached, err := Open(dir, backend, Options{
		IndexOuterLeavesInValueLog:          true,
		ValueLogCompression:                 uint8(vlogCompressionDict),
		ValueLogDictIncompressibleHoldBytes: -1,
		RelaxedSync:                         true,
		AllowUnsafe:                         true,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("open cache: %v", err)
	}
	defer cached.Close()

	injected := errors.New("injected dictionary authority failure")
	store := &failingStableDictionaryStore{
		dictID:     7,
		dict:       bytes.Repeat([]byte("dictionary"), 64),
		captureErr: injected,
	}
	cached.SetDictStore(store)
	before, err := os.ReadDir(cached.leafLogDir)
	if err != nil {
		t.Fatalf("read leaf-log dir before append: %v", err)
	}
	ptrs, resources, err := cached.appendValueLogWithStableResources(
		&cached.leafLog,
		store.dictID,
		store.dict,
		[]valuelog.Record{{RID: 1, Value: bytes.Repeat([]byte("payload|"), 1024)}},
		journalDurabilityNone,
	)
	if !errors.Is(err, injected) {
		t.Fatalf("stable append error=%v want injected failure", err)
	}
	if ptrs != nil || resources != nil {
		t.Fatalf("failed stable append ptrs=%v resources=%v want nil", ptrs, resources)
	}
	if store.captureCalls != 1 {
		t.Fatalf("dictionary capture calls=%d want 1", store.captureCalls)
	}
	after, err := os.ReadDir(cached.leafLogDir)
	if err != nil {
		t.Fatalf("read leaf-log dir after append: %v", err)
	}
	if len(after) != len(before) {
		t.Fatalf("provider failure mutated writer namespace: before=%d after=%d", len(before), len(after))
	}
}
