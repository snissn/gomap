package nativewire

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

func TestNativewireYCSBForcedPointerPublicationReadability(t *testing.T) {
	const docCount = 64
	dir := t.TempDir()
	opts := forcedPointerYCSBNativewireOptions(dir)
	keys, docs := forcedPointerYCSBDocuments(t, docCount)
	samples := []int{0, docCount / 2, docCount - 1}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	env := newForcedPointerYCSBNativewireEnv(t, opts, true)
	clients, handles, cleanups := forcedPointerYCSBNativewireClients(t, ctx, env, ycsbBenchClients)
	loadForcedPointerYCSBThroughNativewire(t, ctx, clients, handles, keys, docs)
	requireNativewireYCSBReadable(t, ctx, clients[0], handles[0], keys, docs, samples, "before flush")

	if err := clients[0].FlushAll(ctx); err != nil {
		t.Fatalf("FlushAll: %v", err)
	}
	if err := clients[0].Checkpoint(ctx); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	requirePublishedValueLogFiles(t, env.server.backend)
	requireValueLogContains(t, dir, docs[0], docs[len(docs)-1])
	requireNativewireYCSBReadable(t, ctx, clients[1], handles[1], keys, docs, samples, "after flush checkpoint")

	closeForcedPointerYCSBNativewireClients(t, cleanups)
	if err := env.cleanup(); err != nil {
		t.Fatalf("close before reopen: %v", err)
	}

	reopened := newForcedPointerYCSBNativewireEnv(t, opts, false)
	defer func() {
		if err := reopened.cleanup(); err != nil {
			t.Fatalf("close reopened env: %v", err)
		}
	}()
	reopenedClient, reopenedCleanup := newYCSBBenchClient(t, ctx, reopened, "inproc")
	defer func() {
		if err := reopenedCleanup(); err != nil {
			t.Fatalf("close reopened client: %v", err)
		}
	}()
	reopenedHandle, err := reopenedClient.OpenCollection(ctx, ycsbBenchCollection)
	if err != nil {
		t.Fatalf("reopened OpenCollection: %v", err)
	}
	requireNativewireYCSBReadable(t, ctx, reopenedClient, reopenedHandle, keys, docs, samples, "after reopen")
	requirePublishedValueLogFiles(t, reopened.server.backend)
}

func TestForcedPointerYCSBNativewirePhaseContextsAreIndependent(t *testing.T) {
	var phaseOne context.Context
	runForcedPointerYCSBNativewirePhase(t, func(ctx context.Context) {
		phaseOne = ctx
	})
	if !errors.Is(phaseOne.Err(), context.Canceled) {
		t.Fatalf("phase one context err=%v want context canceled", phaseOne.Err())
	}

	runForcedPointerYCSBNativewirePhase(t, func(phaseTwo context.Context) {
		if phaseTwo == phaseOne {
			t.Fatal("phase two reused phase one context")
		}
		if err := phaseTwo.Err(); err != nil {
			t.Fatalf("phase two inherited phase one cancellation: %v", err)
		}
	})
}

func TestNativewireYCSBCurrentWritableValueLogReadBarrier(t *testing.T) {
	const docCount = 16
	dir := t.TempDir()
	opts := forcedPointerYCSBNativewireOptions(dir)
	keys, docs := forcedPointerYCSBDocuments(t, docCount)
	samples := []int{0, docCount - 1}

	runForcedPointerYCSBNativewirePhase(t, func(ctx context.Context) {
		env := newForcedPointerYCSBNativewireCurrentWritableEnv(t, opts, true)
		defer func() {
			if err := env.cleanup(); err != nil {
				t.Fatalf("close readable env: %v", err)
			}
		}()
		clients, handles, cleanups := forcedPointerYCSBNativewireClients(t, ctx, env, 1)
		defer closeForcedPointerYCSBNativewireClients(t, cleanups)
		loadForcedPointerYCSBThroughNativewire(t, ctx, clients, handles, keys, docs)
		requirePublishedValueLogFiles(t, env.server.backend)
		requireNativewireYCSBReadable(t, ctx, clients[0], handles[0], keys, docs, samples, "current-writable before flush")
	})

	runForcedPointerYCSBNativewirePhase(t, func(ctx context.Context) {
		faultDir := t.TempDir()
		faultOpts := forcedPointerYCSBNativewireOptions(faultDir)
		faultEnv := newForcedPointerYCSBNativewireCurrentWritableEnv(t, faultOpts, true)
		defer func() {
			if err := faultEnv.cleanup(); err != nil {
				t.Fatalf("close fault env: %v", err)
			}
		}()
		faultClients, faultHandles, faultCleanups := forcedPointerYCSBNativewireClients(t, ctx, faultEnv, 1)
		defer closeForcedPointerYCSBNativewireClients(t, faultCleanups)
		loadForcedPointerYCSBThroughNativewire(t, ctx, faultClients, faultHandles, keys, docs)
		requirePublishedValueLogFiles(t, faultEnv.server.backend)

		var barrierCalls atomic.Int32
		faultEnv.server.backend.SetCurrentValueLogReadBarrierWithSize(func(fileID uint32) (int64, error) {
			barrierCalls.Add(1)
			return -1, io.ErrUnexpectedEOF
		})

		_, directErr := faultEnv.server.handleGetManyBody(&connState{}, []iwire.Section{
			collectionNameRef(ycsbBenchCollection),
			{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, keys[docCount-1])},
		}, nil, ReadMetadata{})
		if directErr == nil {
			t.Fatalf("direct GetMany handler succeeded through injected current-writable read-boundary EOF")
		}
		if !errors.Is(directErr, io.ErrUnexpectedEOF) {
			t.Fatalf("direct GetMany handler err=%v want injected unexpected EOF", directErr)
		}
		if errors.Is(directErr, collections.ErrCommitAmbiguous) {
			t.Fatalf("direct GetMany handler err=%v unexpectedly classified as ErrCommitAmbiguous", directErr)
		}
		if !strings.Contains(directErr.Error(), "nativewire metadata") || !strings.Contains(directErr.Error(), "unexpected EOF") {
			t.Fatalf("direct GetMany handler err=%q missing nativewire unexpected EOF context", directErr)
		}

		got, present, err := faultClients[0].GetMany(ctx, ycsbBenchCollection, [][]byte{keys[docCount-1]})
		if err == nil {
			t.Fatalf("GetMany succeeded through injected current-writable read-boundary EOF: docs=%d present=%v", len(got), present)
		}
		if !isRemoteError(err, iwire.ErrInternal) {
			t.Fatalf("GetMany err=%v want remote internal error", err)
		}
		if calls := barrierCalls.Load(); calls == 0 {
			t.Fatalf("current-writable read barrier was not reached")
		}
	})
}

func runForcedPointerYCSBNativewirePhase(t testing.TB, phase func(context.Context)) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	phase(ctx)
}

func forcedPointerYCSBNativewireOptions(dir string) treedb.Options {
	opts := treedb.OptionsFor(treedb.ProfileCommandWALRelaxed, dir)
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true
	opts.ValueLog.Compression = treedb.ValueLogCompressionOff
	opts.ValueLog.Generational.Policy = treedb.ValueLogGenerationOff
	return opts
}

func newForcedPointerYCSBNativewireEnv(t testing.TB, opts treedb.Options, create bool) *ycsbBenchEnv {
	t.Helper()
	return newForcedPointerYCSBNativewireEnvWithOptions(t, opts, create, collections.CollectionOptions{
		DocumentFormat: collections.DocumentFormatBSON,
	})
}

func newForcedPointerYCSBNativewireCurrentWritableEnv(t testing.TB, opts treedb.Options, create bool) *ycsbBenchEnv {
	t.Helper()
	return newForcedPointerYCSBNativewireEnvWithOptions(t, opts, create, collections.CollectionOptions{
		DocumentFormat:                   collections.DocumentFormatBSON,
		BufferedIndexedWriteMaxDocuments: 1,
		DisableBufferedIndexedAsyncFlush: true,
	})
}

func newForcedPointerYCSBNativewireEnvWithOptions(t testing.TB, opts treedb.Options, create bool, collectionOptions collections.CollectionOptions) *ycsbBenchEnv {
	t.Helper()
	db, cleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		t.Fatalf("OpenBackendWithCachedLeafLog: %v", err)
	}
	mgr := collections.NewCollectionManager(db)
	if create {
		if _, err := mgr.CreateCollection(&collections.CollectionMeta{
			Name:    ycsbBenchCollection,
			Options: collectionOptions,
			Indexes: []collections.IndexDefinition{
				{
					Name:      ycsbBenchKeyIndex,
					Field:     ycsbBenchKeyField,
					ValueType: collections.IndexValueString,
					Unique:    true,
				},
			},
		}); err != nil {
			_ = cleanup()
			t.Fatalf("CreateCollection: %v", err)
		}
	}
	server := NewServer(ServerOptions{Collections: mgr, Backend: db})
	return &ycsbBenchEnv{
		server: server,
		cleanup: func() error {
			return errors.Join(server.Close(), cleanup())
		},
	}
}

func forcedPointerYCSBDocuments(t *testing.T, count int) ([][]byte, [][]byte) {
	t.Helper()
	keys := make([][]byte, count)
	docs := make([][]byte, count)
	for i := 0; i < count; i++ {
		key := ycsbBenchKey(i)
		doc, err := encodeYCSBBSONBinaryDocument(key)
		if err != nil {
			t.Fatalf("encode %s: %v", key, err)
		}
		keys[i] = []byte(key)
		docs[i] = doc
	}
	return keys, docs
}

func forcedPointerYCSBNativewireClients(t *testing.T, ctx context.Context, env *ycsbBenchEnv, count int) ([]*Client, []CollectionHandle, []func() error) {
	t.Helper()
	clients := make([]*Client, count)
	handles := make([]CollectionHandle, count)
	cleanups := make([]func() error, count)
	for i := 0; i < count; i++ {
		client, cleanup := newYCSBBenchClient(t, ctx, env, "inproc")
		handle, err := client.OpenCollection(ctx, ycsbBenchCollection)
		if err != nil {
			_ = cleanup()
			t.Fatalf("client %d OpenCollection: %v", i, err)
		}
		clients[i] = client
		handles[i] = handle
		cleanups[i] = cleanup
	}
	return clients, handles, cleanups
}

func closeForcedPointerYCSBNativewireClients(t *testing.T, cleanups []func() error) {
	t.Helper()
	for i, cleanup := range cleanups {
		if cleanup == nil {
			continue
		}
		if err := cleanup(); err != nil {
			t.Fatalf("close client %d: %v", i, err)
		}
		cleanups[i] = nil
	}
}

func loadForcedPointerYCSBThroughNativewire(t *testing.T, ctx context.Context, clients []*Client, handles []CollectionHandle, keys, docs [][]byte) {
	t.Helper()
	if len(clients) == 0 || len(clients) != len(handles) {
		t.Fatalf("invalid client/handle sets: clients=%d handles=%d", len(clients), len(handles))
	}
	errCh := make(chan error, len(clients))
	base := 0
	for worker := range clients {
		count := len(keys) / len(clients)
		if worker < len(keys)%len(clients) {
			count++
		}
		start := base
		base += count
		worker := worker
		go func() {
			for i := 0; i < count; i++ {
				docIndex := start + i
				key := string(keys[docIndex])
				ids := [][]byte{keys[docIndex]}
				payloads := [][]byte{docs[docIndex]}
				if err := ycsbInsertBatchNoResultIDs(ctx, clients[worker], handles[worker], collections.DocumentFormatBSON, ids, payloads); err != nil {
					errCh <- classifyForcedPointerYCSBInsertError(key, err)
					return
				}
			}
			errCh <- nil
		}()
	}
	for range clients {
		if err := <-errCh; err != nil {
			t.Fatal(err)
		}
	}
}

func classifyForcedPointerYCSBInsertError(key string, err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, io.ErrClosedPipe):
		return fmt.Errorf("insert %s returned connection EOF/closed-pipe error: %w", key, err)
	case isRemoteError(err, iwire.ErrCommitAmbiguous):
		return fmt.Errorf("insert %s returned commit ambiguous: %w", key, err)
	default:
		return fmt.Errorf("insert %s: %w", key, err)
	}
}

func requireNativewireYCSBReadable(t *testing.T, ctx context.Context, client *Client, handle CollectionHandle, keys, docs [][]byte, indexes []int, phase string) {
	t.Helper()
	ids := make([][]byte, len(indexes))
	wantDocs := make([][]byte, len(indexes))
	for i, index := range indexes {
		ids[i] = keys[index]
		wantDocs[i] = docs[index]
	}
	got, present, err := client.GetMany(ctx, ycsbBenchCollection, ids)
	if err != nil {
		t.Fatalf("%s GetMany: %v", phase, err)
	}
	requireNativewireDocuments(t, got, present, wantDocs)

	handleGot, handlePresent, err := client.GetManyHandle(ctx, handle, ids)
	if err != nil {
		t.Fatalf("%s GetManyHandle: %v", phase, err)
	}
	requireNativewireDocuments(t, handleGot, handlePresent, wantDocs)

	for _, index := range indexes {
		key := string(keys[index])
		indexIDs, truncated, err := client.IndexLookup(ctx, ycsbBenchCollection, ycsbBenchKeyIndex, key, CursorLimits{MaxItems: 2})
		if err != nil {
			t.Fatalf("%s IndexLookup(%s): %v", phase, key, err)
		}
		if truncated {
			t.Fatalf("%s IndexLookup(%s) unexpectedly truncated", phase, key)
		}
		if len(indexIDs) != 1 || !bytes.Equal(indexIDs[0], keys[index]) {
			t.Fatalf("%s IndexLookup(%s) ids=%q want [%q]", phase, key, indexIDs, keys[index])
		}
	}
}

func requireNativewireDocuments(t *testing.T, got [][]byte, present []bool, want [][]byte) {
	t.Helper()
	if len(got) != len(want) || len(present) != len(want) {
		t.Fatalf("documents len=%d present len=%d want %d", len(got), len(present), len(want))
	}
	for i := range want {
		wantPresent := want[i] != nil
		if present[i] != wantPresent {
			t.Fatalf("present[%d]=%v want %v", i, present[i], wantPresent)
		}
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("document[%d]=%q want %q", i, got[i], want[i])
		}
	}
}

func requirePublishedValueLogFiles(t *testing.T, db *backenddb.DB) {
	t.Helper()
	state := db.State()
	if state == nil || state.ValueLogSet == nil || len(state.ValueLogSet.Files) == 0 {
		t.Fatalf("published state missing value-log files: %+v", state)
	}
}

func requireValueLogContains(t *testing.T, dir string, values ...[]byte) {
	t.Helper()
	paths := valueLogSegmentPaths(t, dir)
	if len(paths) == 0 {
		t.Fatalf("no value-log files under %s", dir)
	}
	remaining := append([][]byte(nil), values...)
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read value log %s: %v", path, err)
		}
		kept := remaining[:0]
		for _, value := range remaining {
			if !bytes.Contains(data, value) {
				kept = append(kept, value)
			}
		}
		remaining = kept
		if len(remaining) == 0 {
			return
		}
	}
	t.Fatalf("value log did not contain %d inserted document payloads", len(remaining))
}

func valueLogSegmentPaths(t *testing.T, dir string) []string {
	t.Helper()
	var paths []string
	for _, candidate := range []string{
		backenddb.ValueLogDirPath(dir),
		backenddb.ValueLogDirPath(filepath.Join(dir, "maindb")),
	} {
		matches, err := filepath.Glob(filepath.Join(candidate, "value-l*-*.log"))
		if err != nil {
			t.Fatalf("glob value log %s: %v", candidate, err)
		}
		paths = append(paths, matches...)
	}
	return paths
}
