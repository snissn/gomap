package caching

import (
	"bytes"
	"path/filepath"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestOpenSeparateCacheDirKeepsPersistentLogsUnderBackendRoot(t *testing.T) {
	root := t.TempDir()
	cacheDir := filepath.Join(root, "cache")
	backendDir := filepath.Join(root, "backend")
	backend, err := backenddb.Open(backenddb.Options{
		Dir:                    backendDir,
		DisableBackgroundPrune: true,
		ValueLog: backenddb.ValueLogOptions{
			PointerThreshold: 1,
		},
	})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}

	cached, err := Open(cacheDir, backend, Options{
		DisableWAL:               true,
		AllowUnsafe:              true,
		FlushThreshold:           1 << 20,
		MemtableShards:           1,
		JournalLanes:             1,
		ValueLogPointerThreshold: 1,
		ValueLogGenerationPolicy: uint8(backenddb.ValueLogGenerationOff),
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("open cache: %v", err)
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = cached.Close()
		}
	})

	if got, want := cached.valueLogDir, backenddb.ValueLogDirPath(backendDir); got != want {
		t.Fatalf("valueLogDir=%q want backend-owned %q", got, want)
	}
	if got, want := cached.leafLogDir, backenddb.LeafLogDirPath(backendDir); got != want {
		t.Fatalf("leafLogDir=%q want backend-owned %q", got, want)
	}

	key := []byte("durable/separate-layout")
	want := bytes.Repeat([]byte("v"), 1024)
	if err := cached.Set(key, want); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := cached.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := cached.Close(); err != nil {
		t.Fatalf("close cache: %v", err)
	}
	closed = true

	reopened, err := backenddb.Open(backenddb.Options{
		Dir:                    backendDir,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("standalone backend reopen: %v", err)
	}
	defer reopened.Close()
	got, err := reopened.Get(key)
	if err != nil {
		t.Fatalf("Get after standalone backend reopen: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("value after standalone backend reopen differs: got=%d bytes want=%d", len(got), len(want))
	}
}
