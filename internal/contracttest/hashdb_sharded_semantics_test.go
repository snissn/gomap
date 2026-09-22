package contracttest

import (
	"fmt"
	"os"
	"os/exec"
	"sync/atomic"
	"testing"
	"time"

	xxhash "github.com/cespare/xxhash/v2"
	"github.com/snissn/gomap/HashDB"
)

func keysForShard(numShards, wantShard, count int) [][]byte {
	if numShards <= 0 {
		return [][]byte{[]byte("k")}
	}
	if count <= 0 {
		return nil
	}
	out := make([][]byte, 0, count)
	for i := 0; ; i++ {
		key := []byte(fmt.Sprintf("k-%d", i))
		shard := int(xxhash.Sum64(key) % uint64(numShards))
		if shard == wantShard {
			out = append(out, key)
			if len(out) == count {
				return out
			}
		}
	}
}

func TestContract_HashDBSharded_ApplyBatchSync_IsPerShardAtomicOnError(t *testing.T) {
	dir := t.TempDir()
	db, err := openEngine("hashdb-sharded", dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	h, ok := db.(hashdbSharded)
	if !ok {
		t.Fatalf("expected hashdbSharded wrapper")
	}
	// Ensure the "too large" op stays too large.
	h.db.SetCompression(false)

	const numShards = 8
	okKeys := keysForShard(numShards, 0, 2)
	keyOK1 := okKeys[0]
	keyOK2 := okKeys[1]
	keyFail := keysForShard(numShards, 1, 1)[0]

	origMax := atomic.LoadInt64(&hashdb.MaxSegmentSize)
	atomic.StoreInt64(&hashdb.MaxSegmentSize, 1024)
	t.Cleanup(func() { atomic.StoreInt64(&hashdb.MaxSegmentSize, origMax) })

	// This large value forces the per-shard batch writer to reject the shard 1 batch.
	// Shard 0 should still fully apply.
	large := make([]byte, 8<<10)
	for i := range large {
		large[i] = byte(i*31 + 7)
	}

	b, ok := db.(batchKV)
	if !ok {
		t.Fatalf("missing batch api")
	}
	err = b.ApplyBatchSync([]hashdb.BatchOp{
		hashdb.PutOp(keyOK1, []byte("v1")),
		hashdb.PutOp(keyOK2, []byte("v2")),
		hashdb.PutOp(keyFail, large),
	})
	if err == nil {
		t.Fatalf("expected error")
	}

	got, err := db.Get(keyOK1)
	if err != nil || string(got) != "v1" {
		t.Fatalf("keyOK1: got=%q err=%v", string(got), err)
	}
	got, err = db.Get(keyOK2)
	if err != nil || string(got) != "v2" {
		t.Fatalf("keyOK2: got=%q err=%v", string(got), err)
	}
	got, err = db.Get(keyFail)
	if err != nil {
		t.Fatalf("keyFail get: %v", err)
	}
	if got != nil {
		t.Fatalf("keyFail unexpectedly applied: got %q", string(got))
	}
}

func crashWritePutSyncOverridesCache(t *testing.T, dir string) {
	t.Helper()

	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}

	cmd := exec.Command(exe, "-test.run=^TestHelperContractHashDBPutSyncOverridesCacheWriter$", "-test.v")
	cmd.Env = append(os.Environ(),
		"CONTRACT_HELPER=1",
		"CONTRACT_DIR="+dir,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helper failed: %v\n%s", err, string(out))
	}
}

func TestHelperContractHashDBPutSyncOverridesCacheWriter(t *testing.T) {
	if os.Getenv("CONTRACT_HELPER") != "1" {
		t.Skip("helper")
	}
	dir := os.Getenv("CONTRACT_DIR")
	if dir == "" {
		t.Fatalf("missing CONTRACT_DIR")
	}

	db, err := openEngine("hashdb-sharded", dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	if err := db.Put([]byte("x"), []byte("cached")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := db.PutSync([]byte("x"), []byte("durable")); err != nil {
		t.Fatalf("PutSync: %v", err)
	}

	os.Exit(0)
}

func TestContract_HashDBSharded_PutSyncOverridesCachedPutOnCrash(t *testing.T) {
	dir := t.TempDir()

	crashWritePutSyncOverridesCache(t, dir)

	db, err := openEngine("hashdb-sharded", dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	got, err := db.Get([]byte("x"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(got) != "durable" {
		t.Fatalf("x: got %q, want %q", string(got), "durable")
	}
}

func TestContract_HashDBSharded_ForEachBlocksWriters(t *testing.T) {
	dir := t.TempDir()
	dbi, err := openEngine("hashdb-sharded", dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = dbi.Close() })

	if err := dbi.Put([]byte("seed"), []byte("v")); err != nil {
		t.Fatalf("seed: %v", err)
	}

	itStarted := make(chan struct{})
	releaseIt := make(chan struct{})
	itDone := make(chan error, 1)

	it, ok := dbi.(iterableKV)
	if !ok {
		t.Fatalf("missing iteration api")
	}
	go func() {
		err := it.ForEach(func(_, _ []byte) error {
			select {
			case <-itStarted:
			default:
				close(itStarted)
			}
			<-releaseIt
			return nil
		})
		itDone <- err
	}()

	select {
	case <-itStarted:
	case <-time.After(2 * time.Second):
		t.Fatalf("ForEach did not start")
	}

	putDone := make(chan struct{})
	go func() {
		_ = dbi.Put([]byte("blocked"), []byte("v"))
		close(putDone)
	}()

	select {
	case <-putDone:
		t.Fatalf("Put completed while ForEach snapshot was active")
	case <-time.After(100 * time.Millisecond):
	}

	close(releaseIt)

	select {
	case err := <-itDone:
		if err != nil {
			t.Fatalf("ForEach: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("ForEach did not finish")
	}

	select {
	case <-putDone:
	case <-time.After(2 * time.Second):
		t.Fatalf("Put still blocked after ForEach finished")
	}
}
