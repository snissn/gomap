package treedb_test

// Public command-WAL stable-image recovery coverage after empty-shard reuse.
// Direct table-identity assertions remain in caching/checkpoint_empty_shard_test.go.

import (
	"bytes"
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/cespare/xxhash/v2"
	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/powerlossoracle"
	"github.com/snissn/gomap/TreeDB/internal/powerlossreopen"
	"github.com/snissn/gomap/TreeDB/node"
)

// This exercises the real public external-command-WAL owner with the non-COW
// checkpoint branch. NoWAL lower-layer tests already prove object reuse itself.
func TestEmptyShardPublicCommandWALStableImageAfterReuse(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
	opts.MemtableMode = "append_only"
	opts.MemtableShards = 4
	opts.DisableSideStores = true
	opts.DisableBackgroundPrune = true
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.BackgroundIndexVacuumInterval = -1
	opts.MaxWALBytes = -1
	opts.FlushThreshold = 1 << 30
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true
	opts.ValueLog.Generational.Policy = treedb.ValueLogGenerationOff
	database, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if mode := database.Stats()["treedb.cache.memtable_mode"]; mode != "append_only" {
		t.Fatalf("checkpoint reuse path is inactive: mode=%q", mode)
	}
	// Plain keys deliberately contain no version-affinity prefix. Match the
	// existing caching.hashKey/shardIndex mapping: xxhash(key) & (shards-1).
	keys := make([][]byte, 4)
	for i, remaining := 0, len(keys); remaining > 0 && i < 10000; i++ {
		key := []byte(fmt.Sprintf("empty-shard-public-%08d", i))
		shard := int(xxhash.Sum64(key) & 3)
		if keys[shard] == nil {
			keys[shard], remaining = key, remaining-1
		}
	}
	for _, key := range keys {
		if key == nil {
			t.Fatal("could not select a key in every shard")
		}
	}
	before, after := bytes.Repeat([]byte("before/"), 512), bytes.Repeat([]byte("after/"), 512)
	if err := database.SetSync(keys[0], before); err != nil {
		t.Fatal(err)
	}
	held := database.AcquireSnapshot()
	if held == nil {
		t.Fatal("snapshot unavailable")
	}
	defer held.Close()
	// AcquireSnapshot may rotate the populated mutable into its immutable
	// queue. Refill only shard 0 so Checkpoint must execute mutable cutover;
	// otherwise this test could accidentally skip the reuse branch entirely.
	if err := database.SetSync(keys[0], before); err != nil {
		t.Fatal(err)
	}
	// Shards 1..3 have never received a write. This checkpoint retains their
	// empty append-only tables instead of replacing them.
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	baselineLSN, err := strconv.ParseUint(database.Stats()["treedb.applied_command_lsn"], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	model, err := powerlossoracle.Capture(opts.Dir)
	if err != nil {
		t.Fatal(err)
	}
	var observationMu sync.Mutex
	active := true
	commandSyncs := 0
	restoreObserver := durabilitycut.Install(func(e durabilitycut.Event) error {
		if !cowEventBelongsToDB(opts.Dir, e) {
			return nil
		}
		observationMu.Lock()
		defer observationMu.Unlock()
		if !active {
			return nil
		}
		if err := model.Observe(opts.Dir, e); err != nil {
			return err
		}
		if e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.AfterDependencyFileSync {
			commandSyncs++
		}
		return nil
	})
	var restoreOnce sync.Once
	restore := func() { restoreOnce.Do(restoreObserver) }
	defer restore()
	if err := database.SetSync(keys[1], after); err != nil {
		t.Fatal(err)
	}
	observationMu.Lock()
	active = false
	stable := model.Clone()
	syncsAtACK := commandSyncs
	observationMu.Unlock()
	restore()
	if syncsAtACK == 0 {
		t.Fatal("durable ACK did not complete a real command-file sync")
	}
	// Snapshot acquisition may rotate mutable state, so do this activation
	// assertion only after the ACK image has been frozen and observation stopped.
	sourceSnapshot := database.AcquireSnapshot()
	if sourceSnapshot == nil {
		t.Fatal("source pointer snapshot unavailable")
	}
	defer sourceSnapshot.Close()
	entry, err := sourceSnapshot.GetEntryExact(keys[1])
	if err != nil || entry.Flags&node.FlagPointer == 0 || entry.ValuePtr.FileID == 0 || entry.ValuePtr.Length == 0 {
		t.Fatalf("new reused-shard value did not use persistent pointer: entry=%+v err=%v", entry, err)
	}
	if found, err := held.Has(keys[1]); err != nil || found {
		t.Fatalf("old snapshot saw later reused-shard write: found=%t err=%v", found, err)
	}
	if got, err := held.Get(keys[0]); err != nil || !bytes.Equal(got, before) {
		t.Fatalf("old snapshot lost original value: equal=%t err=%v", bytes.Equal(got, before), err)
	}
	wantRevisions := make([]treedb.EntryRevision, 2)
	for i := 0; i < 2; i++ {
		_, revision, err := database.GetVersioned(keys[i])
		if err != nil || revision == treedb.LegacyEntryRevision {
			t.Fatalf("live revision for shard %d: %d %v", i, revision, err)
		}
		wantRevisions[i] = revision
	}
	// The source stays open: clean-close checkpoint bytes cannot enter stable.
	result, reopened, closeReopened, err := powerlossreopen.Stable(stable, opts, false)
	if err != nil || result.Rejected || reopened == nil {
		t.Fatalf("stable image after reuse rejected: %+v %v", result, err)
	}
	defer closeReopened()
	if result.AppliedLSN <= baselineLSN {
		t.Fatalf("recovery did not advance beyond checkpoint: applied=%d baseline=%d", result.AppliedLSN, baselineLSN)
	}
	for i, want := range [][]byte{before, after} {
		got, revision, err := reopened.GetVersioned(keys[i])
		if err != nil || revision != wantRevisions[i] || !bytes.Equal(got, want) {
			t.Fatalf("recovered shard %d: revision=%d want=%d equal=%t err=%v", i, revision, wantRevisions[i], bytes.Equal(got, want), err)
		}
	}
	for _, key := range keys[2:] {
		if found, err := reopened.Has(key); err != nil || found {
			t.Fatalf("unused shard recovered phantom key %q: found=%t err=%v", key, found, err)
		}
	}
}
