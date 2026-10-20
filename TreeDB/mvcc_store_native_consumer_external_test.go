package treedb_test

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/mvcc"
	"github.com/snissn/gomap/TreeDB/node"
)

// This is the existing external-timestamp Store consumer sharing the ordinary
// public DB with the selected native composite. Every storage operation is
// exported. CommitRelaxed uses the selected profile's ordinary ACK boundary;
// prune and the final Store commit explicitly opt up with CommitDurable. The
// checkpoint/reopen assertions cover a graceful durable drain, not crash ACKs.
func TestMVCCStoreNativeCompositeManagedHistory(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			opts := treedb.OptionsFor(profile, t.TempDir())
			opts.MemtableMode = "cow_btree"
			opts.MemtableShards = 4
			opts.FlushThreshold = 1 << 30
			opts.DisableSideStores = true
			opts.BackgroundCheckpointInterval = -1
			opts.BackgroundCheckpointIdleDuration = -1
			opts.MaxWALBytes = -1
			opts.ValueLog.ForcePointers = false
			opts.ValueLog.PointerThreshold = 128
			opts.ValueLog.Compression = treedb.ValueLogCompressionOff
			opts.ValueLog.CompressionAutotune = treedb.AutotuneOptions{Mode: treedb.AutotuneOff}
			database, err := treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if database != nil {
					if err := database.Close(); err != nil {
						t.Errorf("public owner cleanup: %v", err)
					}
				}
			})
			if database.ResolvedProfile() != profile {
				t.Fatalf("resolved profile=%s want=%s", database.ResolvedProfile(), profile)
			}
			t.Logf("BOUNDARY profile=%s ordinary_ack=%s Store=CommitRelaxed native=Commit() prune/final-Store=CommitDurable; reopen follows Checkpoint+Close", profile, database.Stats()["treedb.profile.ordinary_ack_class"])
			value := func(key string, ts uint64) []byte {
				return bytes.Repeat([]byte(fmt.Sprintf("%s@%d/", key, ts)), 128)
			}
			mutation := func(key string, ts uint64) mvcc.Mutation {
				return mvcc.Mutation{Key: []byte(key), Value: value(key, ts)}
			}
			store := mvcc.New(database)
			commit := func(ts uint64, mode mvcc.CommitMode, mutations ...mvcc.Mutation) {
				t.Helper()
				if err := store.CommitAt(ts, mutations, mode); err != nil {
					t.Fatalf("PUBLIC Store.CommitAt ts=%d mode=%d keys=%d: %v", ts, mode, len(mutations), err)
				}
			}
			commit(10, mvcc.CommitRelaxed, mutation("a", 10), mutation("b", 10), mutation("c", 10), mutation("d", 10))
			commit(30, mvcc.CommitRelaxed, mutation("a", 30), mutation("b", 30), mvcc.Mutation{Key: []byte("c"), Delete: true}, mutation("d", 30))
			// Arrival order is independent of the caller-assigned timestamp.
			commit(20, mvcc.CommitRelaxed, mutation("a", 20), mvcc.Mutation{Key: []byte("b"), Delete: true}, mutation("c", 20))
			if err := database.Checkpoint(); err != nil {
				t.Fatalf("pre-activation Store checkpoint: %v", err)
			}
			// Inspect the real persisted entries through the existing exported
			// Snapshot API. Discover physical keys by their distinctive payloads;
			// this test does not reproduce Store's reserved key encoding.
			proof := database.AcquireSnapshot()
			if proof == nil {
				t.Fatal("nil public Store pointer-proof snapshot")
			}
			func() {
				defer func() {
					if err := proof.Close(); err != nil {
						t.Errorf("pointer-proof snapshot Close: %v", err)
					}
				}()
				it, err := proof.Iterator(nil, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := it.Close(); err != nil {
						t.Errorf("pointer-proof iterator Close: %v", err)
					}
				}()
				seen := make(map[string]bool)
				for ; it.Valid(); it.Next() {
					for _, key := range []string{"a", "b", "c", "d"} {
						if !bytes.HasSuffix(it.ValueCopy(nil), value(key, 10)) {
							continue
						}
						entry, err := proof.GetEntryExact(it.KeyCopy(nil))
						if err != nil || entry.Flags&node.FlagPointer == 0 || entry.ValuePtr.FileID == 0 {
							t.Fatalf("actual old Store pointer %s: flags=%x pointer=%+v err=%v", key, entry.Flags, entry.ValuePtr, err)
						}
						seen[key] = true
					}
				}
				if err := it.Error(); err != nil || len(seen) != 4 {
					t.Fatalf("old Store pointer proof found=%v err=%v", seen, err)
				}
			}()
			t.Log("ACTIVATION four original timestamp10 Store entries have physical ValuePtrs; composite raw marker remains below128byte threshold")
			handle, err := database.NativeSemanticBackend()
			if err != nil {
				t.Fatalf("PUBLIC native activation on existing Store owner: %v", err)
			}
			manager, err := collections.NewNativeJSONManager(handle)
			if err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"a", "b"} {
				if _, err := manager.CreateCollection(collections.CollectionMeta{Name: name}); err != nil {
					t.Fatalf("native CreateCollection %s: %v", name, err)
				}
			}
			old, err := store.IterateVersions(mvcc.VersionIteratorOptions{ReadTimestamp: 15})
			if err != nil {
				t.Fatalf("PUBLIC old Store iterator after activation: %v", err)
			}
			t.Cleanup(func() {
				if err := old.Close(); err != nil {
					t.Errorf("old Store cut cleanup: %v", err)
				}
			})
			stat := func(key string) uint64 {
				t.Helper()
				raw := database.Stats()[key]
				n, err := strconv.ParseUint(raw, 10, 64)
				if err != nil {
					t.Fatalf("public stat %s=%q: %v", key, raw, err)
				}
				return n
			}
			if stat("treedb.cache.cow.active_cuts") == 0 {
				t.Fatal("old Store iterator did not retain the selected original COW cut")
			}
			sequence := stat("treedb.cache.cow.txn.logical_seq")
			commit(40, mvcc.CommitRelaxed, mutation("a", 40), mutation("b", 40))
			if got := stat("treedb.cache.cow.txn.logical_seq"); got != sequence+1 {
				t.Fatalf("multi-key Store commit logical advance=%d want=%d", got, sequence+1)
			}
			tx, err := manager.Begin("a", "b")
			if err != nil {
				t.Fatalf("PUBLIC composite Begin: %v", err)
			}
			t.Cleanup(func() {
				if err := tx.Close(); err != nil {
					t.Errorf("composite cleanup: %v", err)
				}
			})
			for _, name := range []string{"a", "b"} {
				if err := tx.InsertBatchInto(name, [][]byte{[]byte("row")}, [][]byte{[]byte(`{"owner":"` + name + `"}`)}); err != nil {
					t.Fatalf("composite stage %s: %v", name, err)
				}
			}
			if err := tx.SetRaw([]byte("store-consumer/raw"), []byte("composite-visible")); err != nil {
				t.Fatalf("composite stage raw: %v", err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatalf("PUBLIC native composite Commit with old Store cut: %v", err)
			}
			if got := stat("treedb.cache.cow.txn.logical_seq"); got != sequence+2 {
				t.Fatalf("one composite logical advance=%d want=%d", got, sequence+2)
			}
			if err := store.AdvanceDiscardFloor(25, mvcc.CommitRelaxed); err != nil {
				t.Fatalf("PUBLIC Store.AdvanceDiscardFloor: %v", err)
			}
			pruned, err := store.PruneVersions(mvcc.PruneOptions{BatchSize: 1, Mode: mvcc.CommitDurable})
			if err != nil {
				t.Fatalf("PUBLIC Store.PruneVersions after native composite: %v (progress=%+v)", err, pruned)
			}
			if pruned.Visited != 11 || pruned.Pruned != 4 || pruned.Retained != 7 || pruned.Batches == 0 || pruned.PrunedBytes == 0 {
				t.Fatalf("inspected physical history progress=%+v want visited11/pruned4/retained7 with committed deletes", pruned)
			}
			if err := database.Checkpoint(); err != nil {
				t.Fatalf("PUBLIC checkpoint with old Store cut after prune: %v", err)
			}
			for _, key := range []string{"a", "b", "c", "d"} {
				if !old.Valid() {
					t.Fatalf("old cut lost %s: %v", key, old.Error())
				}
				entry := old.Entry()
				if string(entry.Key) != key || entry.Timestamp != 10 || entry.State != mvcc.Present || !bytes.Equal(entry.Value, value(key, 10)) {
					t.Fatalf("old cut %s changed: key=%q timestamp=%d state=%d valueBytes=%d", key, entry.Key, entry.Timestamp, entry.State, len(entry.Value))
				}
				old.Next()
			}
			if old.Valid() || old.Error() != nil {
				t.Fatalf("old cut extra/error: valid=%t error=%v", old.Valid(), old.Error())
			}
			if _, err := store.GetAt([]byte("a"), 15); !errors.Is(err, mvcc.ErrReadBeforeDiscardFloor) {
				t.Fatalf("new read at discarded timestamp: %v", err)
			}
			if err := old.Close(); err != nil {
				t.Fatalf("old Store cut release: %v", err)
			}
			if got := stat("treedb.cache.cow.active_cuts"); got != 1 {
				t.Fatalf("original cuts after old iterator release=%d want one current cut", got)
			}
			commit(50, mvcc.CommitDurable, mutation("d", 50), mutation("e", 50))
			check := func() {
				t.Helper()
				if floor, err := store.DiscardFloor(); err != nil || floor != 25 {
					t.Fatalf("surviving floor=%d: %v", floor, err)
				}
				for _, want := range []struct {
					key      string
					read, ts uint64
					state    mvcc.ReadState
				}{{"a", 26, 20, mvcc.Present}, {"b", 26, 0, mvcc.Absent}, {"c", 26, 20, mvcc.Present}, {"d", 26, 10, mvcc.Present}, {"a", 45, 40, mvcc.Present}, {"b", 45, 40, mvcc.Present}, {"c", 45, 30, mvcc.Tombstone}, {"d", 55, 50, mvcc.Present}, {"e", 55, 50, mvcc.Present}} {
					got, err := store.GetAt([]byte(want.key), want.read)
					if err != nil || got.State != want.state || got.Timestamp != want.ts || (want.state == mvcc.Present && !bytes.Equal(got.Value, value(want.key, want.ts))) {
						t.Fatalf("Store %s read%d: state=%d timestamp=%d bytes=%d err=%v want state=%d timestamp=%d", want.key, want.read, got.State, got.Timestamp, len(got.Value), err, want.state, want.ts)
					}
				}
				for _, name := range []string{"a", "b"} {
					if got, err := manager.Get(name, []byte("row")); err != nil || string(got) != `{"owner":"`+name+`"}` {
						t.Fatalf("native %s row=%q: %v", name, got, err)
					}
				}
				if got, err := database.Get([]byte("store-consumer/raw")); err != nil || string(got) != "composite-visible" {
					t.Fatalf("composite raw=%q: %v", got, err)
				}
			}
			check()
			closeOwner := func() {
				t.Helper()
				if err := database.Checkpoint(); err != nil {
					t.Fatalf("PUBLIC final drain: %v", err)
				}
				if got := stat("treedb.cache.cow.active_cuts"); got != 1 {
					t.Fatalf("original active cuts before owner Close=%d want one current cut", got)
				}
				registry := handle.ValueLogIdentityPinRegistry()
				if err := database.Close(); err != nil {
					t.Fatalf("PUBLIC owner Close: %v", err)
				}
				database = nil
				if got := registry.Stats(); got.ActivePins != 0 || got.ActiveIdentities != 0 || got.ActiveStableNamespaceLinks != 0 || registry.CleanupError() != nil {
					t.Fatalf("original pin accounting after release=%+v cleanup=%v", got, registry.CleanupError())
				}
			}
			closeOwner()
			database, err = treedb.Open(opts)
			if err != nil {
				t.Fatalf("PUBLIC reopen: %v", err)
			}
			store = mvcc.New(database)
			handle, err = database.NativeSemanticBackend()
			if err != nil {
				t.Fatalf("PUBLIC reopened native activation: %v", err)
			}
			manager, err = collections.NewNativeJSONManager(handle)
			if err != nil {
				t.Fatal(err)
			}
			check()
			closeOwner()
			t.Logf("PUBLIC PROGRESS Store multi-key ACKs, one composite ACK, prune=%+v, held old pointer values, checkpoint/reopen, old cut released/current cut retained; original pins zero after Close", pruned)
			// DB.Stats is unavailable after public Close. Whole COW-budget zero is
			// covered by existing internal lifecycle controls, not observed here.
		})
	}
}
