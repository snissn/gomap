package treedb_test

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/mvcc"
	"github.com/snissn/gomap/TreeDB/node"
)

// Equivalent physical maintenance may replace current pointers, but a held
// ordered cut keeps its original pointer, native revision and value. Exercise
// the real Store producer and the host's rewrite/compaction fences, then write
// another logical tail through the rebound basis and reopen its durable image.
func TestCOWOrderedRewriteCompactHeldPointerAndNewTailReopen(t *testing.T) {
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			opts := cowOrderedDurabilityOptions(profile, t.TempDir())
			opts.BackgroundCheckpointIdleDuration = -1
			opts.BackgroundIndexVacuumInterval = -1
			opts.MaxWALBytes = -1
			db := cowOrderedDurabilityOpen(t, opts)
			defer func() {
				if db != nil {
					_ = db.Close()
				}
			}()
			store := mvcc.New(db)
			key, peer := []byte("rewrite-history"), []byte{0, 'r', 0xff, 0}
			oldValue := bytes.Repeat([]byte("original-ordered-pointer/"), 256)
			peerValue := bytes.Repeat([]byte("binary-history-pointer/"), 256)
			seed := []cowOrderedVersion{
				{key: key, ts: 10, state: mvcc.Present, value: oldValue},
				{key: key, ts: 20, state: mvcc.Present, value: []byte{}},
				{key: key, ts: 30, state: mvcc.Tombstone},
				{key: peer, ts: 15, state: mvcc.Present, value: peerValue},
			}
			groups := []mvcc.CommitGroup{
				{Timestamp: 10, Mutations: []mvcc.Mutation{{Key: key, Value: oldValue}}},
				{Timestamp: 20, Mutations: []mvcc.Mutation{{Key: key, Value: []byte{}}}},
				{Timestamp: 30, Mutations: []mvcc.Mutation{{Key: key, Delete: true}}},
				{Timestamp: 15, Mutations: []mvcc.Mutation{{Key: peer, Value: peerValue}}},
			}
			if err := store.CommitGroupAt(groups, mvcc.CommitRelaxed); err != nil {
				t.Fatal(err)
			}
			old := db.AcquireSnapshot()
			if old == nil {
				t.Fatal("original ordered cut unavailable")
			}
			oldOpen := true
			defer func() {
				if oldOpen {
					_ = old.Close()
				}
			}()
			original := cowOrderedCapturePhysical(t, db, seed)
			checkOld := func(stage string) {
				t.Helper()
				for _, want := range original {
					value, revision, err := old.GetVersioned(want.key)
					if err != nil || revision != want.revision || !bytes.Equal(value, want.value) {
						t.Fatalf("%s changed held value/revision at %x: revision=%d want=%d error=%v", stage, want.key, revision, want.revision, err)
					}
					entry, err := old.GetEntryExact(want.key)
					if err != nil || entry.Revision != want.revision || entry.Flags != want.flags || entry.ValuePtr != want.ptr {
						t.Fatalf("%s changed held pointer metadata: entry=%+v original=%+v error=%v", stage, entry, want, err)
					}
				}
			}
			// The held cut predates the handoff: it retains the real ordered
			// generation rather than merely a durable backend snapshot.
			if err := db.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			beforeRewrite := cowOrderedCapturePhysical(t, db, seed)
			var sources []uint32
			seen := make(map[uint32]bool)
			for _, version := range beforeRewrite {
				if version.flags&node.FlagPointer != 0 && version.ptr.FileID != 0 && !seen[version.ptr.FileID] {
					seen[version.ptr.FileID] = true
					sources = append(sources, version.ptr.FileID)
				}
			}
			if len(sources) == 0 {
				t.Fatal("forced-pointer seed did not produce a rewrite source")
			}
			logicalSequence := func() uint64 {
				value, err := strconv.ParseUint(db.Stats()["treedb.cache.cow.txn.logical_seq"], 10, 64)
				if err != nil {
					t.Fatal(err)
				}
				return value
			}
			sequence := logicalSequence()
			rewritten, err := db.ValueLogRewriteOnline(context.Background(), treedb.ValueLogRewriteOnlineOptions{SourceFileIDs: sources, BatchSize: 1, SyncEachBatch: true})
			if err != nil || rewritten.ValueRecordsCopied == 0 || rewritten.ValueBytesCopied == 0 {
				t.Fatalf("selected actual ordered rewrite: %+v error=%v", rewritten, err)
			}
			if logicalSequence() != sequence {
				t.Fatal("equivalent rewrite consumed a logical publication")
			}
			checkOld("selected rewrite")
			cowOrderedRequireVersions(t, store, seed)
			afterRewrite := cowOrderedCapturePhysical(t, db, seed)
			changed := false
			for i, version := range afterRewrite {
				before := beforeRewrite[i]
				if version.revision != before.revision || version.flags != before.flags || !bytes.Equal(version.value, before.value) {
					t.Fatalf("rewrite changed logical/native metadata: before=%+v after=%+v", before, version)
				}
				changed = changed || version.ptr != before.ptr
			}
			if !changed {
				t.Fatal("actual rewrite did not replace any current source pointer")
			}

			// The successor includes a same-TS replacement, a late version,
			// and resurrection after a retained logical tombstone.
			replacement := bytes.Repeat([]byte("new-same-timestamp-pointer/"), 256)
			latest := bytes.Repeat([]byte("new-ordered-tail-pointer/"), 256)
			tail := []mvcc.CommitGroup{
				{Timestamp: 10, Mutations: []mvcc.Mutation{{Key: key, Value: replacement}}},
				{Timestamp: 25, Mutations: []mvcc.Mutation{{Key: key, Value: []byte("late-backfill")}}},
				{Timestamp: 40, Mutations: []mvcc.Mutation{{Key: key, Value: latest}, {Key: peer, Delete: true}}},
			}
			if err := store.CommitGroupAt(tail, mvcc.CommitRelaxed); err != nil {
				t.Fatal(err)
			}
			currentVersions := append([]cowOrderedVersion(nil), seed...)
			currentVersions[0].value = replacement
			currentVersions = append(currentVersions,
				cowOrderedVersion{key: key, ts: 25, state: mvcc.Present, value: []byte("late-backfill")},
				cowOrderedVersion{key: key, ts: 40, state: mvcc.Present, value: latest},
				cowOrderedVersion{key: peer, ts: 40, state: mvcc.Tombstone},
			)
			current := cowOrderedCapturePhysical(t, db, currentVersions)
			sequence = logicalSequence()
			report, err := db.CompactStorage(context.Background(), treedb.CompactStorageOptions{})
			if err != nil {
				// Stable namespace promotion is a host capability. Preserve the
				// existing platform contract, never waive another compact failure.
				packSupported := rootpublication.StableRelativeNamespaceSupported() && rootpublication.StableCrossParentMoveNoReplaceSupported()
				if packSupported || !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) || len(report.Phases) == 0 || !strings.HasPrefix(report.Phases[len(report.Phases)-1].Name, "leaf-generation-pack-") {
					t.Fatalf("actual ordered compact: phases=%+v error=%v", report.Phases, err)
				}
			}
			if logicalSequence() != sequence {
				t.Fatal("equivalent compaction consumed a logical publication")
			}
			if err := db.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			checkOld("compaction and newer-tail checkpoint")
			checkCurrent := func() {
				t.Helper()
				cowOrderedRequireVersions(t, store, currentVersions)
				observed := cowOrderedCapturePhysical(t, db, currentVersions)
				for i, version := range observed {
					want := current[i]
					// Current pointers can change again during compaction. Their
					// native identity, flags and retained bytes cannot.
					if version.revision != want.revision || version.flags != want.flags || !bytes.Equal(version.value, want.value) {
						t.Fatalf("current retained metadata changed: before=%+v after=%+v", want, version)
					}
				}
			}
			checkCurrent()
			if err := old.Close(); err != nil {
				t.Fatal(err)
			}
			oldOpen = false
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db = nil
			db = cowOrderedDurabilityOpen(t, opts)
			store = mvcc.New(db)
			checkCurrent()
		})
	}
}
