package treedb_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
	"github.com/snissn/gomap/TreeDB/internal/powerlossoracle"
	"github.com/snissn/gomap/TreeDB/internal/powerlossreopen"
	"github.com/snissn/gomap/TreeDB/mvcc"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func cowOrderedDurabilityOptions(profile treedb.Profile, dir string) treedb.Options {
	opts := treedb.OptionsFor(profile, dir)
	opts.MemtableMode = "cow_ordered"
	opts.MemtableShards = 2
	opts.FlushThreshold = 1 << 30
	opts.DisableSideStores = true
	opts.DisableBackgroundPrune = true
	opts.BackgroundCheckpointInterval = -1
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true
	opts.ValueLog.Compression = treedb.ValueLogCompressionOff
	return opts
}

func cowOrderedDurabilityOpen(t *testing.T, opts treedb.Options) *treedb.DB {
	t.Helper()
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	if got := db.Stats()["treedb.cache.memtable_mode"]; got != "cow_ordered" {
		_ = db.Close()
		t.Fatalf("logical memtable did not activate: actual mode %q", got)
	}
	capability, ok := any(db).(interface{ SupportsLogicalMVCC() bool })
	if !ok || !capability.SupportsLogicalMVCC() {
		_ = db.Close()
		t.Fatal("cow_ordered did not expose its native logical MVCC commit seam")
	}
	return db
}

// Dgraph's initial posting transaction contains 500 distinct keys in map order.
// Admission must account for those independent histories, rather than reserving
// a 500-version history separately for every key in an unordered shard.
func TestCOWOrderedUnorderedPostingBatchAdmission(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed} {
		t.Run(string(profile), func(t *testing.T) {
			opts := cowOrderedDurabilityOptions(profile, t.TempDir())
			opts.MemtableShards = 4 // The Dgraph adapter's ordinary configuration.
			db := cowOrderedDurabilityOpen(t, opts)
			defer func() {
				if db != nil {
					_ = db.Close()
				}
			}()
			store := mvcc.New(db)
			key := []byte(fmt.Sprintf("posting.next/%064d", 0))
			if err := store.CommitAt(10, []mvcc.Mutation{{Key: key, Value: []byte("old")}}, mvcc.CommitRelaxed); err != nil {
				t.Fatal(err)
			}
			old := db.AcquireSnapshot()
			if old == nil {
				t.Fatal("old posting snapshot unavailable")
			}
			oldOpen := true
			defer func() {
				if oldOpen {
					_ = old.Close()
				}
			}()
			physicalKey, err := mvcckey.Encode(key, 10)
			if err != nil {
				t.Fatal(err)
			}
			oldValue, oldRevision, err := old.GetVersioned(physicalKey)
			if err != nil {
				t.Fatal(err)
			}
			oldValue = bytes.Clone(oldValue)
			mutations := make([]mvcc.Mutation, 500)
			versions := []cowOrderedVersion{{key: key, ts: 10, state: mvcc.Present, value: []byte("old")}}
			for i := range mutations {
				// A fixed permutation keeps the failing order reproducible.
				k := []byte(fmt.Sprintf("posting.next/%064d", i*197%len(mutations)))
				value := bytes.Repeat([]byte{byte(i), byte(i >> 8)}, 64)
				mutations[i] = mvcc.Mutation{Key: k, Value: value}
				versions = append(versions, cowOrderedVersion{key: k, ts: 20, state: mvcc.Present, value: value})
			}
			prepared, appended, swapped := 0, 0, 0
			restore := durabilitycut.Install(func(event durabilitycut.Event) error {
				switch event.Point {
				case durabilitycut.AfterCOWCanonicalPreparation:
					prepared++
				case durabilitycut.BeforeCOWCutSwap:
					swapped++
				case durabilitycut.BeforeDependencyAppend:
					if event.Resource == durabilitycut.ResourceCommandWAL && event.Root == opts.Dir {
						appended++
					}
				}
				return nil
			})
			mode := mvcc.CommitRelaxed
			if profile == treedb.ProfileCommandWALDurable {
				mode = mvcc.CommitDurable
			}
			err = store.CommitAt(20, mutations, mode)
			restore()
			if err != nil || prepared != 1 || appended != 1 || swapped != 1 {
				t.Fatalf("500-key posting transaction: err=%v prepared=%d appends=%d swaps=%d", err, prepared, appended, swapped)
			}
			cowOrderedRequireVersions(t, store, versions)
			physical := cowOrderedCapturePhysical(t, db, versions)
			if err := db.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			if got, revision, err := old.GetVersioned(physicalKey); err != nil || revision != oldRevision || !bytes.Equal(got, oldValue) {
				t.Fatalf("old posting snapshot changed: revision=%d err=%v", revision, err)
			}
			oldErr := old.Close()
			oldOpen = false
			if err := oldErr; err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db = nil
			db = cowOrderedDurabilityOpen(t, opts)
			cowOrderedRequireVersions(t, mvcc.New(db), versions)
			cowOrderedRequirePhysical(t, db, physical)
		})
	}
}

type cowOrderedVersion struct {
	key   []byte
	ts    uint64
	state mvcc.ReadState
	value []byte
}

type cowOrderedPhysicalVersion struct {
	key      []byte
	value    []byte
	revision treedb.EntryRevision
	ptr      page.ValuePtr
	flags    byte
}

func cowOrderedRequireVersions(t *testing.T, store *mvcc.Store, versions []cowOrderedVersion) {
	t.Helper()
	for _, version := range versions {
		got, err := store.GetAt(version.key, version.ts)
		if err != nil || got.State != version.state || !bytes.Equal(got.Value, version.value) {
			t.Fatalf("GetAt(%x,%d): state=%v value=%x timestamp=%d err=%v; want state=%v value=%x", version.key, version.ts, got.State, got.Value, got.Timestamp, err, version.state, version.value)
		}
		if version.state != mvcc.Absent && got.Timestamp != version.ts {
			t.Fatalf("GetAt(%x,%d): retained timestamp=%d", version.key, version.ts, got.Timestamp)
		}
	}
}

func cowOrderedCapturePhysical(t *testing.T, db *treedb.DB, versions []cowOrderedVersion) []cowOrderedPhysicalVersion {
	t.Helper()
	snapshot := db.AcquireSnapshot()
	if snapshot == nil {
		t.Fatal("physical snapshot unavailable")
	}
	defer snapshot.Close()
	physical := make([]cowOrderedPhysicalVersion, 0, len(versions))
	for _, version := range versions {
		if version.state == mvcc.Absent {
			continue
		}
		key, err := mvcckey.Encode(version.key, version.ts)
		if err != nil {
			t.Fatal(err)
		}
		value, revision, err := snapshot.GetVersioned(key)
		if err != nil || revision == treedb.LegacyEntryRevision {
			t.Fatalf("physical version %x@%d: revision=%d err=%v", version.key, version.ts, revision, err)
		}
		entry, err := snapshot.GetEntryExact(key)
		if err != nil || entry.Revision != revision || !bytes.Equal(entry.Key, key) {
			t.Fatalf("physical metadata %x@%d: entry=%+v revision=%d err=%v", version.key, version.ts, entry, revision, err)
		}
		// The logical deletion marker is a retained value in the physical
		// index. Treating it as a physical tombstone loses historical reads.
		if entry.Flags&node.FlagTombstone != 0 {
			t.Fatalf("logical version %x@%d became a physical tombstone", version.key, version.ts)
		}
		if len(version.value) > 1024 && entry.Flags&node.FlagPointer == 0 {
			t.Fatalf("large logical version %x@%d did not exercise a persistent ValuePtr", version.key, version.ts)
		}
		physical = append(physical, cowOrderedPhysicalVersion{key: key, value: bytes.Clone(value), revision: revision, ptr: entry.ValuePtr, flags: entry.Flags})
	}
	return physical
}

func cowOrderedRequirePhysical(t *testing.T, db *treedb.DB, physical []cowOrderedPhysicalVersion) {
	t.Helper()
	snapshot := db.AcquireSnapshot()
	if snapshot == nil {
		t.Fatal("physical snapshot unavailable")
	}
	defer snapshot.Close()
	for _, version := range physical {
		value, revision, err := snapshot.GetVersioned(version.key)
		if err != nil || revision != version.revision || !bytes.Equal(value, version.value) {
			t.Fatalf("physical version changed at %x: revision %d -> %d, value_equal=%t, err=%v", version.key, version.revision, revision, bytes.Equal(value, version.value), err)
		}
		entry, err := snapshot.GetEntryExact(version.key)
		if err != nil || entry.Revision != version.revision || entry.Flags != version.flags || entry.ValuePtr != version.ptr {
			t.Fatalf("physical metadata changed at %x: entry=%+v want flags=%d ptr=%+v revision=%d err=%v", version.key, entry, version.flags, version.ptr, version.revision, err)
		}
	}
}

// This exercises the full retained physical history, rather than just the
// latest logical winner. The old cut must retain both a replaced same-TS value
// and its persistent pointer while checkpoint and GC retire current overlays.
func TestCOWOrderedHistoryPointerRevisionCheckpointReopen(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			opts := cowOrderedDurabilityOptions(profile, t.TempDir())
			db := cowOrderedDurabilityOpen(t, opts)
			defer func() {
				if db != nil {
					_ = db.Close()
				}
			}()
			store := mvcc.New(db)
			key, binaryKey := []byte("history"), []byte{0, 'h', 0xff, 0}
			oldValue := bytes.Repeat([]byte("old-persistent-version/"), 256)
			newValue := bytes.Repeat([]byte("replacement-persistent-version/"), 256)
			groups := []mvcc.CommitGroup{
				{Timestamp: 10, Mutations: []mvcc.Mutation{{Key: key, Value: oldValue}}},
				{Timestamp: 20, Mutations: []mvcc.Mutation{{Key: key, Value: []byte{}}}},
				{Timestamp: 30, Mutations: []mvcc.Mutation{{Key: key, Delete: true}}},
				{Timestamp: 15, Mutations: []mvcc.Mutation{{Key: binaryKey, Value: []byte("binary")}}},
			}
			if err := store.CommitGroupAt(groups, mvcc.CommitRelaxed); err != nil {
				t.Fatal(err)
			}
			oldCut := db.AcquireSnapshot()
			if oldCut == nil {
				t.Fatal("old history cut unavailable")
			}
			oldCutOpen := true
			defer func() {
				if oldCutOpen {
					_ = oldCut.Close()
				}
			}()
			oldKey, err := mvcckey.Encode(key, 10)
			if err != nil {
				t.Fatal(err)
			}
			oldEncoded, oldRevision, err := oldCut.GetVersioned(oldKey)
			if err != nil || oldRevision == treedb.LegacyEntryRevision {
				t.Fatalf("old history: revision=%d err=%v", oldRevision, err)
			}
			oldEncoded = bytes.Clone(oldEncoded)
			if err := store.CommitGroupAt([]mvcc.CommitGroup{
				{Timestamp: 10, Mutations: []mvcc.Mutation{{Key: key, Value: newValue}}},
				{Timestamp: 40, Mutations: []mvcc.Mutation{{Key: key, Value: []byte("head")}, {Key: binaryKey, Value: []byte("binary-head")}}},
			}, mvcc.CommitRelaxed); err != nil {
				t.Fatal(err)
			}
			versions := []cowOrderedVersion{
				{key: key, ts: 9, state: mvcc.Absent},
				{key: key, ts: 10, state: mvcc.Present, value: newValue},
				{key: key, ts: 20, state: mvcc.Present, value: []byte{}},
				{key: key, ts: 30, state: mvcc.Tombstone},
				{key: key, ts: 40, state: mvcc.Present, value: []byte("head")},
				{key: binaryKey, ts: 15, state: mvcc.Present, value: []byte("binary")},
				{key: binaryKey, ts: 40, state: mvcc.Present, value: []byte("binary-head")},
			}
			cowOrderedRequireVersions(t, store, versions)
			physical := cowOrderedCapturePhysical(t, db, versions)
			for _, version := range physical {
				if bytes.Equal(version.key, oldKey) && version.revision <= oldRevision {
					t.Fatalf("same timestamp replacement did not advance native revision: %d -> %d", oldRevision, version.revision)
				}
			}
			if err := db.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			cowOrderedRequirePhysical(t, db, physical)
			if _, err := db.ValueLogGC(context.Background(), treedb.ValueLogGCOptions{}); err != nil {
				t.Fatal(err)
			}
			if value, revision, err := oldCut.GetVersioned(oldKey); err != nil || revision != oldRevision || !bytes.Equal(value, oldEncoded) {
				t.Fatalf("GC/checkpoint invalidated old history owner: revision=%d err=%v equal=%t", revision, err, bytes.Equal(value, oldEncoded))
			}
			cowOrderedRequireVersions(t, store, versions)
			cowOrderedRequirePhysical(t, db, physical)
			oldCutErr := oldCut.Close()
			oldCutOpen = false
			if oldCutErr != nil {
				t.Fatal(oldCutErr)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db = nil
			db = cowOrderedDurabilityOpen(t, opts)
			cowOrderedRequireVersions(t, mvcc.New(db), versions)
			cowOrderedRequirePhysical(t, db, physical)
		})
	}
}

// The observer stops at the acknowledgement. Neither Close nor a later
// checkpoint contributes bytes to the stable-only recovery image.
func TestCOWOrderedDurableGroupStableImageAtAcknowledgement(t *testing.T) {
	cases := []struct {
		profile treedb.Profile
		mode    mvcc.CommitMode
	}{
		{treedb.ProfileCommandWALDurable, mvcc.CommitRelaxed},
		{treedb.ProfileCommandWALRelaxed, mvcc.CommitDurable},
		{treedb.ProfileNoWALFast, mvcc.CommitDurable},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("%s/mode_%d", tc.profile, tc.mode), func(t *testing.T) {
			opts := cowOrderedDurabilityOptions(tc.profile, t.TempDir())
			db := cowOrderedDurabilityOpen(t, opts)
			defer db.Close()
			if err := db.SetSync([]byte("seed"), []byte("baseline")); err != nil {
				t.Fatal(err)
			}
			if err := db.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			baselineLSN, err := strconv.ParseUint(db.Stats()["treedb.applied_command_lsn"], 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			model, err := powerlossoracle.Capture(opts.Dir)
			if err != nil {
				t.Fatal(err)
			}
			var observerMu sync.Mutex
			active, metaSyncs, commandSyncs := true, 0, 0
			restore := durabilitycut.Install(func(event durabilitycut.Event) error {
				if !cowEventBelongsToDB(opts.Dir, event) {
					return nil
				}
				observerMu.Lock()
				defer observerMu.Unlock()
				if !active {
					return nil
				}
				if err := model.Observe(opts.Dir, event); err != nil {
					return err
				}
				if event.Point == durabilitycut.AfterMetaSync {
					metaSyncs++
				}
				if event.Point == durabilitycut.AfterDependencyFileSync && event.Resource == durabilitycut.ResourceCommandWAL {
					commandSyncs++
				}
				return nil
			})
			observing := true
			defer func() {
				if observing {
					restore()
				}
			}()
			key := []byte{'g', 0, 'r'}
			large := bytes.Repeat([]byte("durable-logical-payload/"), 256)
			store := mvcc.New(db)
			if err := store.CommitGroupAt([]mvcc.CommitGroup{
				{Timestamp: 11, Mutations: []mvcc.Mutation{{Key: key, Value: large}}},
				{Timestamp: 22, Mutations: []mvcc.Mutation{{Key: key, Value: []byte{}}}},
				{Timestamp: 33, Mutations: []mvcc.Mutation{{Key: key, Delete: true}, {Key: []byte("other"), Value: []byte("atomic-peer")}}},
			}, tc.mode); err != nil {
				t.Fatal(err)
			}
			observerMu.Lock()
			active = false
			stable := model.Clone()
			observerMu.Unlock()
			restore()
			observing = false
			versions := []cowOrderedVersion{
				{key: key, ts: 11, state: mvcc.Present, value: large},
				{key: key, ts: 22, state: mvcc.Present, value: []byte{}},
				{key: key, ts: 33, state: mvcc.Tombstone},
				{key: []byte("other"), ts: 33, state: mvcc.Present, value: []byte("atomic-peer")},
			}
			physical := cowOrderedCapturePhysical(t, db, versions)
			if tc.profile == treedb.ProfileNoWALFast {
				if metaSyncs == 0 {
					t.Fatal("NoWAL durable acknowledgement did not complete a root/meta seal")
				}
			} else {
				if commandSyncs == 0 {
					t.Fatal("command-WAL durable acknowledgement did not complete a command-file sync")
				}
				applied, err := strconv.ParseUint(db.Stats()["treedb.applied_command_lsn"], 10, 64)
				if err != nil || applied != baselineLSN {
					t.Fatalf("replay test unexpectedly covered the command in a root: applied=%d baseline=%d err=%v", applied, baselineLSN, err)
				}
			}
			result, reopened, closeReopened, err := powerlossreopen.Stable(stable, opts, false)
			if err != nil || result.Rejected || reopened == nil {
				t.Fatalf("acknowledged stable image rejected: %+v %v", result, err)
			}
			defer closeReopened()
			defer func() {
				if !t.Failed() {
					return
				}
				trace := stable.Trace()
				if len(trace) > 20 {
					trace = trace[len(trace)-20:]
				}
				t.Logf("durable ACK recovery diagnostic: applied=%d baseline=%d commit=%d command_syncs=%d meta_syncs=%d stable=%s trace_tail=%q", result.AppliedLSN, baselineLSN, result.CommitSeq, commandSyncs, metaSyncs, stable.StableFingerprint(), trace)
				for _, version := range physical {
					value, revision, err := reopened.GetVersioned(version.key)
					t.Logf("durable ACK physical diagnostic: key=%x length=%d want_length=%d revision=%d want_revision=%d equal=%t err=%v", version.key, len(value), len(version.value), revision, version.revision, bytes.Equal(value, version.value), err)
				}
			}()
			if value, err := reopened.Get([]byte("seed")); err != nil || string(value) != "baseline" {
				t.Fatalf("baseline after stable reopen: %q, %v", value, err)
			}
			cowOrderedRequireVersions(t, mvcc.New(reopened), versions)
			// Replay may materialize a fresh persistent pointer, so compare
			// canonical bytes and native revision, rather than file placement.
			for _, version := range physical {
				value, revision, err := reopened.GetVersioned(version.key)
				if err != nil || revision != version.revision || !bytes.Equal(value, version.value) {
					t.Fatalf("stable replay changed physical history %x: revision=%d want=%d equal=%t err=%v", version.key, revision, version.revision, bytes.Equal(value, version.value), err)
				}
			}
			if tc.profile != treedb.ProfileNoWALFast && result.AppliedLSN <= baselineLSN {
				t.Fatalf("stable reopen did not replay accepted command: applied=%d baseline=%d", result.AppliedLSN, baselineLSN)
			}
		})
	}
}

// Canonical preparation has built every private history and resource owner,
// but it is still before command acceptance. A refusal here may leave unused
// persistent value-log bytes, never a partial logical group or a WAL command.
func TestCOWOrderedCanonicalPreparationRefusalPrecedesCommandAppend(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed} {
		t.Run(string(profile), func(t *testing.T) {
			opts := cowOrderedDurabilityOptions(profile, t.TempDir())
			db := cowOrderedDurabilityOpen(t, opts)
			defer db.Close()
			store := mvcc.New(db)
			key := []byte("canonical-history")
			old := cowOrderedVersion{key: key, ts: 10, state: mvcc.Present, value: []byte("old")}
			if err := store.CommitAt(old.ts, []mvcc.Mutation{{Key: key, Value: old.value}}, mvcc.CommitRelaxed); err != nil {
				t.Fatal(err)
			}
			physical := cowOrderedCapturePhysical(t, db, []cowOrderedVersion{old})
			refusal := errors.New("refuse ordered canonical preparation before WAL append")
			var prepared, appends atomic.Int32
			restore := durabilitycut.Install(func(event durabilitycut.Event) error {
				if !cowEventBelongsToDB(opts.Dir, event) {
					return nil
				}
				if event.Point == durabilitycut.BeforeDependencyAppend && event.Resource == durabilitycut.ResourceCommandWAL {
					appends.Add(1)
				}
				if event.Point == durabilitycut.AfterCOWCanonicalPreparation {
					prepared.Add(1)
					return refusal
				}
				return nil
			})
			observing := true
			defer func() {
				if observing {
					restore()
				}
			}()
			large := bytes.Repeat([]byte("private-prepared-pointer/"), 256)
			groups := []mvcc.CommitGroup{
				{Timestamp: 10, Mutations: []mvcc.Mutation{{Key: key, Value: large}}},
				{Timestamp: 20, Mutations: []mvcc.Mutation{{Key: key, Delete: true}}},
				{Timestamp: 30, Mutations: []mvcc.Mutation{{Key: []byte("unaccepted-peer"), Value: []byte("peer")}}},
			}
			err := store.CommitGroupAt(groups, mvcc.CommitRelaxed)
			restore()
			observing = false
			if !errors.Is(err, refusal) || prepared.Load() == 0 || appends.Load() != 0 {
				t.Fatalf("pre-acceptance refusal: err=%v canonical_preparations=%d command_appends=%d", err, prepared.Load(), appends.Load())
			}
			cowOrderedRequireVersions(t, store, []cowOrderedVersion{old, {key: []byte("unaccepted-peer"), ts: 30, state: mvcc.Absent}})
			if got, err := store.GetAt(key, 20); err != nil || got.State != mvcc.Present || got.Timestamp != old.ts || !bytes.Equal(got.Value, old.value) {
				t.Fatalf("private tombstone leaked after refusal: %+v %v", got, err)
			}
			cowOrderedRequirePhysical(t, db, physical)
			if err := store.CommitGroupAt(groups, mvcc.CommitDurable); err != nil {
				t.Fatalf("unaccepted group was not retryable: %v", err)
			}
			cowOrderedRequireVersions(t, store, []cowOrderedVersion{
				{key: key, ts: 10, state: mvcc.Present, value: large},
				{key: key, ts: 20, state: mvcc.Tombstone},
				{key: []byte("unaccepted-peer"), ts: 30, state: mvcc.Present, value: []byte("peer")},
			})
		})
	}
}
