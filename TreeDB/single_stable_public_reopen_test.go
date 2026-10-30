package treedb_test

import (
	"bytes"
	"errors"
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

func singleStablePublicStat(t *testing.T, stats map[string]string, key string) uint64 {
	t.Helper()
	v, err := strconv.ParseUint(stats[key], 10, 64)
	if err != nil {
		t.Fatalf("stat %s=%q: %v", key, stats[key], err)
	}
	return v
}

func singleStablePublicMode(t *testing.T, stats map[string]string) {
	t.Helper()
	if stats["treedb.cache.memtable_mode_config"] != "adaptive" || stats["treedb.cache.memtable_mode"] != "append_only" {
		t.Fatalf("adaptive append-only route inactive: config=%q actual=%q", stats["treedb.cache.memtable_mode_config"], stats["treedb.cache.memtable_mode"])
	}
	if singleStablePublicStat(t, stats, "treedb.cache.memtable_shards") != 2 {
		t.Fatal("fixture must retain two mutable shards")
	}
}

type singleStablePublicReader interface {
	Get([]byte) ([]byte, error)
	Has([]byte) (bool, error)
}

// missingErr is nil for public DB.Get and ErrKeyNotFound for Snapshot.Get.
func singleStablePublicValues(t *testing.T, reader singleStablePublicReader, keys [][]byte, want map[string][]byte, missingErr error) {
	t.Helper()
	for _, key := range keys {
		value, exists := want[string(key)]
		found, err := reader.Has(key)
		if err != nil || found != exists {
			t.Fatalf("Has(%q)=%v,%v want=%v", key, found, err, exists)
		}
		got, err := reader.Get(key)
		if exists {
			if err != nil || !bytes.Equal(got, value) {
				t.Fatalf("Get(%q): equal=%v err=%v", key, bytes.Equal(got, value), err)
			}
		} else if got != nil || !errors.Is(err, missingErr) {
			t.Fatalf("deleted/absent Get(%q): value=%v err=%v want=nil,%v", key, got, err, missingErr)
		}
	}
}

func singleStablePublicPhysical(t *testing.T, d *treedb.DB, want map[string][]byte) map[string]treedb.EntryRevision {
	t.Helper()
	snapshot := d.AcquireSnapshot()
	if snapshot == nil {
		t.Fatal("physical snapshot unavailable")
	}
	defer snapshot.Close()
	revisions := make(map[string]treedb.EntryRevision, len(want))
	for key, value := range want {
		entry, err := snapshot.GetEntryExact([]byte(key))
		if err != nil || entry.Flags&node.FlagPointer == 0 || entry.ValuePtr.FileID == 0 || entry.ValuePtr.Length == 0 {
			t.Fatalf("persistent pointer inactive for %q: %+v err=%v", key, entry, err)
		}
		got, revision, err := snapshot.GetVersioned([]byte(key))
		if err != nil || !bytes.Equal(got, value) {
			t.Fatalf("pointer resolve %q: equal=%v err=%v", key, bytes.Equal(got, value), err)
		}
		revisions[key] = revision // Legacy zero is valid; compare exactly on reopen.
	}
	it, err := snapshot.Iterator(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	seen := make(map[string]bool, len(want))
	for ; it.Valid(); it.Next() {
		key := string(it.KeyCopy(nil))
		value, exists := want[key]
		if !exists || seen[key] || !bytes.Equal(it.ValueCopy(nil), value) {
			t.Fatalf("unexpected/duplicate/incorrect scan key %q", key)
		}
		seen[key] = true
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	if len(seen) != len(want) {
		t.Fatalf("scan count=%d want=%d", len(seen), len(want))
	}
	return revisions
}

// This is an untimed public checkpoint/stable-image/reopen control. Source
// routing plus existing counters establish eligibility; no new branch probe or
// allocation assertion is introduced. The original source stays open while the
// stable-only image reopens, excluding clean-Close writes from that image.
func TestSingleStablePublicAdaptiveCheckpointPointersAndReopen(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileNoWALFast, t.TempDir())
	opts.MemtableMode = "" // Exercise the public default-to-adaptive resolution.
	opts.MemtableShards = 2
	opts.JournalLanes = 1
	opts.DisableSideStores = true
	opts.DisableBackgroundPrune = true
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.BackgroundIndexVacuumInterval = -1
	opts.MaxWALBytes = -1
	opts.FlushThreshold = 1 << 30
	opts.FlushBackendMaxEntries = 2
	opts.FlushBackendMaxBatches = -1
	opts.FlushSpanRunTargetPlanning = false
	opts.ChunkSize = 64 << 10
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true
	opts.ValueLog.Generational.Policy = treedb.ValueLogGenerationOff
	d, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if d != nil {
			_ = d.Close()
		}
	}()
	singleStablePublicMode(t, d.Stats())
	// Plain ASCII keys have no MVCC version-affinity prefix. Use the existing
	// xxhash(key)&(shards-1) mapping, leaving the other shard genuinely empty.
	keys := make([][]byte, 0, 8)
	for n := 0; n < 10000 && len(keys) < 8; n++ {
		key := []byte(fmt.Sprintf("single-stable-public-%08d", n))
		if xxhash.Sum64(key)&1 == 1 {
			keys = append(keys, key)
		}
	}
	if len(keys) != 8 {
		t.Fatal("could not select eight keys in one shard")
	}
	value := func(label string) []byte { return bytes.Repeat([]byte(label+"/"), 512) }
	seed := map[string][]byte{string(keys[0]): value("seed0"), string(keys[1]): value("seed1"), string(keys[2]): value("seed2"), string(keys[3]): value("seed3")}
	type mutation struct {
		index   int
		value   []byte
		deleted bool
	}
	write := func(ms []mutation) {
		t.Helper()
		b := d.NewBatch()
		if b == nil {
			t.Fatal("batch unavailable")
		}
		defer b.Close()
		for _, m := range ms {
			var e error
			if m.deleted {
				e = b.Delete(keys[m.index])
			} else {
				e = b.Set(keys[m.index], m.value)
			}
			if e != nil {
				t.Fatal(e)
			}
		}
		if e := b.Write(); e != nil {
			t.Fatal(e)
		}
	}
	write([]mutation{{index: 0, value: seed[string(keys[0])]}, {index: 1, value: seed[string(keys[1])]}, {index: 2, value: seed[string(keys[2])]}, {index: 3, value: seed[string(keys[3])]}})
	if err := d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	held := d.AcquireSnapshot()
	if held == nil {
		t.Fatal("held snapshot unavailable")
	}
	defer func() {
		if held != nil {
			_ = held.Close()
		}
	}()
	// Capture only after the seed checkpoint and snapshot admission, so neither
	// introduces hidden queued units inside the measured-by-counters window.
	baseline := d.Stats()
	singleStablePublicMode(t, baseline)
	if singleStablePublicStat(t, baseline, "treedb.cache.queue_len") != 0 {
		t.Fatal("snapshot admission left queued work")
	}
	baselineSeq := singleStablePublicStat(t, baseline, "treedb.durable_root.commit_seq")
	model, err := powerlossoracle.Capture(opts.Dir)
	if err != nil {
		t.Fatal(err)
	}
	var observationMu sync.Mutex
	active := true
	metaSyncs := 0
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
		if e.Point == durabilitycut.AfterMetaSync {
			metaSyncs++
		}
		return nil
	})
	var restoreOnce sync.Once
	restore := func() { restoreOnce.Do(restoreObserver) }
	defer restore()
	rounds := []struct {
		ops  []mutation
		want map[string][]byte
	}{
		{
			ops:  []mutation{{index: 4, value: value("r1-new4")}, {index: 0, value: value("discard0")}, {index: 1, deleted: true}, {index: 2, deleted: true}, {index: 2, value: value("r1-revive2")}, {index: 0, value: value("r1-latest0")}, {index: 6, value: value("r1-new6")}},
			want: map[string][]byte{string(keys[0]): value("r1-latest0"), string(keys[2]): value("r1-revive2"), string(keys[3]): seed[string(keys[3])], string(keys[4]): value("r1-new4"), string(keys[6]): value("r1-new6")},
		},
		{
			ops:  []mutation{{index: 1, value: value("r2-revive1")}, {index: 0, deleted: true}, {index: 4, value: value("discard4")}, {index: 5, deleted: true}, {index: 5, value: value("transient5")}, {index: 5, deleted: true}, {index: 4, value: value("r2-latest4")}, {index: 6, value: value("discard6")}, {index: 6, deleted: true}},
			want: map[string][]byte{string(keys[1]): value("r2-revive1"), string(keys[2]): value("r1-revive2"), string(keys[3]): seed[string(keys[3])], string(keys[4]): value("r2-latest4")},
		},
	}
	var finalStats map[string]string
	for n, round := range rounds {
		write(round.ops)
		before := d.Stats()
		singleStablePublicMode(t, before)
		if singleStablePublicStat(t, before, "treedb.cache.queue_len") != 0 {
			t.Fatalf("round%d rotated before Checkpoint", n)
		}
		if err := d.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		after := d.Stats()
		singleStablePublicMode(t, after)
		for _, check := range []struct {
			key   string
			delta uint64
		}{
			{"treedb.cache.flush_span_run.runs_total", 1},
			{"treedb.cache.flush_span_run.source_memtables_total", 1},
			{"treedb.cache.flush_span_run.planned_point_ops_total", 5},
			{"treedb.cache.flush_span_run.backend_chunks_total", 3},
			{"treedb.cache.flush_span_run.shadowed_ops_total", 0},
			{"treedb.cache.flush_span_run.range_delete_ops_total", 0},
			{"treedb.cache.flush_span_run.range_barriers_total", 0},
		} {
			old, next := singleStablePublicStat(t, before, check.key), singleStablePublicStat(t, after, check.key)
			if next < old || next-old != check.delta {
				t.Fatalf("round%d %s delta=%d->%d want=%d", n, check.key, old, next, check.delta)
			}
		}
		if singleStablePublicStat(t, after, "treedb.cache.queue_len") != 0 {
			t.Fatalf("round%d checkpoint retained queue", n)
		}
		// These are existing source-run counters, not a new timing/branch hook.
		// Exactly one nonempty point unit in resolved AppendOnly, with target
		// planning off, must use the stable iterator caller and len(units)==1.
		singleStablePublicValues(t, d, keys, round.want, nil)
		singleStablePublicValues(t, held, keys, seed, treedb.ErrKeyNotFound)
		finalStats = after
	}
	observationMu.Lock()
	active = false
	stable := model.Clone()
	syncsAtCheckpoint := metaSyncs
	observationMu.Unlock()
	restore()
	if syncsAtCheckpoint == 0 {
		t.Fatal("no real checkpoint meta sync observed")
	}
	checkpointSeq := singleStablePublicStat(t, finalStats, "treedb.durable_root.commit_seq")
	if checkpointSeq <= baselineSeq {
		t.Fatalf("checkpoint root did not advance: %d->%d", baselineSeq, checkpointSeq)
	}
	want := rounds[len(rounds)-1].want
	revisions := singleStablePublicPhysical(t, d, want)
	result, reopened, closeReopened, err := powerlossreopen.Stable(stable, opts, false)
	if closeReopened != nil {
		defer func() {
			if closeErr := closeReopened(); closeErr != nil {
				t.Errorf("close stable reopen: %v", closeErr)
			}
		}()
	}
	if err != nil || result.Rejected || reopened == nil {
		t.Fatalf("checkpoint stable image rejected: %+v err=%v", result, err)
	}
	if result.CommitSeq < checkpointSeq {
		t.Fatalf("stable root regressed: %d < %d", result.CommitSeq, checkpointSeq)
	}
	singleStablePublicValues(t, reopened, keys, want, nil)
	gotRevisions := singleStablePublicPhysical(t, reopened, want)
	for key, revision := range revisions {
		if gotRevisions[key] != revision {
			t.Fatalf("stable reopen revision %q=%d want=%d", key, gotRevisions[key], revision)
		}
	}
	// A separate clean close/reopen checks normal directory reuse as well; it
	// cannot contribute bytes to the already frozen stable-only reopen above.
	closingSnapshot := held
	held = nil
	if err := closingSnapshot.Close(); err != nil {
		t.Fatal(err)
	}
	closingDB := d
	d = nil
	if err := closingDB.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	singleStablePublicValues(t, d, keys, want, nil)
	gotRevisions = singleStablePublicPhysical(t, d, want)
	for key, revision := range revisions {
		if gotRevisions[key] != revision {
			t.Fatalf("clean reopen revision %q=%d want=%d", key, gotRevisions[key], revision)
		}
	}
	closingDB = d
	d = nil
	if err := closingDB.Close(); err != nil {
		t.Fatal(err)
	}
}
