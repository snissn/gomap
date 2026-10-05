package caching

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/leafrefscan"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

// Use the real compressed outer-leaf backend and the production scheduler.
// The hook makes repeated whole-record proof exceed the unchanged public
// budget deterministically; certified physical closure must avoid that work.
func TestRetainedValueLogPruneCertifiedQuietProgress(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{
		Dir: dir, IndexOuterLeavesInValueLog: true, LeafPrefixCompression: true,
		DisableBackgroundPrune: true, IndexColumnarLeaves: true, IndexPackedValuePtr: true,
		ValueLog: backenddb.ValueLogOptions{Compression: backenddb.ValueLogCompressionBlock},
	})
	if err != nil {
		t.Fatal(err)
	}
	cache, err := Open(dir, backend, Options{
		DisableWAL: true, RelaxedSync: true, AllowUnsafe: true,
		FlushThreshold: 1 << 20, MemtableShards: 1, JournalLanes: 1,
		IndexOuterLeavesInValueLog: true, ValueLogCompression: 2,
		ValueLogPointerThreshold: 1, MaxValueLogRetainedBytes: 64 << 20,
		ValueLogRewriteTriggerTotalBytes: 1 << 60,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatal(err)
	}
	defer cache.Close()
	cache.testSkipRetainedPrune = true
	value := bytes.Repeat([]byte("compressed-live-value|"), 200)
	for i := 0; i < 512; i++ {
		if err := cache.Set([]byte(fmt.Sprintf("live/%06d", i)), value); err != nil {
			t.Fatal(err)
		}
	}
	if err := cache.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := cache.Set([]byte("slot-advance"), []byte(fmt.Sprint(i))); err != nil {
			t.Fatal(err)
		}
		if err := cache.Checkpoint(); err != nil {
			t.Fatal(err)
		}
	}
	cache.mu.RLock()
	published := clonePublishedRootSet(cache.rootPublishedSet)
	cache.mu.RUnlock()
	if published != nil {
		t.Fatal("ordinary production fixture unexpectedly has detached published roots")
	}

	assertRetainedPruneCompressedLeafFixture(t, backend)
	rawSnapshot := backend.AcquireSnapshot()
	var rawPath string
	for _, file := range rawSnapshot.State().ValueLogSet.Files {
		if filepath.Base(filepath.Dir(file.Path)) == "leaf_vlog" {
			rawPath = file.Path
			break
		}
	}
	_ = rawSnapshot.Close()
	if rawPath == "" {
		t.Fatal("missing raw producer path")
	}
	cache.markValueLogRetain(rawPath)

	// Use the existing scheduler-pressure seam after loading; the physical
	// segment is small, while only scheduling pressure is synthetic.
	stalePath, _ := seedRetainedPruneSegment(t, cache, 406, 128<<20)
	for _, seq := range []uint32{407, 408} {
		path, _ := seedRetainedPruneSegment(t, cache, seq, 128)
		cache.forgetValueLogRetain(path)
	}
	if err := backend.RefreshValueLogSet(); err != nil {
		t.Fatal(err)
	}
	pin := backend.AcquireSnapshot()
	if pin == nil {
		t.Fatal("missing legitimate snapshot pin")
	}
	defer pin.Close()
	if got := cache.retainedPruneBackgroundFullLiveIDScanBudget(); got != 2*time.Second {
		t.Fatalf("production budget=%s", got)
	}
	cache.testRetainedPruneScanHook = func(ctx context.Context, phase string) error {
		if phase == "iterator_record" {
			<-ctx.Done()
			return retainedPruneContextErr(ctx)
		}
		return nil
	}
	cache.testSkipRetainedPrune = false
	cache.lastForegroundWriteUnixNano.Store(time.Now().Add(-2 * retainedPruneQuietWindow).UnixNano())
	cache.lastForegroundReadUnixNano.Store(time.Now().Add(-2 * retainedPruneQuietWindow).UnixNano())
	cache.scheduleRetainedValueLogPrune()
	cache.waitForRetainedValueLogPrune()
	stats := cache.Stats()
	if stats["treedb.cache.vlog_retained_prune.completed_runs"] != "1" || stats["treedb.cache.vlog_retained_prune.budget_abort_runs"] != "0" {
		t.Fatalf("certified quiet prune did not finish: status=%s completed=%s budget_aborts=%s", stats["treedb.cache.vlog_retained_prune.last_status"], stats["treedb.cache.vlog_retained_prune.completed_runs"], stats["treedb.cache.vlog_retained_prune.budget_abort_runs"])
	}
	if stats["treedb.cache.vlog_retained_prune.membership.last_full_root_scans"] != "0" || stats["treedb.cache.vlog_retained_prune.membership.last_uncovered_roots"] != "0" || stats["treedb.cache.vlog_retained_prune.membership.last_gc_calls"] != "1" || stats["treedb.cache.vlog_retained_prune.membership.last_marked_ids"] != "1" || stats["treedb.cache.vlog_retained_prune.membership.last_pending_ids"] != "1" {
		t.Fatalf("production membership/batch work: scans=%s uncovered=%s gc=%s marked=%s pending=%s reason=%s", stats["treedb.cache.vlog_retained_prune.membership.last_full_root_scans"], stats["treedb.cache.vlog_retained_prune.membership.last_uncovered_roots"], stats["treedb.cache.vlog_retained_prune.membership.last_gc_calls"], stats["treedb.cache.vlog_retained_prune.membership.last_marked_ids"], stats["treedb.cache.vlog_retained_prune.membership.last_pending_ids"], stats["treedb.cache.vlog_retained_prune.membership.last_fallback_reason"])
	}
	if !cache.valueLogRetained(rawPath) {
		t.Fatal("main retained pruning forgot raw leaf producer inventory")
	}
	if cache.valueLogRetained(stalePath) {
		t.Fatal("eligible stale identity remained in retained inventory")
	}
	if _, err := os.Stat(stalePath); err != nil {
		t.Fatalf("pinned physical segment retired early: %v", err)
	}
	if err := pin.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(stalePath); !os.IsNotExist(err) {
		t.Fatalf("marked physical segment not retired after pin release: %v", err)
	}
	for i := 0; i < 512; i++ {
		got, err := cache.Get([]byte(fmt.Sprintf("live/%06d", i)))
		if err != nil || !bytes.Equal(got, value) {
			t.Fatalf("live oracle key=%d: %v", i, err)
		}
	}
}

func TestRetainedValueLogPruneBatchCutoverFences(t *testing.T) {
	for _, mode := range []string{"foreground", "domain", "contention", "cancel", "stale-publication"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			backend, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
			if err != nil {
				t.Fatal(err)
			}
			cache, err := Open(dir, backend, Options{DisableWAL: true, RelaxedSync: true, AllowUnsafe: true, FlushThreshold: 1 << 20, ValueLogPointerThreshold: 1})
			if err != nil {
				_ = backend.Close()
				t.Fatal(err)
			}
			defer cache.Close()
			cache.testSkipRetainedPrune = true
			if err := cache.Set([]byte("live"), []byte("persistent-live")); err != nil {
				t.Fatal(err)
			}
			if err := cache.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			path, id := seedRetainedPruneSegment(t, cache, 406, 128)
			for _, seq := range []uint32{407, 408} {
				recent, _ := seedRetainedPruneSegment(t, cache, seq, 128)
				cache.forgetValueLogRetain(recent)
			}
			if err := backend.RefreshValueLogSet(); err != nil {
				t.Fatal(err)
			}
			cache.lastForegroundWriteUnixNano.Store(time.Now().Add(-2 * retainedPruneQuietWindow).UnixNano())
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			held := false
			defer func() {
				if held {
					cache.writeMu.Unlock()
				}
			}()
			cache.testRetainedPruneScanHook = func(ctx context.Context, phase string) error {
				if phase != "before_batch_gc" {
					return nil
				}
				switch mode {
				case "foreground":
					cache.lastForegroundWriteUnixNano.Store(time.Now().UnixNano())
				case "domain":
					cache.rootDomainVersion.Add(1)
				case "contention":
					cache.writeMu.Lock()
					held = true
				case "cancel":
					cancel()
				case "stale-publication":
					if err := backend.Set([]byte("backend-change"), []byte("changed")); err != nil {
						return err
					}
				}
				return nil
			}
			out := cache.pruneRetainedValueLogsWithObservedContext(ctx, false, nil)
			if held {
				cache.writeMu.Unlock()
				held = false
			}
			if out.ZombieMarkedSegments != 0 || len(out.GCStats.ZombieMarkedFileIDs) != 0 || !cache.valueLogRetained(path) {
				t.Fatalf("unfenced mutation mode=%s marked=%d retained=%t", mode, out.ZombieMarkedSegments, cache.valueLogRetained(path))
			}
			if _, err := os.Stat(path); err != nil {
				t.Fatalf("unfenced file retired: %v", err)
			}
			if mode == "foreground" || mode == "domain" || mode == "contention" {
				if !out.AbortedForegroundWrites {
					t.Fatalf("missing foreground abort mode=%s", mode)
				}
			} else if !out.ScanError {
				t.Fatalf("missing cancellation/stale abort mode=%s", mode)
			}
			// The writer fence must have been released on every exit.
			if !cache.writeMu.TryLock() {
				t.Fatal("cutover leaked writer lock")
			}
			cache.writeMu.Unlock()
			if err := backend.MarkValueLogZombie(id); err != nil && !errors.Is(err, backenddb.ErrValueLogZombieDeferred) {
				t.Fatal(err)
			}
		})
	}
}

// A closed file can still be one of the manager's newest two sequences. The
// explicit maintenance fence distinguishes it from the actual writable lane;
// a force request used only for scheduler admission must not make that claim.
func TestRetainedValueLogPruneClosedRecentAuthorization(t *testing.T) {
	for _, mode := range []string{"ordinary", "scheduled-force", "explicit-force", "observed-force", "became-in-use"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			backend, err := backenddb.Open(backenddb.Options{
				Dir: dir, DisableBackgroundPrune: true, IndexOuterLeavesInValueLog: true,
			})
			if err != nil {
				t.Fatal(err)
			}
			cache, err := Open(dir, backend, Options{
				DisableWAL: true, RelaxedSync: true, AllowUnsafe: true,
				FlushThreshold: 1 << 20, ValueLogPointerThreshold: 1,
				IndexOuterLeavesInValueLog: true,
			})
			if err != nil {
				_ = backend.Close()
				t.Fatal(err)
			}
			defer cache.Close()
			cache.testSkipRetainedPrune = true
			for i := 0; i < 3; i++ {
				if err := cache.Set([]byte("live/root-horizon"), []byte(fmt.Sprint(i))); err != nil {
					t.Fatal(err)
				}
				if err := cache.Checkpoint(); err != nil {
					t.Fatal(err)
				}
			}
			writable := cache.currentValueLogPaths()
			if len(writable) == 0 {
				t.Fatal("missing actual writable lane")
			}
			for _, path := range writable {
				cache.markValueLogRetain(path)
			}
			closed, id := seedRetainedPruneSegment(t, cache, 406, 128)
			if err := backend.RefreshValueLogSet(); err != nil {
				t.Fatal(err)
			}
			cache.lastForegroundWriteUnixNano.Store(time.Now().Add(-2 * retainedPruneQuietWindow).UnixNano())
			if mode == "became-in-use" {
				cache.testRetainedPruneScanHook = func(_ context.Context, phase string) error {
					if phase == "before_batch_gc" {
						cache.mu.Lock()
						cache.queueValueLogPaths = append(cache.queueValueLogPaths, []string{closed})
						cache.mu.Unlock()
					}
					return nil
				}
			}
			var out retainedValueLogPruneStats
			switch mode {
			case "explicit-force", "became-in-use":
				out = cache.pruneRetainedValueLogs(true)
			case "observed-force":
				out = cache.pruneRetainedValueLogsWithObservedContextOptions(context.Background(), true,
					map[uint32]struct{}{id: {}}, retainedValueLogPruneRunOptions{})
			default:
				out = cache.pruneRetainedValueLogsWithObservedContextOptions(context.Background(),
					mode == "scheduled-force", nil, retainedValueLogPruneRunOptions{})
			}
			if mode == "became-in-use" {
				cache.mu.Lock()
				cache.queueValueLogPaths = cache.queueValueLogPaths[:len(cache.queueValueLogPaths)-1]
				cache.mu.Unlock()
			}
			wantMark := mode == "explicit-force" || mode == "observed-force"
			if got := len(out.GCStats.ZombieMarkedFileIDs); (got == 1) != wantMark || got > 1 {
				t.Fatalf("mode=%s marked=%v active=%d stats=%+v", mode, out.GCStats.ZombieMarkedFileIDs, out.GCStats.SegmentsActive, out)
			}
			if actions := out.RemovedSegments + out.ZombieMarkedSegments; (actions == 1) != wantMark || actions > 1 {
				t.Fatalf("mode=%s duplicate or missing legacy action count: removed=%d pending=%d", mode, out.RemovedSegments, out.ZombieMarkedSegments)
			}
			if cache.valueLogRetained(closed) == wantMark {
				t.Fatalf("mode=%s retained closed candidate disagrees with actual marks", mode)
			}
			if mode == "became-in-use" && !out.AbortedForegroundWrites {
				t.Fatalf("late in-use change did not abort: %+v", out)
			}
			if !wantMark && mode != "became-in-use" && out.GCStats.SegmentsActive != 1 {
				t.Fatalf("ordinary guard did not classify recent closed file: %+v", out.GCStats)
			}
			for _, path := range writable {
				if _, err := os.Stat(path); err != nil {
					t.Fatalf("actual writable lane reclaimed: %v", err)
				}
			}
			got, err := cache.Get([]byte("live/root-horizon"))
			if err != nil || !bytes.Equal(got, []byte("2")) {
				t.Fatalf("live oracle: value=%q err=%v", got, err)
			}
		})
	}
}

func TestRetainedPruneDetachedRootIDEqualityRequiresProjection(t *testing.T) {
	for _, domain := range []string{"point", "system", "iterator"} {
		t.Run(domain, func(t *testing.T) {
			dir := t.TempDir()
			backend, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
			if err != nil {
				t.Fatal(err)
			}
			cache, err := Open(dir, backend, Options{DisableWAL: true, RelaxedSync: true, AllowUnsafe: true, FlushThreshold: 1 << 20, ValueLogPointerThreshold: 1})
			if err != nil {
				_ = backend.Close()
				t.Fatal(err)
			}
			defer cache.Close()
			cache.testSkipRetainedPrune = true
			if err := cache.Set([]byte("live"), []byte("persistent-live")); err != nil {
				t.Fatal(err)
			}
			if err := cache.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			if domain == "system" {
				if _, err := backend.PublishSystemRootIterator(mustFrozenRawRootTable(t, "system/fixture", "persisted-system-entry").NewIterator(nil, nil)); err != nil {
					t.Fatal(err)
				}
			}

			snap := backend.AcquireSnapshot()
			state := snap.State()
			published := &publishedRootSet{generation: 1}
			switch domain {
			case "point":
				published.pointShards = []publishedRootRef{{rootID: state.RootPageID}}
			case "system":
				published.system.rootID = state.SystemRootPageID
			case "iterator":
				published.iterator.rootID = state.RootPageID
			}
			defer snap.Close()
			cache.mu.Lock()
			cache.rootPublishedSet = published
			cache.mu.Unlock()
			defer func() { cache.mu.Lock(); cache.rootPublishedSet = nil; cache.mu.Unlock() }()
			for _, collector := range []struct {
				name    string
				collect func(context.Context, int64, *valueLogLiveIDScanStats) (map[uint32]struct{}, error)
			}{
				{"protected", cache.collectRetainedPruneProtectedValueLogIDs},
				{"visible", cache.collectValueLogLiveIDsUntilWithContext},
			} {
				records := 0
				cache.testRetainedPruneScanHook = func(ctx context.Context, phase string) error {
					if phase == "iterator_record" {
						records++
					}
					return nil
				}
				var work valueLogLiveIDScanStats
				if _, err := collector.collect(context.Background(), 0, &work); err != nil {
					t.Fatal(err)
				}
				// Membership fallback has separate counters. Records here prove
				// the protected collector actually projects the detached root.
				if records == 0 || work.Records == 0 {
					t.Fatalf("%s collector skipped matching detached %s root: records=%d", collector.name, domain, records)
				}
			}
		})
	}
}

// Inspect published leaf references and stored frame headers independently of
// cache admission/codec counters before installing the adversarial proof hook.
func assertRetainedPruneCompressedLeafFixture(t *testing.T, backend *backenddb.DB) {
	t.Helper()
	snap := backend.AcquireSnapshot()
	if snap == nil {
		t.Fatal("missing fixture snapshot")
	}
	defer snap.Close()
	state := snap.State()
	leaves := 0
	if err := leafrefscan.Walk(context.Background(), state.RootPageID, snap.Pager().Get, nil, func(ptr page.LeafLogPtr) error {
		file := state.ValueLogSet.Files[ptr.ValueLogFileID()]
		if file == nil || filepath.Base(filepath.Dir(file.Path)) != "leaf_vlog" {
			return fmt.Errorf("outer leaf lacks registered raw leaf producer: id=%d", ptr.FileID)
		}
		leaves++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if leaves == 0 {
		t.Fatal("fixture has no published outer leaf references")
	}
	compressed := 0
	for id, file := range state.ValueLogSet.Files {
		reader, err := valuelog.NewReader(file.Path, id)
		if err != nil {
			t.Fatal(err)
		}
		for {
			_, ptr, err := reader.ReadNextMeta()
			if err == io.EOF {
				break
			}
			if err != nil {
				_ = reader.Close()
				t.Fatal(err)
			}
			if !page.ValuePtrIsGrouped(ptr) {
				continue
			}
			var header [valuelog.FrameHeaderSize]byte
			if _, err := file.File.ReadAt(header[:], int64(ptr.Offset)+valuelog.HeaderSize-4); err != nil {
				_ = reader.Close()
				t.Fatal(err)
			}
			if header[0] == valuelog.FrameVersion && header[1]&valuelog.FrameFlagCompressed != 0 && header[3] == byte(valuelog.BlockCodecSnappy) {
				compressed++
			}
		}
		if err := reader.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if compressed == 0 {
		t.Fatal("fixture has no physically stored Snappy compressed frames")
	}
}
