package treedb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestOpenBackendWithCachedLeafLog_CompactStorageNoDeletedFDs(t *testing.T) {
	dir := t.TempDir()
	opts := OptionsFor(ProfileNoWALFast, dir)
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.BackgroundIndexVacuumInterval = -1
	opts.MaxWALBytes = -1
	opts.JournalLanes = 4
	opts.IndexOuterLeavesInValueLog = true
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true
	opts.ValueLog.Compression = ValueLogCompressionOff
	opts.ValueLog.Generational.Policy = ValueLogGenerationHotWarmCold
	opts.ValueLog.Generational.HotSegmentTargetBytes = 8 << 10

	valueLogDir := backenddb.ValueLogDirPath(dir)
	ctx := context.Background()
	expected := make(map[string][]byte)

	const cycles = 3
	const keysPerCycle = 128
	const roundsPerCycle = 8

	makePayload := func(cycle, round, i int) []byte {
		buf := make([]byte, 1024)
		x := uint64(cycle+1)*0x9e3779b97f4a7c15 ^ uint64(round+1)*0xbf58476d1ce4e5b9 ^ uint64(i+1)*0x94d049bb133111eb
		for off := 0; off < len(buf); off += 8 {
			x ^= x >> 12
			x ^= x << 25
			x ^= x >> 27
			v := x * 0x2545f4914f6cdd1d
			buf[off+0] = byte(v)
			buf[off+1] = byte(v >> 8)
			buf[off+2] = byte(v >> 16)
			buf[off+3] = byte(v >> 24)
			buf[off+4] = byte(v >> 32)
			buf[off+5] = byte(v >> 40)
			buf[off+6] = byte(v >> 48)
			buf[off+7] = byte(v >> 56)
		}
		return buf
	}

	for cycle := 0; cycle < cycles; cycle++ {
		backend, cleanup, statsFn, err := OpenBackendWithCachedLeafLogStats(opts)
		if err != nil {
			t.Fatalf("cycle %d OpenBackendWithCachedLeafLogStats: %v", cycle, err)
		}

		// Verify reopen did not explode effective or hot lanes after prior rewrite lanes (e.g. lane 254).
		st := statsFn()
		if st == nil {
			_ = cleanup()
			t.Fatalf("cycle %d statsFn returned nil", cycle)
		}
		rawEffective, ok := st["treedb.cache.journal_lanes.effective"]
		if !ok {
			_ = cleanup()
			t.Fatalf("cycle %d missing stat treedb.cache.journal_lanes.effective", cycle)
		}
		effectiveLanes, parseErr := strconv.Atoi(rawEffective)
		if parseErr != nil {
			_ = cleanup()
			t.Fatalf("cycle %d parse treedb.cache.journal_lanes.effective=%q: %v", cycle, rawEffective, parseErr)
		}
		if effectiveLanes > opts.JournalLanes {
			t.Errorf("cycle %d reopen exploded effective lanes: got %d, want <= %d", cycle, effectiveLanes, opts.JournalLanes)
		}
		rawHot, ok := st["treedb.cache.journal_lanes.hot"]
		if !ok {
			_ = cleanup()
			t.Fatalf("cycle %d missing stat treedb.cache.journal_lanes.hot", cycle)
		}
		hotLanes, parseErr := strconv.Atoi(rawHot)
		if parseErr != nil {
			_ = cleanup()
			t.Fatalf("cycle %d parse treedb.cache.journal_lanes.hot=%q: %v", cycle, rawHot, parseErr)
		}
		if hotLanes > opts.JournalLanes {
			t.Errorf("cycle %d reopen exploded hot lanes: got %d, want <= %d", cycle, hotLanes, opts.JournalLanes)
		}

		// Verify pointer durability across reopen BEFORE overwriting keys in the
		// next cycle so missing/corrupt values cannot be masked by new writes.
		if len(expected) > 0 {
			for k, want := range expected {
				if len(want) == 0 {
					exists, hasErr := backend.Has([]byte(k))
					if hasErr != nil || !exists {
						_ = cleanup()
						t.Fatalf("cycle %d pre-write reopen Has(%q) = (%v, %v), want true", cycle, k, exists, hasErr)
					}
				}
				got, getErr := backend.Get([]byte(k))
				if getErr != nil {
					_ = cleanup()
					t.Fatalf("cycle %d pre-write reopen Get(%q): %v", cycle, k, getErr)
				}
				if !bytes.Equal(got, want) {
					_ = cleanup()
					t.Fatalf("cycle %d pre-write reopen Get(%q) mismatch: got %d bytes, want %d bytes", cycle, k, len(got), len(want))
				}
			}
		}

		// Write and overwrite values via the backend value-log appender so
		// value_vlog rotates multiple sealed segments and accumulates both
		// unreferenced and partially-stale segments eligible for rewrite +
		// ValueLogGC during CompactStorage.
		for round := 0; round < roundsPerCycle; round++ {
			keys := make([][]byte, keysPerCycle)
			vals := make([][]byte, keysPerCycle)
			for i := 0; i < keysPerCycle; i++ {
				keys[i] = []byte(fmt.Sprintf("k-%04d", i))
				vals[i] = makePayload(cycle, round, i)
				expected[string(keys[i])] = vals[i]
			}
			ptrs, err := backend.AppendValueLogValues(vals)
			if err != nil {
				_ = cleanup()
				t.Fatalf("cycle %d round %d AppendValueLogValues: %v", cycle, round, err)
			}
			b := backend.NewBatch()
			pb, ok := b.(interface {
				SetPointer([]byte, page.ValuePtr) error
			})
			if !ok {
				_ = b.Close()
				_ = cleanup()
				t.Fatalf("cycle %d round %d NewBatch(%T) does not support SetPointer", cycle, round, b)
			}
			for i := range keys {
				if err := pb.SetPointer(keys[i], ptrs[i]); err != nil {
					_ = b.Close()
					_ = cleanup()
					t.Fatalf("cycle %d round %d SetPointer: %v", cycle, round, err)
				}
			}
			if err := b.WriteSync(); err != nil {
				_ = b.Close()
				_ = cleanup()
				t.Fatalf("cycle %d round %d WriteSync: %v", cycle, round, err)
			}
			if err := b.Close(); err != nil {
				_ = cleanup()
				t.Fatalf("cycle %d round %d Batch.Close: %v", cycle, round, err)
			}
		}
		// Overwrite 90% of keys in a partial round so the prior sealed segments
		// have high stale ratio (eligible for ValueLogRewriteOnline into lane 254+),
		// then commit two inline empty-value updates so KeepRecent=1 advances past
		// older recoverable roots.
		{
			partialCount := (keysPerCycle * 9) / 10
			keys := make([][]byte, partialCount)
			vals := make([][]byte, partialCount)
			for i := 0; i < partialCount; i++ {
				keys[i] = []byte(fmt.Sprintf("k-%04d", i))
				vals[i] = makePayload(cycle, roundsPerCycle, i)
				expected[string(keys[i])] = vals[i]
			}
			ptrs, err := backend.AppendValueLogValues(vals)
			if err != nil {
				_ = cleanup()
				t.Fatalf("cycle %d partial AppendValueLogValues: %v", cycle, err)
			}
			b := backend.NewBatch()
			pb := b.(interface {
				SetPointer([]byte, page.ValuePtr) error
			})
			for i := range keys {
				if err := pb.SetPointer(keys[i], ptrs[i]); err != nil {
					_ = b.Close()
					_ = cleanup()
					t.Fatalf("cycle %d partial SetPointer: %v", cycle, err)
				}
			}
			if err := b.WriteSync(); err != nil {
				_ = b.Close()
				_ = cleanup()
				t.Fatalf("cycle %d partial WriteSync: %v", cycle, err)
			}
			if err := b.Close(); err != nil {
				_ = cleanup()
				t.Fatalf("cycle %d partial Batch.Close: %v", cycle, err)
			}
		}
		for partial := 0; partial < 2; partial++ {
			b := backend.NewBatch()
			key := []byte(fmt.Sprintf("meta-bump-%d-%d", cycle, partial))
			if err := b.Set(key, nil); err != nil {
				_ = b.Close()
				_ = cleanup()
				t.Fatalf("cycle %d meta-bump Set: %v", cycle, err)
			}
			expected[string(key)] = []byte{}
			if err := b.WriteSync(); err != nil {
				_ = b.Close()
				_ = cleanup()
				t.Fatalf("cycle %d meta-bump WriteSync: %v", cycle, err)
			}
			if err := b.Close(); err != nil {
				_ = cleanup()
				t.Fatalf("cycle %d meta-bump Close: %v", cycle, err)
			}
		}

		// Capture pre-compaction value_vlog segment identities & inodes.
		type trackedSeg struct {
			path     string
			identity rootpublication.StableIdentity
			inode    segmentInode
			hasInode bool
		}
		var preSegments []trackedSeg
		entries, err := os.ReadDir(valueLogDir)
		if err != nil {
			_ = cleanup()
			t.Fatalf("cycle %d ReadDir(value_vlog): %v", cycle, err)
		}
		for _, entry := range entries {
			if entry.IsDir() || filepath.Ext(entry.Name()) != ".log" {
				continue
			}
			p := filepath.Join(valueLogDir, entry.Name())
			f, openErr := os.Open(p)
			if openErr != nil {
				_ = cleanup()
				t.Fatalf("cycle %d Open(%s): %v", cycle, p, openErr)
			}
			id, idErr := rootpublication.StableIdentityFromFile(f)
			_ = f.Close()
			if idErr != nil {
				_ = cleanup()
				t.Fatalf("cycle %d StableIdentityFromFile(%s): %v", cycle, p, idErr)
			}
			ino, hasIno := statSegmentInode(p)
			preSegments = append(preSegments, trackedSeg{
				path:     p,
				identity: id,
				inode:    ino,
				hasInode: hasIno,
			})
		}

		if _, err := backend.CompactStorage(ctx, backenddb.CompactStorageOptions{
			Mode:                                backenddb.CompactStorageFull,
			ValueLogRewriteMinSegmentStaleRatio: 0.01,
			ValueLogRewriteMinSegmentStaleBytes: 1,
		}); err != nil {
			_ = cleanup()
			t.Fatalf("cycle %d CompactStorage: %v", cycle, err)
		}

		registry := backend.ValueLogIdentityPinRegistry()
		deletedInodes := make(map[segmentInode]string)
		var staleObservers uint64
		deletedCount := 0
		for _, seg := range preSegments {
			if _, statErr := os.Stat(seg.path); os.IsNotExist(statErr) {
				deletedCount++
				if seg.hasInode {
					deletedInodes[seg.inode] = seg.path
				}
				if obs := registry.ObserverCount(seg.identity); obs != 0 {
					staleObservers += obs
					t.Errorf("cycle %d deleted segment %s still has registry ObserverCount=%d (retired=%t)",
						cycle, filepath.Base(seg.path), obs, registry.IsRetired(seg.identity))
				}
			}
		}
		if deletedCount == 0 {
			_ = cleanup()
			t.Fatalf("cycle %d expected at least one pre-existing value_vlog file to be unlinked", cycle)
		}

		matchedInodeFDs, procDeletedFDs, fdCheckAvailable := countOpenDeletedSegmentFDs(t, valueLogDir, deletedInodes)
		if !fdCheckAvailable {
			if runtime.GOOS == "linux" || runtime.GOOS == "darwin" {
				t.Errorf("cycle %d FD scan unavailable on %s", cycle, runtime.GOOS)
			} else {
				t.Logf("cycle %d FD scan unavailable on %s; relying on registry observer assertions", cycle, runtime.GOOS)
			}
		}
		t.Logf("cycle %d metrics: hotLanes=%d preSegments=%d deleted=%d staleObservers=%d matchedInodeFDs=%d procDeletedFDs=%d fdCheckAvailable=%t",
			cycle, hotLanes, len(preSegments), deletedCount, staleObservers, matchedInodeFDs, procDeletedFDs, fdCheckAvailable)
		if fdCheckAvailable && (matchedInodeFDs != 0 || procDeletedFDs != 0) {
			t.Errorf("cycle %d leaked deleted value_vlog FDs after CompactStorage: matchedInodeFDs=%d procDeletedFDs=%d staleObservers=%d",
				cycle, matchedInodeFDs, procDeletedFDs, staleObservers)
		}

		// Read-back all expected keys (including Has check for empty values) before closing this cycle.
		for k, want := range expected {
			if len(want) == 0 {
				exists, hasErr := backend.Has([]byte(k))
				if hasErr != nil || !exists {
					_ = cleanup()
					t.Fatalf("cycle %d Has(%q)=(%t, %v), want (true, nil)", cycle, k, exists, hasErr)
				}
			}
			got, getErr := backend.Get([]byte(k))
			if getErr != nil {
				_ = cleanup()
				t.Fatalf("cycle %d Get(%q): %v", cycle, k, getErr)
			}
			if !bytes.Equal(got, want) {
				_ = cleanup()
				t.Fatalf("cycle %d Get(%q) mismatch: got %dB want %dB", cycle, k, len(got), len(want))
			}
		}

		if err := cleanup(); err != nil {
			t.Fatalf("cycle %d cleanup: %v", cycle, err)
		}
	}

	// Final reopen verification.
	reopen, cleanup, err := OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		t.Fatalf("final reopen OpenBackendWithCachedLeafLog: %v", err)
	}
	defer func() {
		if err := cleanup(); err != nil {
			t.Errorf("final reopen cleanup: %v", err)
		}
	}()
	for k, want := range expected {
		if len(want) == 0 {
			exists, hasErr := reopen.Has([]byte(k))
			if hasErr != nil || !exists {
				t.Fatalf("final reopen Has(%q)=(%t, %v), want (true, nil)", k, exists, hasErr)
			}
		}
		got, err := reopen.Get([]byte(k))
		if err != nil {
			t.Fatalf("final reopen Get(%q): %v", k, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("final reopen Get(%q) mismatch", k)
		}
	}
}

func TestPostValueLogGCHookErrorPropagatesAndRunsOutsidePublishLocks(t *testing.T) {
	dir := t.TempDir()
	opts := OptionsFor(ProfileNoWALFast, dir)
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.BackgroundIndexVacuumInterval = -1
	opts.MaxWALBytes = -1
	opts.JournalLanes = 2
	opts.IndexOuterLeavesInValueLog = true
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true
	opts.ValueLog.Compression = ValueLogCompressionOff
	opts.ValueLog.Generational.Policy = ValueLogGenerationHotWarmCold
	opts.ValueLog.Generational.HotSegmentTargetBytes = 8 << 10

	backend, cleanup, err := OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		t.Fatalf("OpenBackendWithCachedLeafLog: %v", err)
	}
	defer func() {
		if err := cleanup(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()

	// Create multiple sealed value-log segments and overwrite them so GC deletes segments.
	for round := 0; round < 6; round++ {
		vals := make([][]byte, 64)
		for i := range vals {
			buf := make([]byte, 512)
			for j := range buf {
				buf[j] = byte((round*31 + i*17 + j*13) & 0xff)
			}
			vals[i] = buf
		}
		ptrs, err := backend.AppendValueLogValues(vals)
		if err != nil {
			t.Fatalf("round %d AppendValueLogValues: %v", round, err)
		}
		b := backend.NewBatch()
		pb := b.(interface {
			SetPointer([]byte, page.ValuePtr) error
		})
		for i := range vals {
			if err := pb.SetPointer([]byte(fmt.Sprintf("hk-%03d", i)), ptrs[i]); err != nil {
				_ = b.Close()
				t.Fatalf("SetPointer: %v", err)
			}
		}
		if err := b.WriteSync(); err != nil {
			_ = b.Close()
			t.Fatalf("WriteSync: %v", err)
		}
		if err := b.Close(); err != nil {
			t.Fatalf("Batch.Close: %v", err)
		}
	}
	for i := 0; i < 2; i++ {
		b := backend.NewBatch()
		if err := b.Set([]byte(fmt.Sprintf("hb-%d", i)), nil); err != nil {
			_ = b.Close()
			t.Fatalf("Set: %v", err)
		}
		if err := b.WriteSync(); err != nil {
			_ = b.Close()
			t.Fatalf("WriteSync: %v", err)
		}
		if err := b.Close(); err != nil {
			t.Fatalf("Batch.Close: %v", err)
		}
	}

	ctx := context.Background()
	hookCalls := 0
	injectedErr := errors.New("injected post-gc reconcile failure")
	backend.SetPostValueLogGCHook(func() error {
		hookCalls++
		// Writing a tiny inline key synchronously requires publishPrepareMu
		// (and RefreshValueLogSet requires valueLogPublicationMu); succeeding
		// here proves neither lock is held when the hook runs.
		if err := backend.RefreshValueLogSet(); err != nil {
			return fmt.Errorf("RefreshValueLogSet inside post-GC hook failed: %w", err)
		}
		b := backend.NewBatch()
		if err := b.Set([]byte("hook-lock-probe"), nil); err != nil {
			_ = b.Close()
			return err
		}
		err := b.WriteSync()
		_ = b.Close()
		if err != nil {
			return err
		}
		return injectedErr
	})

	// DryRun must never invoke the post-GC hook.
	if _, err := backend.ValueLogGC(ctx, backenddb.ValueLogGCOptions{DryRun: true}); err != nil {
		t.Fatalf("ValueLogGC DryRun: %v", err)
	}
	if hookCalls != 0 {
		t.Fatalf("DryRun invoked post-GC hook %d times, want 0", hookCalls)
	}

	// Non-dry-run ValueLogGC must run the hook outside publish locks and propagate its error.
	gcStats, err := backend.ValueLogGC(ctx, backenddb.ValueLogGCOptions{})
	if !errors.Is(err, injectedErr) {
		t.Fatalf("ValueLogGC error=%v, want %v (stats=%+v, hookCalls=%d)", err, injectedErr, gcStats, hookCalls)
	}
	if hookCalls != 1 {
		t.Fatalf("post-GC hook calls=%d, want 1", hookCalls)
	}

	// Verify CompactStorage also invokes the post-GC hook after its final
	// retirement phases (including zero-byte value-log pruning) outside publish
	// locks and propagates hook errors WITHOUT wrapping in ErrRecoveryRequired.
	valueLogDir := backenddb.ValueLogDirPath(dir)
	zeroID, err := valuelog.EncodeFileID(253, 1)
	if err != nil {
		t.Fatalf("EncodeFileID zero-byte segment: %v", err)
	}
	zeroPath := valuelog.SegmentPath(valueLogDir, zeroID)
	if err := os.WriteFile(zeroPath, nil, 0o600); err != nil {
		t.Fatalf("WriteFile(%s): %v", zeroPath, err)
	}
	if err := backend.RefreshValueLogSet(); err != nil {
		t.Fatalf("RefreshValueLogSet after creating zero-byte segment: %v", err)
	}
	compactHookErr := errors.New("injected final compact-storage post-gc hook failure")
	compactHookCallsAfterZeroPrune := 0
	backend.SetPostValueLogGCHook(func() error {
		// Succeed on intermediate GC passes; fail on the final pass after the
		// zero-byte segment has been unlinked so we specifically exercise the
		// post-retirement final CompactStorage reconciliation boundary.
		if _, statErr := os.Stat(zeroPath); os.IsNotExist(statErr) {
			compactHookCallsAfterZeroPrune++
			if err := backend.RefreshValueLogSet(); err != nil {
				return fmt.Errorf("RefreshValueLogSet inside final CompactStorage hook: %w", err)
			}
			return compactHookErr
		}
		return nil
	})
	compactStats, err := backend.CompactStorage(ctx, backenddb.CompactStorageOptions{})
	if !errors.Is(err, compactHookErr) {
		t.Fatalf("CompactStorage error=%v, want %v (stats=%+v)", err, compactHookErr, compactStats)
	}
	if errors.Is(err, backenddb.ErrRecoveryRequired) {
		t.Fatalf("CompactStorage wrongly wrapped secondary post-GC hook error with ErrRecoveryRequired: %v", err)
	}
	if compactHookCallsAfterZeroPrune == 0 {
		t.Fatalf("expected post-GC hook to run after zero-byte segment pruning (ZeroByteValueLogFilesDeleted=%d)", compactStats.ZeroByteValueLogFilesDeleted)
	}
	if _, statErr := os.Stat(zeroPath); !os.IsNotExist(statErr) {
		t.Fatalf("expected zero-byte segment %s to be pruned, stat err=%v", zeroPath, statErr)
	}
}

func TestOpen_CompactStorageNoDeletedFDs(t *testing.T) {
	dir := t.TempDir()
	opts := OptionsFor(ProfileNoWALFast, dir)
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.BackgroundIndexVacuumInterval = -1
	opts.MaxWALBytes = -1
	opts.JournalLanes = 4
	opts.IndexOuterLeavesInValueLog = false
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true
	opts.ValueLog.Compression = ValueLogCompressionOff
	opts.ValueLog.Generational.Policy = ValueLogGenerationHotWarmCold
	opts.ValueLog.Generational.HotSegmentTargetBytes = 8 << 10

	valueLogDir := backenddb.ValueLogDirPath(dir)
	ctx := context.Background()

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()

	expected := make(map[string][]byte)
	for round := 0; round < 6; round++ {
		b := db.NewBatch()
		for i := 0; i < 64; i++ {
			k := []byte(fmt.Sprintf("ok-%04d", i))
			v := make([]byte, 1024)
			for j := range v {
				v[j] = byte((round*41 + i*19 + j*7) & 0xff)
			}
			if err := b.Set(k, v); err != nil {
				_ = b.Close()
				t.Fatalf("Set: %v", err)
			}
			expected[string(k)] = v
		}
		if err := b.WriteSync(); err != nil {
			_ = b.Close()
			t.Fatalf("WriteSync: %v", err)
		}
		if err := b.Close(); err != nil {
			t.Fatalf("Batch.Close: %v", err)
		}
		if err := db.Checkpoint(); err != nil {
			t.Fatalf("Checkpoint: %v", err)
		}
	}

	type trackedSeg struct {
		path     string
		identity rootpublication.StableIdentity
		inode    segmentInode
		hasInode bool
	}
	var preSegments []trackedSeg
	entries, err := os.ReadDir(valueLogDir)
	if err != nil {
		t.Fatalf("ReadDir(value_vlog): %v", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".log" {
			continue
		}
		p := filepath.Join(valueLogDir, entry.Name())
		f, err := os.Open(p)
		if err != nil {
			t.Fatalf("Open(%s): %v", p, err)
		}
		id, idErr := rootpublication.StableIdentityFromFile(f)
		_ = f.Close()
		if idErr != nil {
			t.Fatalf("StableIdentityFromFile(%s): %v", p, idErr)
		}
		ino, hasIno := statSegmentInode(p)
		preSegments = append(preSegments, trackedSeg{path: p, identity: id, inode: ino, hasInode: hasIno})
	}

	if _, err := db.CompactStorage(ctx, CompactStorageOptions{
		Mode:                                CompactStorageFull,
		ValueLogRewriteMinSegmentStaleRatio: 0.01,
		ValueLogRewriteMinSegmentStaleBytes: 1,
	}); err != nil {
		t.Fatalf("CompactStorage: %v", err)
	}

	registry := db.backend.ValueLogIdentityPinRegistry()
	deletedInodes := make(map[segmentInode]string)
	deletedCount := 0
	for _, seg := range preSegments {
		if _, err := os.Stat(seg.path); os.IsNotExist(err) {
			deletedCount++
			if seg.hasInode {
				deletedInodes[seg.inode] = seg.path
			}
			if obs := registry.ObserverCount(seg.identity); obs != 0 {
				t.Errorf("deleted segment %s still has ObserverCount=%d", filepath.Base(seg.path), obs)
			}
		}
	}
	if deletedCount == 0 {
		t.Fatalf("expected CompactStorage to delete at least 1 segment")
	}
	matchedInodeFDs, procDeletedFDs, fdCheckAvailable := countOpenDeletedSegmentFDs(t, valueLogDir, deletedInodes)
	if !fdCheckAvailable {
		if runtime.GOOS == "linux" || runtime.GOOS == "darwin" {
			t.Errorf("FD scan unavailable on %s", runtime.GOOS)
		} else {
			t.Logf("FD scan unavailable on %s; relying on registry observer assertions", runtime.GOOS)
		}
	}
	if fdCheckAvailable && (matchedInodeFDs != 0 || procDeletedFDs != 0) {
		t.Errorf("leaked deleted FDs after treedb.Open CompactStorage: matchedInodeFDs=%d procDeletedFDs=%d", matchedInodeFDs, procDeletedFDs)
	}
	for k, want := range expected {
		got, err := db.Get([]byte(k))
		if err != nil {
			t.Fatalf("Get(%q): %v", k, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("Get(%q) mismatch", k)
		}
	}
}

func TestOpenBackendWithCachedLeafLog_DeferredSnapshotReleaseAsyncClosesDeletedFDs(t *testing.T) {
	dir := t.TempDir()
	opts := OptionsFor(ProfileNoWALFast, dir)
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.BackgroundIndexVacuumInterval = -1
	opts.MaxWALBytes = -1
	opts.JournalLanes = 2
	opts.IndexOuterLeavesInValueLog = true
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true
	opts.ValueLog.Compression = ValueLogCompressionOff
	opts.ValueLog.Generational.Policy = ValueLogGenerationHotWarmCold
	opts.ValueLog.Generational.HotSegmentTargetBytes = 8 << 10

	backend, cleanup, err := OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		t.Fatalf("OpenBackendWithCachedLeafLog: %v", err)
	}
	defer func() {
		if err := cleanup(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()

	for round := 0; round < 6; round++ {
		vals := make([][]byte, 64)
		for i := range vals {
			buf := make([]byte, 512)
			for j := range buf {
				buf[j] = byte((round*37 + i*19 + j*11) & 0xff)
			}
			vals[i] = buf
		}
		ptrs, err := backend.AppendValueLogValues(vals)
		if err != nil {
			t.Fatalf("round %d AppendValueLogValues: %v", round, err)
		}
		b := backend.NewBatch()
		pb := b.(interface {
			SetPointer([]byte, page.ValuePtr) error
		})
		for i := range vals {
			if err := pb.SetPointer([]byte(fmt.Sprintf("ds-%03d", i)), ptrs[i]); err != nil {
				_ = b.Close()
				t.Fatalf("SetPointer: %v", err)
			}
		}
		if err := b.WriteSync(); err != nil {
			_ = b.Close()
			t.Fatalf("WriteSync: %v", err)
		}
		if err := b.Close(); err != nil {
			t.Fatalf("Batch.Close: %v", err)
		}
	}
	for i := 0; i < 2; i++ {
		b := backend.NewBatch()
		if err := b.Set([]byte(fmt.Sprintf("ds-bump-%d", i)), nil); err != nil {
			_ = b.Close()
			t.Fatalf("Set bump: %v", err)
		}
		if err := b.WriteSync(); err != nil {
			_ = b.Close()
			t.Fatalf("WriteSync bump: %v", err)
		}
		if err := b.Close(); err != nil {
			t.Fatalf("Batch.Close bump: %v", err)
		}
	}

	valueLogDir := backenddb.ValueLogDirPath(dir)
	type trackedSeg struct {
		path     string
		identity rootpublication.StableIdentity
		inode    segmentInode
		hasInode bool
	}
	var preSegments []trackedSeg
	entries, err := os.ReadDir(valueLogDir)
	if err != nil {
		t.Fatalf("ReadDir(value_vlog): %v", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".log" {
			continue
		}
		p := filepath.Join(valueLogDir, entry.Name())
		f, err := os.Open(p)
		if err != nil {
			t.Fatalf("Open(%s): %v", p, err)
		}
		id, idErr := rootpublication.StableIdentityFromFile(f)
		_ = f.Close()
		if idErr != nil {
			t.Fatalf("StableIdentityFromFile(%s): %v", p, idErr)
		}
		ino, hasIno := statSegmentInode(p)
		preSegments = append(preSegments, trackedSeg{path: p, identity: id, inode: ino, hasInode: hasIno})
	}

	// Pin all segments across ValueLogGC so deletion happens strictly AFTER
	// ValueLogGC returns, when the snapshot closes, with no extra GC calls.
	snap := backend.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	gcStats, err := backend.ValueLogGC(context.Background(), backenddb.ValueLogGCOptions{})
	if err != nil {
		_ = snap.Close()
		t.Fatalf("ValueLogGC: %v", err)
	}
	if gcStats.SegmentsPending == 0 {
		_ = snap.Close()
		t.Fatalf("expected SegmentsPending > 0 while snapshot is held, got %+v", gcStats)
	}

	// Record the exact pending zombie segments that are still linked on disk
	// while snap pins them (present on disk and absent from backend's current
	// non-zombie ValueLogSet). Every single one must disappear and drain both
	// registry observers and open FDs after snap.Close() without another GC call.
	livePaths := make(map[string]struct{})
	if st := backend.State(); st != nil && st.ValueLogSet != nil {
		for _, f := range st.ValueLogSet.Files {
			if f != nil && f.Path != "" {
				livePaths[filepath.Clean(f.Path)] = struct{}{}
			}
		}
	}
	var pendingSegments []trackedSeg
	for _, seg := range preSegments {
		if _, statErr := os.Stat(seg.path); statErr != nil {
			continue
		}
		if _, isLive := livePaths[filepath.Clean(seg.path)]; !isLive {
			pendingSegments = append(pendingSegments, seg)
		}
	}
	if len(pendingSegments) != gcStats.SegmentsPending {
		_ = snap.Close()
		t.Fatalf("identified %d linked pending segments, want gcStats.SegmentsPending=%d", len(pendingSegments), gcStats.SegmentsPending)
	}

	if err := snap.Close(); err != nil {
		t.Fatalf("snap.Close: %v", err)
	}

	registry := backend.ValueLogIdentityPinRegistry()
	deadline := time.Now().Add(5 * time.Second)
	for {
		deletedInodes := make(map[segmentInode]string, len(pendingSegments))
		stillLinked := 0
		var staleObservers uint64
		for _, seg := range pendingSegments {
			if _, err := os.Stat(seg.path); !os.IsNotExist(err) {
				stillLinked++
			}
			if seg.hasInode {
				deletedInodes[seg.inode] = seg.path
			}
			staleObservers += registry.ObserverCount(seg.identity)
		}
		matchedFDs, procDeletedFDs, fdCheckAvailable := countOpenDeletedSegmentFDs(t, valueLogDir, deletedInodes)
		if !fdCheckAvailable && (runtime.GOOS == "linux" || runtime.GOOS == "darwin") {
			t.Fatalf("FD scan unavailable on %s", runtime.GOOS)
		}
		if stillLinked == 0 && staleObservers == 0 && (!fdCheckAvailable || (matchedFDs == 0 && procDeletedFDs == 0)) {
			if !fdCheckAvailable {
				t.Logf("FD scan unavailable on %s; verified all %d pending segments unlinked with 0 registry observers", runtime.GOOS, len(pendingSegments))
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("async retired reconciler did not drain all %d pending segments after deferred snapshot release: stillLinked=%d staleObservers=%d matchedFDs=%d procDeletedFDs=%d fdCheckAvailable=%t",
				len(pendingSegments), stillLinked, staleObservers, matchedFDs, procDeletedFDs, fdCheckAvailable)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestOpen_ReopenLane254PrunedByCompactStorage(t *testing.T) {
	dir := t.TempDir()
	opts := OptionsFor(ProfileNoWALFast, dir)
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.BackgroundIndexVacuumInterval = -1
	opts.MaxWALBytes = -1
	opts.JournalLanes = 4
	opts.IndexOuterLeavesInValueLog = false
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true
	opts.ValueLog.Compression = ValueLogCompressionOff
	opts.ValueLog.Generational.Policy = ValueLogGenerationHotWarmCold

	// Initialize DB and close so maindb/value_vlog exists.
	initDB, err := Open(opts)
	if err != nil {
		t.Fatalf("initial Open: %v", err)
	}
	if err := initDB.SetSync([]byte("live-key"), bytes.Repeat([]byte("l"), 512)); err != nil {
		_ = initDB.Close()
		t.Fatalf("SetSync: %v", err)
	}
	if err := initDB.Checkpoint(); err != nil {
		_ = initDB.Close()
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := initDB.Close(); err != nil {
		t.Fatalf("initial Close: %v", err)
	}

	// Plant a dead lane-254 rewrite segment on disk before reopening.
	valueLogDir := backenddb.ValueLogDirPath(dir)
	fileID, err := valuelog.EncodeFileID(254, 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	lane254Path := valuelog.SegmentPath(valueLogDir, fileID)
	w, err := valuelog.NewWriter(lane254Path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if _, err := w.Append(0, nil, 9999, bytes.Repeat([]byte("stale-rewrite"), 64)); err != nil {
		_ = w.Close()
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	info, err := os.Stat(lane254Path)
	if err != nil {
		t.Fatalf("Stat(%s): %v", lane254Path, err)
	}
	lane254Size := info.Size()

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen Open: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("reopen Close: %v", err)
		}
	}()

	st := db.Stats()
	if hotLanes, err := strconv.Atoi(st["treedb.cache.journal_lanes.hot"]); err != nil || hotLanes > opts.JournalLanes {
		t.Fatalf("reopen with lane-254 exploded hot lanes: %q (err=%v)", st["treedb.cache.journal_lanes.hot"], err)
	}
	if closedBytes, err := strconv.ParseInt(st["treedb.cache.vlog_retained_prune.closed_bytes"], 10, 64); err != nil || closedBytes < lane254Size {
		t.Fatalf("reopened lane-254 missing from retained closed_bytes: got %q (err=%v), want >= %d",
			st["treedb.cache.vlog_retained_prune.closed_bytes"], err, lane254Size)
	}

	if _, err := db.CompactStorage(context.Background(), CompactStorageOptions{
		Mode: CompactStorageFull,
	}); err != nil {
		t.Fatalf("CompactStorage: %v", err)
	}
	if _, err := os.Stat(lane254Path); !os.IsNotExist(err) {
		t.Fatalf("dead reopened lane-254 segment %s was stranded after CompactStorage (stat err=%v)", lane254Path, err)
	}
	got, err := db.Get([]byte("live-key"))
	if err != nil || !bytes.Equal(got, bytes.Repeat([]byte("l"), 512)) {
		t.Fatalf("live-key readback failed: err=%v len=%d", err, len(got))
	}
}
