package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/page"
)

func openLeafGenerationGCTestDB(t *testing.T) (*DB, *rewriteWriter) {
	t.Helper()
	dir := t.TempDir()
	db, err := Open(Options{
		Dir:                        dir,
		Durability:                 DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	leafLog := newRewriteWriter(ValueLogDirPath(dir), 0, 0, 64<<20)
	leafLog.ConfigureLeafLog(LeafLogDirPath(dir), rewriteLeafLogLaneID, 0)
	db.SetLeafPageLog(leafLog)
	t.Cleanup(func() { closeNoErr(t, leafLog) })
	t.Cleanup(func() { closeNoErr(t, db) })
	return db, leafLog
}

func writeLeafGenerationKeys(t *testing.T, db *DB, prefix string, count int, fill byte) {
	t.Helper()
	writeLeafGenerationKeyRange(t, db, prefix, 0, count, fill)
}

func writeLeafGenerationKeyRange(t *testing.T, db *DB, prefix string, start, count int, fill byte) {
	t.Helper()
	raw := db.NewBatch()
	b, ok := raw.(*Batch)
	if !ok {
		closeNoErr(t, raw)
		t.Fatalf("NewBatch type=%T, want *Batch", raw)
	}
	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("%s-%04d", prefix, start+i))
		value := bytes.Repeat([]byte{fill}, 32)
		if err := b.Set(key, value); err != nil {
			closeNoErr(t, b)
			t.Fatalf("Set(%q): %v", key, err)
		}
	}
	if err := b.WriteSync(); err != nil {
		closeNoErr(t, b)
		t.Fatalf("WriteSync: %v", err)
	}
	closeNoErr(t, b)
}

func currentLeafSegmentOrFatal(t *testing.T, leafLog *rewriteWriter) (string, uint32) {
	t.Helper()
	path, fileID, ok := leafLog.CurrentValueLogSegment()
	if !ok || path == "" || fileID == 0 {
		t.Fatalf("CurrentValueLogSegment ok=%v path=%q fileID=%d", ok, path, fileID)
	}
	return path, fileID
}

func openLeafLogLaneGCTestDB(t *testing.T, maxSegmentBytes int64) (*DB, LeafPageLogCloser, Options) {
	t.Helper()
	dir := t.TempDir()
	opts := Options{
		Dir:                        dir,
		ChunkSize:                  64 * 1024,
		Durability:                 DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		FlushAdmissionPolicy:       FlushAdmissionPolicyExplicit,
		FlushApplyConcurrency:      4,
		FlushApplyMinEntries:       1,
		FlushApplyMinSpans:         1,
		FlushApplyMinBytes:         1,
		FlushApplySpanNative:       true,
		ValueLog: ValueLogOptions{
			Compression: ValueLogCompressionOff,
		},
	}
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	leafLog, err := NewStandaloneLeafPageLog(dir, StandaloneLeafPageLogOptions{
		MaxSegmentBytes: maxSegmentBytes,
		Compression:     ValueLogCompressionOff,
	})
	if err != nil {
		_ = db.Close()
		t.Fatalf("NewStandaloneLeafPageLog: %v", err)
	}
	db.SetLeafPageLog(leafLog)
	return db, leafLog, opts
}

func closeLeafLogLaneGCTestDB(t *testing.T, db *DB, leafLog LeafPageLogCloser) {
	t.Helper()
	if db != nil {
		if err := db.Close(); err != nil {
			t.Fatalf("Close DB: %v", err)
		}
	}
	if leafLog != nil {
		if err := leafLog.Close(); err != nil {
			t.Fatalf("Close leaf log: %v", err)
		}
	}
}

func requireLeafLogCurrentSegments(t *testing.T, db *DB, min int) []LeafPageLogSegment {
	t.Helper()
	segments, err := leafPageLogCurrentSegments(db.leafPageLog)
	if err != nil {
		t.Fatalf("leafPageLogCurrentSegments: %v", err)
	}
	if len(segments) < min {
		t.Fatalf("current leaf-log segments=%d want >=%d: %+v", len(segments), min, segments)
	}
	return segments
}

func leafGenerationManifestRawFileIDSet(manifest *leafGenerationManifest) map[uint32]struct{} {
	out := make(map[uint32]struct{})
	if manifest == nil {
		return out
	}
	for _, gen := range manifest.Generations {
		for _, rawFileID := range gen.FileIDs {
			if rawFileID != 0 {
				out[rawFileID] = struct{}{}
			}
		}
	}
	return out
}

func currentLeafLogRawFileIDSetForTest(t *testing.T, db *DB) map[uint32]struct{} {
	t.Helper()
	currentRaw, err := db.currentLeafPageLogRawFileIDSet()
	if err != nil {
		t.Fatalf("currentLeafPageLogRawFileIDSet: %v", err)
	}
	if currentRaw == nil {
		return map[uint32]struct{}{}
	}
	return currentRaw
}

func requireLeafGenerationGCPublisherDrained(t *testing.T, db *DB) {
	t.Helper()
	state, ok := db.StateToken()
	if !ok {
		t.Fatal("StateToken unavailable after checkpoint")
	}
	if db.rootPublication == nil || db.rootPublication.coordinator == nil {
		t.Fatal("missing root-publication coordinator after checkpoint")
	}
	publication := db.rootPublication.coordinator.Stats()
	if publication.PendingCommits != 0 {
		t.Fatalf("checkpoint left %d queued root publications", publication.PendingCommits)
	}
	if publication.DurableCommitSeq < state.CommitSeq {
		t.Fatalf("checkpoint durable root sequence=%d want >= visible sequence %d", publication.DurableCommitSeq, state.CommitSeq)
	}
}

func leafGenerationRawFileIDsWithout(base, exclude map[uint32]struct{}) []uint32 {
	out := make([]uint32, 0, len(base))
	for rawFileID := range base {
		if _, skip := exclude[rawFileID]; skip {
			continue
		}
		out = append(out, rawFileID)
	}
	return out
}

func leafGenerationLiveGenerationIDsForTest(t *testing.T, db *DB) map[uint64]struct{} {
	t.Helper()
	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer func() {
		if err := snap.Close(); err != nil {
			t.Fatalf("Close snapshot: %v", err)
		}
	}()
	if len(snap.leafGenerationIDs) > 0 {
		snap.releaseLeafGenerationPins()
	}
	live, err := collectLiveLeafGenerationIDs(context.Background(), snap, nil, nil)
	if err != nil {
		t.Fatalf("collectLiveLeafGenerationIDs: %v", err)
	}
	return live
}

func leafGenerationReclaimableRawFileIDsForTest(t *testing.T, db *DB, candidates []uint32) []uint32 {
	t.Helper()
	manifest := loadLeafGenerationManifestOrFatal(t, db.dir)
	liveGenerations := leafGenerationLiveGenerationIDsForTest(t, db)
	currentRawFileIDs := currentLeafLogRawFileIDSetForTest(t, db)
	out := make([]uint32, 0, len(candidates))
	for _, rawFileID := range candidates {
		if _, current := currentRawFileIDs[rawFileID]; current {
			continue
		}
		gen := findLeafGenerationByFileID(t, manifest, rawFileID)
		if gen.State == leafGenerationStateWritable || gen.State == leafGenerationStateDeleted {
			continue
		}
		if _, live := liveGenerations[gen.GenerationID]; live {
			continue
		}
		out = append(out, rawFileID)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func findLeafGenerationByFileID(t *testing.T, manifest *leafGenerationManifest, fileID uint32) leafGenerationRecord {
	t.Helper()
	if manifest == nil {
		t.Fatal("manifest=nil")
	}
	for _, gen := range manifest.Generations {
		for _, id := range gen.FileIDs {
			if id == fileID {
				return gen
			}
		}
	}
	t.Fatalf("fileID %d not found in manifest %+v", fileID, manifest.Generations)
	return leafGenerationRecord{}
}

func loadLeafGenerationManifestOrFatal(t *testing.T, dir string) *leafGenerationManifest {
	t.Helper()
	manifest, ok, err := loadLeafGenerationManifest(LeafLogDirPath(dir))
	if err != nil {
		t.Fatalf("loadLeafGenerationManifest: %v", err)
	}
	if !ok {
		t.Fatal("expected manifest to exist")
	}
	return manifest
}

type leafGenerationGCTestResult struct {
	stats LeafGenerationGCStats
	err   error
}

func startLeafGenerationGCPausedAtLiveScan(t *testing.T, db *DB, opts LeafGenerationGCOptions) (func(), <-chan leafGenerationGCTestResult) {
	t.Helper()
	entered := make(chan struct{})
	release := make(chan struct{})
	var enterOnce sync.Once
	var releaseOnce sync.Once
	unregister := registerLeafGenerationLiveScanHook(func() {
		enterOnce.Do(func() { close(entered) })
		<-release
	})
	done := make(chan leafGenerationGCTestResult, 1)
	go func() {
		stats, err := db.LeafGenerationGC(context.Background(), opts)
		done <- leafGenerationGCTestResult{stats: stats, err: err}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		unregister()
		t.Fatal("LeafGenerationGC did not enter live scan")
	}
	releaseScan := func() {
		releaseOnce.Do(func() { close(release) })
	}
	t.Cleanup(func() {
		releaseScan()
		unregister()
	})
	return releaseScan, done
}

func requireLeafGenerationForegroundSetCompletes(t *testing.T, db *DB, key string) {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		done <- db.SetSync([]byte(key), []byte("ok"))
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("SetSync while leaf-generation GC scan paused: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("SetSync blocked while leaf-generation GC scan was paused")
	}
}

func queueLeafGenerationCheckpointBehindReadLock(t *testing.T, db *DB) (func(), <-chan error) {
	t.Helper()
	db.writeMu.RLock()
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(db.writeMu.RUnlock)
	}
	t.Cleanup(release)

	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		done <- db.Checkpoint()
	}()
	<-started

	deadline := time.Now().Add(5 * time.Second)
	for {
		if !db.writeMu.TryRLock() {
			break
		}
		db.writeMu.RUnlock()
		if time.Now().After(deadline) {
			t.Fatal("Checkpoint did not queue for writeMu")
		}
		time.Sleep(100 * time.Microsecond)
	}
	return release, done
}

func requireLeafGenerationOperationCompletes(t *testing.T, name string, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("%s did not complete", name)
	}
}

func requireLeafGenerationGCResult(t *testing.T, done <-chan leafGenerationGCTestResult) LeafGenerationGCStats {
	t.Helper()
	select {
	case result := <-done:
		if result.err != nil {
			t.Fatalf("LeafGenerationGC: %v", result.err)
		}
		return result.stats
	case <-time.After(5 * time.Second):
		t.Fatal("LeafGenerationGC did not finish after releasing live scan")
	}
	return LeafGenerationGCStats{}
}

func requireLeafGenerationGCResultAllowRecoverableStale(t *testing.T, done <-chan leafGenerationGCTestResult) LeafGenerationGCStats {
	t.Helper()
	select {
	case result := <-done:
		if result.err != nil && !errors.Is(result.err, ErrRecoverableRootSetStale) {
			t.Fatalf("LeafGenerationGC: %v", result.err)
		}
		return result.stats
	case <-time.After(5 * time.Second):
		t.Fatal("LeafGenerationGC did not finish after releasing live scan")
	}
	return LeafGenerationGCStats{}
}

func TestLeafGenerationView_SkipsRetiringAndDeletedGenerations(t *testing.T) {
	manifest := &leafGenerationManifest{
		Version:             leafGenerationManifestVersion,
		CurrentGenerationID: 4,
		NextGenerationID:    5,
		Generations: []leafGenerationRecord{
			{GenerationID: 1, State: leafGenerationStateDeleted, FileIDs: []uint32{101}},
			{GenerationID: 2, State: leafGenerationStateRetiring, FileIDs: []uint32{202}},
			{GenerationID: 3, State: leafGenerationStateSealed, FileIDs: []uint32{303}},
			{GenerationID: 4, State: leafGenerationStateWritable, FileIDs: []uint32{404}},
		},
	}
	view := newLeafGenerationView(manifest)
	if view == nil {
		t.Fatal("expected leaf generation view")
	}
	if got, want := len(view.GenerationOrder), 2; got != want {
		t.Fatalf("len(GenerationOrder)=%d, want %d", got, want)
	}
	if _, ok := view.Generations[1]; ok {
		t.Fatalf("deleted generation should be absent from view")
	}
	if _, ok := view.FileToGeneration[101]; ok {
		t.Fatalf("deleted file should be absent from file map")
	}
	if _, ok := view.Generations[2]; ok {
		t.Fatalf("retiring generation should be absent from view")
	}
	if _, ok := view.FileToGeneration[202]; ok {
		t.Fatalf("retiring file should be absent from file map")
	}
	if got, want := view.FileToGeneration[303], uint64(3); got != want {
		t.Fatalf("FileToGeneration[303]=%d, want %d", got, want)
	}
}

func TestLeafGenerationGC_LiveScanDoesNotBlockForegroundWrite(t *testing.T) {
	db, leafLog := openLeafGenerationGCTestDB(t)

	writeLeafGenerationKeys(t, db, "scan-old", 64, 'a')
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeys(t, db, "scan-new", 64, 'b')

	releaseScan, done := startLeafGenerationGCPausedAtLiveScan(t, db, LeafGenerationGCOptions{})
	requireLeafGenerationForegroundSetCompletes(t, db, "scan-concurrent")
	releaseScan()
	_ = requireLeafGenerationGCResultAllowRecoverableStale(t, done)

	got, err := db.Get([]byte("scan-concurrent"))
	if err != nil {
		t.Fatalf("Get concurrent write: %v", err)
	}
	if !bytes.Equal(got, []byte("ok")) {
		t.Fatalf("concurrent write value=%q want ok", got)
	}
}

func TestLeafGenerationGC_QueuedCheckpointDoesNotGateLaterWrite(t *testing.T) {
	db, leafLog := openLeafGenerationGCTestDB(t)

	writeLeafGenerationKeys(t, db, "checkpoint-scan-old", 64, 'a')
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeys(t, db, "checkpoint-scan-new", 64, 'b')

	releaseScan, gcDone := startLeafGenerationGCPausedAtLiveScan(t, db, LeafGenerationGCOptions{})
	releaseCheckpointBlocker, checkpointDone := queueLeafGenerationCheckpointBehindReadLock(t, db)

	setStarted := make(chan struct{})
	setDone := make(chan error, 1)
	go func() {
		close(setStarted)
		setDone <- db.SetSync([]byte("checkpoint-scan-concurrent"), []byte("ok"))
	}()
	<-setStarted
	releaseCheckpointBlocker()

	requireLeafGenerationOperationCompletes(t, "Checkpoint while live scan paused", checkpointDone)
	requireLeafGenerationOperationCompletes(t, "SetSync queued after Checkpoint", setDone)
	releaseScan()
	_ = requireLeafGenerationGCResultAllowRecoverableStale(t, gcDone)

	got, err := db.Get([]byte("checkpoint-scan-concurrent"))
	if err != nil {
		t.Fatalf("Get concurrent write: %v", err)
	}
	if !bytes.Equal(got, []byte("ok")) {
		t.Fatalf("concurrent write value=%q want ok", got)
	}
}

func TestLeafGenerationGC_LifetimeGateLockOrderStress(t *testing.T) {
	db, leafLog := openLeafGenerationGCTestDB(t)

	writeLeafGenerationKeys(t, db, "lifetime-stress-old", 64, 'a')
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeys(t, db, "lifetime-stress-new", 64, 'b')

	for round := 0; round < 24; round++ {
		start := make(chan struct{})
		errs := make(chan error, 3)
		go func() {
			<-start
			_, err := db.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{DryRun: true})
			if errors.Is(err, ErrLeafGenerationGCStaleScan) {
				err = nil
			}
			errs <- err
		}()
		go func() {
			<-start
			errs <- db.Checkpoint()
		}()
		go func(round int) {
			<-start
			key := []byte(fmt.Sprintf("lifetime-stress-write-%02d", round))
			errs <- db.SetSync(key, []byte("ok"))
		}(round)
		close(start)

		for operation := 0; operation < 3; operation++ {
			select {
			case err := <-errs:
				if err != nil {
					t.Fatalf("round %d operation %d: %v", round, operation, err)
				}
			case <-time.After(10 * time.Second):
				t.Fatalf("round %d operation %d timed out", round, operation)
			}
		}
	}
}

func TestLeafGenerationGC_RevalidationFailureSkipsStalePublish(t *testing.T) {
	db, leafLog := openLeafGenerationGCTestDB(t)

	writeLeafGenerationKeys(t, db, "revalidate", 64, 'a')
	path1, fileID1 := currentLeafSegmentOrFatal(t, leafLog)
	rawFileID1 := page.ValueLogSegmentID(fileID1)
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeys(t, db, "revalidate", 64, 'b')
	before := db.State()

	releaseScan, done := startLeafGenerationGCPausedAtLiveScan(t, db, LeafGenerationGCOptions{})
	requireLeafGenerationForegroundSetCompletes(t, db, "revalidate-concurrent")
	afterWrite := db.State()
	if before == nil || afterWrite == nil || afterWrite.CommitSeq == before.CommitSeq {
		t.Fatalf("concurrent write did not advance CommitSeq: before=%+v after=%+v", before, afterWrite)
	}
	releaseScan()
	stats := requireLeafGenerationGCResultAllowRecoverableStale(t, done)
	if stats.GenerationsDeleted != 0 || stats.FilesDeleted != 0 {
		t.Fatalf("stale scan applied deletion: stats=%+v", stats)
	}
	if _, err := os.Stat(path1); err != nil {
		t.Fatalf("stale scan removed candidate leaf segment: %v", err)
	}
	manifestAfter := loadLeafGenerationManifestOrFatal(t, db.dir)
	gen1 := findLeafGenerationByFileID(t, manifestAfter, rawFileID1)
	if gen1.State == leafGenerationStateDeleted {
		t.Fatalf("stale scan marked generation deleted: %+v", gen1)
	}
}

func TestLeafGenerationGC_DryRunLiveScanDoesNotBlockForegroundWrite(t *testing.T) {
	db, leafLog := openLeafGenerationGCTestDB(t)

	writeLeafGenerationKeys(t, db, "dry-scan-old", 64, 'a')
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeys(t, db, "dry-scan-new", 64, 'b')

	releaseScan, done := startLeafGenerationGCPausedAtLiveScan(t, db, LeafGenerationGCOptions{DryRun: true})
	requireLeafGenerationForegroundSetCompletes(t, db, "dry-scan-concurrent")
	releaseScan()
	_ = requireLeafGenerationGCResult(t, done)

	got, err := db.Get([]byte("dry-scan-concurrent"))
	if err != nil {
		t.Fatalf("Get concurrent dry-run write: %v", err)
	}
	if !bytes.Equal(got, []byte("ok")) {
		t.Fatalf("concurrent dry-run write value=%q want ok", got)
	}
}

func TestLeafGenerationGC_DryRunRetriesInvalidatedScan(t *testing.T) {
	db, leafLog := openLeafGenerationGCTestDB(t)

	writeLeafGenerationKeys(t, db, "dry-retry-old", 64, 'a')
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeys(t, db, "dry-retry-new", 64, 'b')

	entered := make(chan struct{})
	release := make(chan struct{})
	var mu sync.Mutex
	scans := 0
	unregister := registerLeafGenerationLiveScanHook(func() {
		mu.Lock()
		scans++
		scan := scans
		mu.Unlock()
		if scan == 1 {
			close(entered)
			<-release
		}
	})
	t.Cleanup(unregister)

	done := make(chan leafGenerationGCTestResult, 1)
	go func() {
		stats, err := db.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{DryRun: true})
		done <- leafGenerationGCTestResult{stats: stats, err: err}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("dry-run GC did not enter first live scan")
	}
	if err := db.SetSync([]byte("dry-retry-concurrent"), []byte("ok")); err != nil {
		t.Fatalf("SetSync invalidating first dry-run scan: %v", err)
	}
	close(release)

	stats := requireLeafGenerationGCResult(t, done)
	mu.Lock()
	gotScans := scans
	mu.Unlock()
	if gotScans != 2 {
		t.Fatalf("live scans=%d, want 2 after one invalidation", gotScans)
	}
	if stats.GenerationsEligible == 0 || stats.BytesEligible == 0 {
		t.Fatalf("retried dry-run reported zero actionable debt: %+v", stats)
	}
}

func TestLeafGenerationGC_DryRunRetryExhaustionReturnsStale(t *testing.T) {
	db, leafLog := openLeafGenerationGCTestDB(t)

	writeLeafGenerationKeys(t, db, "dry-stale-old", 64, 'a')
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeys(t, db, "dry-stale-new", 64, 'b')

	entered := make(chan int, 2)
	release := make(chan struct{})
	var mu sync.Mutex
	scans := 0
	unregister := registerLeafGenerationLiveScanHook(func() {
		mu.Lock()
		scans++
		scan := scans
		mu.Unlock()
		entered <- scan
		<-release
	})
	t.Cleanup(unregister)

	done := make(chan leafGenerationGCTestResult, 1)
	go func() {
		stats, err := db.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{DryRun: true})
		done <- leafGenerationGCTestResult{stats: stats, err: err}
	}()
	for attempt := 1; attempt <= 2; attempt++ {
		select {
		case scan := <-entered:
			if scan != attempt {
				t.Fatalf("live scan=%d, want attempt %d", scan, attempt)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("dry-run GC did not enter live scan attempt %d", attempt)
		}
		key := fmt.Sprintf("dry-stale-concurrent-%d", attempt)
		if err := db.SetSync([]byte(key), []byte("ok")); err != nil {
			t.Fatalf("SetSync invalidating dry-run attempt %d: %v", attempt, err)
		}
		release <- struct{}{}
	}
	select {
	case result := <-done:
		if !errors.Is(result.err, ErrLeafGenerationGCStaleScan) {
			t.Fatalf("LeafGenerationGC error=%v, want ErrLeafGenerationGCStaleScan", result.err)
		}
		if result.stats != (LeafGenerationGCStats{}) {
			t.Fatalf("stale dry-run stats=%+v, want zero with explicit error", result.stats)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("dry-run GC did not return after bounded stale retries")
	}
}

func TestLeafGenerationGC_CloseWaitsForUnlockedScan(t *testing.T) {
	db, leafLog := openLeafGenerationGCTestDB(t)

	writeLeafGenerationKeys(t, db, "close-scan-old", 64, 'a')
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeys(t, db, "close-scan-new", 64, 'b')

	releaseScan, gcDone := startLeafGenerationGCPausedAtLiveScan(t, db, LeafGenerationGCOptions{})
	closeDone := make(chan error, 1)
	go func() {
		closeDone <- db.Close()
	}()
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned before live scan released: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	releaseScan()
	_ = requireLeafGenerationGCResult(t, gcDone)
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close after live scan: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not finish after live scan released")
	}
}

func TestLeafGenerationGC_DeletesFullyDeadGeneration(t *testing.T) {
	db, leafLog := openLeafGenerationGCTestDB(t)

	writeLeafGenerationKeys(t, db, "k", 64, 'a')
	path1, fileID1 := currentLeafSegmentOrFatal(t, leafLog)
	rawFileID1 := page.ValueLogSegmentID(fileID1)
	indexPath1 := leafGenerationRecordLengthIndexPath(db.dir, rawFileID1)
	if _, err := os.Stat(path1); err != nil {
		t.Fatalf("stat first leaf segment: %v", err)
	}

	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeys(t, db, "k", 64, 'b')
	path2, fileID2 := currentLeafSegmentOrFatal(t, leafLog)
	rawFileID2 := page.ValueLogSegmentID(fileID2)
	if _, err := os.Stat(path2); err != nil {
		t.Fatalf("stat second leaf segment: %v", err)
	}

	manifestBefore := loadLeafGenerationManifestOrFatal(t, db.dir)
	if got, want := len(manifestBefore.Generations), 2; got != want {
		t.Fatalf("len(manifestBefore.Generations)=%d, want %d", got, want)
	}
	gen1 := findLeafGenerationByFileID(t, manifestBefore, rawFileID1)
	if got, want := gen1.State, leafGenerationStateSealed; got != want {
		t.Fatalf("generation1 state=%q, want %q", got, want)
	}

	advancePastRetainedDurableSlotForTest(t, db)
	stats1, err := db.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{})
	if err != nil {
		t.Fatalf("LeafGenerationGC first: %v", err)
	}
	if got := stats1.GenerationsEligible; got < 1 {
		t.Fatalf("expected at least one eligible generation, got %d", got)
	}
	if got, want := stats1.GenerationsDeleted, 1; got != want {
		t.Fatalf("GenerationsDeleted=%d, want %d (stats=%+v)", got, want, stats1)
	}
	if got, want := stats1.FilesDeleted, 1; got != want {
		t.Fatalf("FilesDeleted=%d, want %d (stats=%+v)", got, want, stats1)
	}
	if got := stats1.BytesEligible; got <= 0 {
		t.Fatalf("BytesEligible=%d, want > 0 (stats=%+v)", got, stats1)
	}
	if got := stats1.BytesDeleted; got <= 0 {
		t.Fatalf("BytesDeleted=%d, want > 0 (stats=%+v)", got, stats1)
	}
	if err := waitForPathRemoval(path1, 5*time.Second); err != nil {
		t.Fatalf("waitForPathRemoval(%s): %v", path1, err)
	}
	if err := waitForPathRemoval(indexPath1, 5*time.Second); err != nil {
		t.Fatalf("waitForPathRemoval(%s): %v", indexPath1, err)
	}
	if _, err := os.Stat(path1); !os.IsNotExist(err) {
		t.Fatalf("expected first leaf segment removed, stat err=%v", err)
	}
	if _, err := os.Stat(indexPath1); !os.IsNotExist(err) {
		t.Fatalf("expected first leaf segment record-length index removed, stat err=%v", err)
	}
	if _, err := os.Stat(path2); err != nil {
		t.Fatalf("expected second leaf segment to remain, stat err=%v", err)
	}

	manifestAfter := loadLeafGenerationManifestOrFatal(t, db.dir)
	if got, want := len(manifestAfter.Generations), 1; got != want {
		t.Fatalf("len(manifestAfter.Generations)=%d, want %d", got, want)
	}
	remaining := manifestAfter.Generations[0]
	if got, want := remaining.FileIDs[0], rawFileID2; got != want {
		t.Fatalf("remaining generation fileID=%d, want %d", got, want)
	}
}

func TestLeafGenerationGC_DryRunRetainsOlderRecoverableRootGeneration(t *testing.T) {
	db, leafLog := openLeafGenerationGCTestDB(t)

	writeLeafGenerationKeys(t, db, "recoverable", 64, 'a')
	path1, fileID1 := currentLeafSegmentOrFatal(t, leafLog)
	rawFileID1 := page.ValueLogSegmentID(fileID1)
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeys(t, db, "recoverable", 64, 'b')

	manifest := loadLeafGenerationManifestOrFatal(t, db.dir)
	gen1 := findLeafGenerationByFileID(t, manifest, rawFileID1)
	if got, want := gen1.State, leafGenerationStateSealed; got != want {
		t.Fatalf("older generation state=%q, want %q", got, want)
	}

	stats, err := db.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{DryRun: true})
	if err != nil {
		t.Fatalf("LeafGenerationGC dry-run: %v", err)
	}
	if got, want := stats.GenerationsRetiring, 1; got != want {
		t.Fatalf("GenerationsRetiring=%d, want %d for older recoverable root (stats=%+v)", got, want, stats)
	}
	if stats.GenerationsEligible != 0 || stats.BytesEligible != 0 {
		t.Fatalf("dry-run reported older recoverable generation eligible: %+v", stats)
	}
	if _, err := os.Stat(path1); err != nil {
		t.Fatalf("dry-run disturbed older recoverable generation: %v", err)
	}
}

func TestLeafPageLogLanes_CheckpointReopenPublishesEveryCurrentLane(t *testing.T) {
	db, leafLog, opts := openLeafLogLaneGCTestDB(t, 0)
	closed := false
	defer func() {
		if !closed {
			closeLeafLogLaneGCTestDB(t, db, leafLog)
		}
	}()

	putBatch(t, db, 0, 4096, "base")
	updates := db.NewBatch()
	expected := map[string][]byte{
		"key-000000": []byte("base-000000"),
	}
	for _, anchor := range []int{17, 1029, 2053, 3079} {
		for j := 0; j < 48; j++ {
			key := []byte(fmt.Sprintf("key-%06d-%03d", anchor, j))
			val := bytes.Repeat([]byte{byte(1 + (anchor+j)%251)}, 180)
			if err := updates.Set(key, val); err != nil {
				_ = updates.Close()
				t.Fatalf("Set update anchor=%d j=%d: %v", anchor, j, err)
			}
			if j == 7 || j == 47 {
				expected[string(key)] = append([]byte(nil), val...)
			}
		}
	}
	if err := updates.Write(); err != nil {
		_ = updates.Close()
		t.Fatalf("Write updates: %v", err)
	}
	if err := updates.Close(); err != nil {
		t.Fatalf("Close updates: %v", err)
	}
	if got := requireDBStatUint64(t, db, "treedb.flush_apply.span_native.used_ops_total"); got == 0 {
		t.Fatalf("span-native used ops = 0, want lane-routed leaf output")
	}
	currentSegments := requireLeafLogCurrentSegments(t, db, 2)
	currentLaneFileIDs := make(map[uint32]string, len(currentSegments))
	currentLaneRawFileIDs := make(map[uint32]string, len(currentSegments))
	for _, seg := range currentSegments {
		currentLaneFileIDs[seg.FileID] = seg.Path
		currentLaneRawFileIDs[page.ValueLogSegmentID(seg.FileID)] = seg.Path
	}

	manifest := loadLeafGenerationManifestOrFatal(t, opts.Dir)
	view := newLeafGenerationView(manifest)
	if view == nil {
		t.Fatal("leaf generation view missing")
	}
	for _, seg := range currentSegments {
		rawFileID := page.ValueLogSegmentID(seg.FileID)
		if _, ok := view.FileToGeneration[rawFileID]; !ok {
			t.Fatalf("current lane segment %d (%s) missing from leaf-generation manifest", seg.FileID, seg.Path)
		}
	}
	set := db.valueLogManager.CurrentSetNoRefresh()
	if set == nil {
		t.Fatal("missing value-log set")
	}
	for _, seg := range currentSegments {
		if _, ok := set.Files[seg.FileID]; !ok {
			_ = db.valueLogManager.Release(set)
			t.Fatalf("current lane segment %d (%s) missing from value-log set", seg.FileID, seg.Path)
		}
	}
	if err := db.valueLogManager.Release(set); err != nil {
		t.Fatalf("Release value-log set: %v", err)
	}

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close DB: %v", err)
	}
	if err := leafLog.Close(); err != nil {
		t.Fatalf("Close leaf log: %v", err)
	}
	closed = true

	reopened, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()

	reopenedManifest := loadLeafGenerationManifestOrFatal(t, opts.Dir)
	reopenedView := newLeafGenerationView(reopenedManifest)
	if reopenedView == nil {
		t.Fatal("reopened leaf generation view missing")
	}
	for rawFileID, path := range currentLaneRawFileIDs {
		if _, ok := reopenedView.FileToGeneration[rawFileID]; !ok {
			t.Fatalf("reopened manifest missing current lane raw file %d (%s)", rawFileID, path)
		}
	}
	reopenedSet := reopened.valueLogManager.CurrentSetNoRefresh()
	if reopenedSet == nil {
		t.Fatal("missing reopened value-log set")
	}
	for fileID, path := range currentLaneFileIDs {
		if _, ok := reopenedSet.Files[fileID]; !ok {
			_ = reopened.valueLogManager.Release(reopenedSet)
			t.Fatalf("reopened value-log set missing current lane segment %d (%s)", fileID, path)
		}
	}
	if err := reopened.valueLogManager.Release(reopenedSet); err != nil {
		t.Fatalf("Release reopened value-log set: %v", err)
	}

	for key, want := range expected {
		got, err := reopened.Get([]byte(key))
		if err != nil {
			t.Fatalf("reopen Get(%q): %v", key, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("reopen Get(%q)=%q want %q", key, got, want)
		}
	}
}

func TestLeafGenerationGC_RetainsCurrentUnreachableLeafLogLaneSegment(t *testing.T) {
	db, leafLog, _ := openLeafLogLaneGCTestDB(t, 0)
	defer closeLeafLogLaneGCTestDB(t, db, leafLog)

	putBatch(t, db, 0, 4096, "old")
	baseSegments := requireLeafLogCurrentSegments(t, db, 1)
	base := baseSegments[0]
	baseRawFileID := page.ValueLogSegmentID(base.FileID)

	putBatch(t, db, 0, 4096, "new")
	// putBatch deliberately uses Batch.Write so other flush-apply tests can
	// exercise asynchronous admission. This fixture edits the manifest directly,
	// however, so it must first cross the normal publication and durability
	// boundary. Otherwise a queued publisher can legitimately change the
	// RecoverableRootSet while LeafGenerationGC revalidates it.
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint fixture writes before manifest edit: %v", err)
	}
	requireLeafGenerationGCPublisherDrained(t, db)
	currentRaw := currentLeafLogRawFileIDSetForTest(t, db)
	if _, ok := currentRaw[baseRawFileID]; !ok {
		t.Fatalf("base segment %d is no longer a current leaf-log lane; current=%v", base.FileID, currentRaw)
	}
	manifest := loadLeafGenerationManifestOrFatal(t, db.dir)
	markedRetiring := false
	for i := range manifest.Generations {
		for _, rawFileID := range manifest.Generations[i].FileIDs {
			if rawFileID != baseRawFileID {
				continue
			}
			manifest.Generations[i].State = leafGenerationStateRetiring
			manifest.Generations[i].RetiredCommitSeq = 123
			markedRetiring = true
			break
		}
		if markedRetiring {
			break
		}
	}
	if !markedRetiring {
		t.Fatalf("base raw file id %d missing from manifest", baseRawFileID)
	}
	if err := saveLeafGenerationManifest(LeafLogDirPath(db.dir), manifest); err != nil {
		t.Fatalf("save retiring manifest: %v", err)
	}
	db.mu.Lock()
	db.leafGenerationManifest = manifest
	db.mu.Unlock()
	if err := db.publishLeafGenerationState(false); err != nil {
		t.Fatalf("publish retiring manifest: %v", err)
	}
	// This is a fixture invariant, not a retry: the direct manifest mutation
	// must leave a stable recoverable-root basis before this test asks the
	// destructive GC path to capture it.
	recoverableRoots, err := db.CaptureRecoverableRootSet(context.Background())
	if err != nil {
		t.Fatalf("capture recoverable root set after manifest edit: %v", err)
	}
	defer recoverableRoots.Release()
	if err := recoverableRoots.Revalidate(); err != nil {
		t.Fatalf("fixture recoverable root set changed after manifest edit: %v", err)
	}

	if _, err := db.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{}); err != nil {
		t.Fatalf("LeafGenerationGC: %v", err)
	}
	afterManifest := loadLeafGenerationManifestOrFatal(t, db.dir)
	if gen := findLeafGenerationByFileID(t, afterManifest, baseRawFileID); gen.State != leafGenerationStateSealed {
		t.Fatalf("current leaf-log generation state=%q want %q", gen.State, leafGenerationStateSealed)
	}
	if _, err := os.Stat(base.Path); err != nil {
		t.Fatalf("current but unreachable leaf-log segment was removed: %s err=%v", base.Path, err)
	}
	for _, seg := range requireLeafLogCurrentSegments(t, db, 2) {
		if _, err := os.Stat(seg.Path); err != nil {
			t.Fatalf("current leaf-log segment missing after GC: %s err=%v", seg.Path, err)
		}
	}
}

func TestLeafGenerationGC_RemovesUnreachableMultiLaneSegmentsAfterReopen(t *testing.T) {
	db, leafLog, opts := openLeafLogLaneGCTestDB(t, 32<<10)
	closed := false
	defer func() {
		if !closed {
			closeLeafLogLaneGCTestDB(t, db, leafLog)
		}
	}()

	putBatch(t, db, 0, 4096, "old")
	baseIDs := leafGenerationManifestRawFileIDSet(loadLeafGenerationManifestOrFatal(t, opts.Dir))
	putBatch(t, db, 0, 4096, "mid")
	midIDs := leafGenerationManifestRawFileIDSet(loadLeafGenerationManifestOrFatal(t, opts.Dir))
	midOnly := leafGenerationRawFileIDsWithout(midIDs, baseIDs)
	putBatch(t, db, 0, 4096, "final")

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close DB: %v", err)
	}
	if err := leafLog.Close(); err != nil {
		t.Fatalf("Close leaf log: %v", err)
	}
	closed = true

	reopened, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	reopenedLeafLog, err := NewStandaloneLeafPageLog(opts.Dir, StandaloneLeafPageLogOptions{
		MaxSegmentBytes: 32 << 10,
		Compression:     ValueLogCompressionOff,
	})
	if err != nil {
		_ = reopened.Close()
		t.Fatalf("reopen leaf log: %v", err)
	}
	reopened.SetLeafPageLog(reopenedLeafLog)
	defer func() {
		_ = reopened.Close()
		_ = reopenedLeafLog.Close()
	}()
	advancePastRetainedDurableSlotForTest(t, reopened)
	reclaimableMid := leafGenerationReclaimableRawFileIDsForTest(t, reopened, midOnly)
	if len(reclaimableMid) < 2 {
		t.Fatalf("reclaimable mid-generation lane segments=%d want >=2 (midOnly=%v)", len(reclaimableMid), midOnly)
	}
	reclaimableMidPaths := make([]string, 0, len(reclaimableMid))
	for _, rawFileID := range reclaimableMid {
		path := leafGenerationFallbackPath(opts.Dir, rawFileID)
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("stat reclaimable mid segment %d: %v", rawFileID, err)
		}
		reclaimableMidPaths = append(reclaimableMidPaths, path)
	}
	stats, err := reopened.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{})
	if err != nil {
		t.Fatalf("LeafGenerationGC: %v", err)
	}
	if stats.FilesDeleted == 0 {
		t.Fatalf("FilesDeleted=0 want reclaimed unreachable mid-generation lane segments (stats=%+v)", stats)
	}
	for _, path := range reclaimableMidPaths {
		if err := waitForPathRemoval(path, 5*time.Second); err != nil {
			t.Fatalf("waitForPathRemoval(%s): %v (stats=%+v)", path, err, stats)
		}
	}
	for _, idx := range []int{0, 1029, 3079} {
		key := []byte(fmt.Sprintf("key-%06d", idx))
		got, err := reopened.Get(key)
		if err != nil {
			t.Fatalf("reopened Get(%q): %v", key, err)
		}
		want := []byte(fmt.Sprintf("final-%06d", idx))
		if !bytes.Equal(got, want) {
			t.Fatalf("reopened Get(%q)=%q want %q", key, got, want)
		}
	}
}

func TestLeafGenerationGC_DoesNotZombieSharedActiveFileID(t *testing.T) {
	db, _ := openLeafGenerationGCTestDB(t)
	path, fileID := createLeafGenerationTestSegment(t, LeafLogDirPath(db.dir), rewriteLeafLogLaneID, 1)
	rawFileID := page.ValueLogSegmentID(fileID)
	if err := db.valueLogManager.Refresh(); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	manifest := &leafGenerationManifest{
		Version:             leafGenerationManifestVersion,
		CurrentGenerationID: 2,
		NextGenerationID:    3,
		Generations: []leafGenerationRecord{
			{GenerationID: 1, State: leafGenerationStateSealed, FileIDs: []uint32{rawFileID}, CreatedCommitSeq: 1, SealedCommitSeq: 1, PublishedCommitSeq: 1},
			{GenerationID: 2, State: leafGenerationStateWritable, FileIDs: []uint32{rawFileID}, CreatedCommitSeq: 2, PublishedCommitSeq: 2},
		},
	}
	if err := saveLeafGenerationManifest(LeafLogDirPath(db.dir), manifest); err != nil {
		t.Fatalf("save manifest: %v", err)
	}
	db.mu.Lock()
	db.leafGenerationManifest = manifest
	db.mu.Unlock()
	if err := db.publishLeafGenerationState(true); err != nil {
		t.Fatalf("publish leaf state: %v", err)
	}

	stats, err := db.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{})
	if err != nil {
		t.Fatalf("LeafGenerationGC: %v", err)
	}
	if got, want := stats.GenerationsEligible, 1; got != want {
		t.Fatalf("GenerationsEligible=%d want %d stats=%+v", got, want, stats)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("shared active segment was removed: %v", err)
	}
	manifestAfter := loadLeafGenerationManifestOrFatal(t, db.dir)
	gen := findLeafGenerationByFileID(t, manifestAfter, rawFileID)
	if gen.GenerationID != 1 || gen.State != leafGenerationStateDeleted {
		t.Fatalf("first shared generation not marked deleted: %+v manifest=%+v", gen, manifestAfter.Generations)
	}
}

func TestLeafGenerationGC_RetriesDeletedGenerationFileAfterReopen(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Dir:                        dir,
		Durability:                 DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
	}
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open initial: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close initial: %v", err)
	}

	path, fileID := createLeafGenerationTestSegment(t, LeafLogDirPath(dir), rewriteLeafLogLaneID, 1)
	rawFileID := page.ValueLogSegmentID(fileID)
	manifest := &leafGenerationManifest{
		Version:             leafGenerationManifestVersion,
		CurrentGenerationID: 2,
		NextGenerationID:    3,
		Generations: []leafGenerationRecord{
			{GenerationID: 1, State: leafGenerationStateDeleted, FileIDs: []uint32{rawFileID}, CreatedCommitSeq: 1, DeletedCommitSeq: 2, PublishedCommitSeq: 2},
			{GenerationID: 2, State: leafGenerationStateWritable, CreatedCommitSeq: 3, PublishedCommitSeq: 3},
		},
	}
	if err := saveLeafGenerationManifest(LeafLogDirPath(dir), manifest); err != nil {
		t.Fatalf("save manifest: %v", err)
	}

	db, err = Open(opts)
	if err != nil {
		t.Fatalf("Open with pending deleted leaf file: %v", err)
	}
	defer closeNoErr(t, db)
	current := db.leafGenerationManifest.Generations[db.leafGenerationManifest.currentGenerationIndex()]
	if len(current.FileIDs) != 0 {
		t.Fatalf("current FileIDs=%v, want empty; pending-deleted file should not be resurrected", current.FileIDs)
	}

	stats, err := db.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{})
	if err != nil {
		t.Fatalf("LeafGenerationGC: %v", err)
	}
	if got, want := stats.GenerationsDeleted, 1; got != want {
		t.Fatalf("GenerationsDeleted=%d, want %d (stats=%+v)", got, want, stats)
	}
	if got, want := stats.FilesDeleted, 1; got != want {
		t.Fatalf("FilesDeleted=%d, want %d (stats=%+v)", got, want, stats)
	}
	if err := waitForPathRemoval(path, 5*time.Second); err != nil {
		t.Fatalf("waitForPathRemoval(%s): %v", path, err)
	}
	manifestAfter := loadLeafGenerationManifestOrFatal(t, dir)
	if len(manifestAfter.Generations) != 1 {
		t.Fatalf("len(Gens)=%d, want only writable generation after pruning: %+v", len(manifestAfter.Generations), manifestAfter.Generations)
	}
	if gen := manifestAfter.Generations[0]; gen.State != leafGenerationStateWritable || gen.GenerationID != 2 {
		t.Fatalf("remaining generation=%+v, want writable generation 2", gen)
	}
}

func TestLeafGenerationGC_ProtectedRootIDsKeepDetachedRootLive(t *testing.T) {
	db, leafLog := openLeafGenerationGCTestDB(t)

	rootTable := mustFrozenSystemMemtable(t, systemRangeKVs(2048, nil)...)
	rootID, err := db.PublishOrderedRootIterator(0, rootTable.NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("PublishOrderedRootIterator: %v", err)
	}
	if rootID == 0 {
		t.Fatal("expected non-zero detached root")
	}
	if state := db.State(); state.RootPageID == rootID || state.SystemRootPageID == rootID {
		t.Fatalf("test requires detached root, got state roots user=%d system=%d detached=%d", state.RootPageID, state.SystemRootPageID, rootID)
	}

	path1, fileID1 := currentLeafSegmentOrFatal(t, leafLog)
	rawFileID1 := page.ValueLogSegmentID(fileID1)
	refs := collectLeafRefIDsFromRoot(t, db, rootID)
	if len(refs) == 0 {
		t.Fatalf("expected detached root %d to contain leaf-log refs", rootID)
	}
	refsFile := false
	for ptr := range refs {
		if ptr.FileID == rawFileID1 {
			refsFile = true
			break
		}
	}
	if !refsFile {
		t.Fatalf("detached root refs do not include raw file id %d: %+v", rawFileID1, refs)
	}

	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeys(t, db, "current", 1, 'z')

	probe, err := db.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{DryRun: true})
	if err != nil {
		t.Fatalf("LeafGenerationGC dry-run: %v", err)
	}
	if got := probe.GenerationsEligible; got == 0 {
		t.Fatalf("GenerationsEligible=%d want detached generation eligible without protection (stats=%+v)", got, probe)
	}

	stats, err := db.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{
		ProtectedRootIDs: []uint64{0, rootID, rootID},
	})
	if err != nil {
		t.Fatalf("LeafGenerationGC protected: %v", err)
	}
	if got := stats.GenerationsLive; got == 0 {
		t.Fatalf("GenerationsLive=%d want protected detached generation live (stats=%+v)", got, stats)
	}
	if got := stats.GenerationsDeleted; got != 0 {
		t.Fatalf("GenerationsDeleted=%d want 0 for protected detached root (stats=%+v)", got, stats)
	}
	if _, err := os.Stat(path1); err != nil {
		t.Fatalf("expected protected leaf segment to remain: %v", err)
	}
}

func TestLeafGenerationPlan_ProtectedOrdinaryRootDoesNotParseDescriptors(t *testing.T) {
	db, _ := openLeafGenerationGCTestDB(t)

	rootTable := mustFrozenRawMemtable(
		t,
		collectionRootDescriptorPrefix+"ordinary-user-key", encodeMaintenanceRootID(123456789),
		"doc/a", []byte("value-a"),
	)
	rootID, err := db.PublishOrderedRootIterator(0, rootTable.NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("PublishOrderedRootIterator: %v", err)
	}

	if _, err := db.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{
		ProtectedRootIDs: []uint64{rootID},
	}); err != nil {
		t.Fatalf("LeafGenerationPlan with protected ordinary root: %v", err)
	}
}

func TestLeafGenerationPlan_ProtectedRootDescriptorReadErrorFailsClosed(t *testing.T) {
	db, _ := openLeafGenerationGCTestDB(t)

	missingPtr := page.ValuePtr{
		FileID: page.ValueLogFileID(99),
		Offset: 8,
		Length: 8,
	}
	rootID, err := db.PublishOrderedRootIterator(
		0,
		mustFrozenSystemPointerMemtable(t, maintenanceTestCollectionRootKey, missingPtr).NewIterator(nil, nil),
	)
	if err != nil {
		t.Fatalf("PublishOrderedRootIterator: %v", err)
	}

	if _, err := db.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{
		ProtectedSystemRootIDs: []uint64{rootID},
	}); err == nil {
		t.Fatal("LeafGenerationPlan with protected pointer-backed descriptor succeeded; want fail-closed read error")
	}
}

func TestLeafGenerationGC_ProtectedSystemRootDescriptorsKeepCollectionRootLive(t *testing.T) {
	db, leafLog := openLeafGenerationGCTestDB(t)

	collectionRootID, err := db.PublishOrderedRootIterator(0, mustFrozenSystemMemtable(t, systemRangeKVs(2048, nil)...).NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("PublishOrderedRootIterator collection: %v", err)
	}
	if collectionRootID == 0 {
		t.Fatal("expected non-zero collection root")
	}
	refs := collectLeafRefIDsFromRoot(t, db, collectionRootID)
	if len(refs) == 0 {
		t.Fatalf("expected collection root %d to contain leaf-log refs", collectionRootID)
	}
	path1, fileID1 := currentLeafSegmentOrFatal(t, leafLog)
	rawFileID1 := page.ValueLogSegmentID(fileID1)
	refsFile := false
	for ptr := range refs {
		if ptr.FileID == rawFileID1 {
			refsFile = true
			break
		}
	}
	if !refsFile {
		t.Fatalf("collection root refs do not include raw file id %d: %+v", rawFileID1, refs)
	}

	systemRootID, err := db.PublishOrderedRootIterator(0, mustFrozenRawMemtable(t, maintenanceTestCollectionRootKey, encodeMaintenanceRootID(collectionRootID)).NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("PublishOrderedRootIterator system: %v", err)
	}
	if systemRootID == 0 {
		t.Fatal("expected non-zero system root")
	}
	if state := db.State(); state.RootPageID == collectionRootID || state.SystemRootPageID == systemRootID {
		t.Fatalf("test requires detached roots, got state user=%d system=%d collection=%d protectedSystem=%d", state.RootPageID, state.SystemRootPageID, collectionRootID, systemRootID)
	}

	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeys(t, db, "current", 1, 'z')

	probe, err := db.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{DryRun: true})
	if err != nil {
		t.Fatalf("LeafGenerationGC dry-run: %v", err)
	}
	if got := probe.GenerationsEligible; got == 0 {
		t.Fatalf("GenerationsEligible=%d want descriptor collection generation eligible without protection (stats=%+v)", got, probe)
	}

	stats, err := db.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{
		ProtectedSystemRootIDs: []uint64{systemRootID},
	})
	if err != nil {
		t.Fatalf("LeafGenerationGC protected: %v", err)
	}
	if got := stats.GenerationsLive; got == 0 {
		t.Fatalf("GenerationsLive=%d want protected descriptor collection generation live (stats=%+v)", got, stats)
	}
	if got := stats.GenerationsDeleted; got != 0 {
		t.Fatalf("GenerationsDeleted=%d want 0 for protected system descriptor root (stats=%+v)", got, stats)
	}
	if _, err := os.Stat(path1); err != nil {
		t.Fatalf("expected protected descriptor leaf segment to remain: %v", err)
	}
}

func TestLeafGenerationGC_IgnoresStaleReachabilityCache(t *testing.T) {
	db, leafLog := openLeafGenerationGCTestDB(t)

	writeLeafGenerationKeys(t, db, "k", 512, 'a')
	path1, fileID1 := currentLeafSegmentOrFatal(t, leafLog)
	rawFileID1 := page.ValueLogSegmentID(fileID1)
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeys(t, db, "z", 1, 'z')

	manifestBefore := loadLeafGenerationManifestOrFatal(t, db.dir)
	gen1 := findLeafGenerationByFileID(t, manifestBefore, rawFileID1)
	if got, want := gen1.State, leafGenerationStateSealed; got != want {
		t.Fatalf("generation1 state=%q, want %q", got, want)
	}
	state := db.state.Load()
	cacheKey, ok := leafGenerationLiveStatsKeyForState(state)
	if !ok {
		t.Fatal("expected cacheable leaf-generation state")
	}

	db.leafGenerationLiveStatsMu.Lock()
	db.leafGenerationLiveStatsCache = leafGenerationLiveStatsCache{
		key:   cacheKey,
		stats: leafGenerationLiveScanStats{Generations: map[uint64]leafGenerationLiveTotals{}},
		ok:    true,
	}
	db.leafGenerationLiveStatsMu.Unlock()

	stats, err := db.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{})
	if err != nil {
		t.Fatalf("LeafGenerationGC: %v", err)
	}
	if got := stats.GenerationsLive; got == 0 {
		t.Fatalf("GenerationsLive=%d, want stale cache ignored (stats=%+v)", got, stats)
	}
	if got := stats.GenerationsDeleted; got != 0 {
		t.Fatalf("GenerationsDeleted=%d, want 0 for live generation (stats=%+v)", got, stats)
	}
	if _, err := os.Stat(path1); err != nil {
		t.Fatalf("expected live first leaf segment to remain: %v", err)
	}
	expectLeafGenerationValue(t, db, leafGenerationKey("k", 0), 'a')
	expectLeafGenerationValue(t, db, leafGenerationKey("k", 511), 'a')
}

func TestLeafGenerationGC_RetiresPinnedGenerationUntilSnapshotCloses(t *testing.T) {
	db, leafLog := openLeafGenerationGCTestDB(t)

	writeLeafGenerationKeys(t, db, "k", 64, 'a')
	path1, fileID1 := currentLeafSegmentOrFatal(t, leafLog)
	rawFileID1 := page.ValueLogSegmentID(fileID1)
	manifestBefore := loadLeafGenerationManifestOrFatal(t, db.dir)
	gen1 := findLeafGenerationByFileID(t, manifestBefore, rawFileID1)

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	if got := db.leafGenerationPinCountForTesting(gen1.GenerationID); got != 0 {
		closeNoErr(t, snap)
		t.Fatalf("pin count before republish=%d, want 0", got)
	}

	if err := leafLog.rotateLeaf(); err != nil {
		closeNoErr(t, snap)
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeys(t, db, "k", 64, 'b')
	if got, want := db.leafGenerationPinCountForTesting(gen1.GenerationID), uint64(1); got != want {
		closeNoErr(t, snap)
		t.Fatalf("pin count after republish=%d, want %d", got, want)
	}
	// Rotate both durable slots past the old generation so this assertion
	// isolates the user snapshot pin rather than the independently recoverable
	// fallback meta root now included by RecoverableRootSet.
	advancePastRetainedDurableSlotForTest(t, db)

	stats1, err := db.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{})
	if err != nil {
		closeNoErr(t, snap)
		t.Fatalf("LeafGenerationGC while pinned: %v", err)
	}
	if got, want := stats1.GenerationsRetiring, 1; got != want {
		closeNoErr(t, snap)
		t.Fatalf("GenerationsRetiring=%d, want %d", got, want)
	}
	if got := stats1.GenerationsDeleted; got != 0 {
		closeNoErr(t, snap)
		t.Fatalf("GenerationsDeleted=%d, want 0 while pinned", got)
	}
	if got := stats1.BytesEligible; got != 0 {
		closeNoErr(t, snap)
		t.Fatalf("BytesEligible=%d, want 0 while pinned", got)
	}
	if got := stats1.BytesDeleted; got != 0 {
		closeNoErr(t, snap)
		t.Fatalf("BytesDeleted=%d, want 0 while pinned", got)
	}
	if _, err := os.Stat(path1); err != nil {
		closeNoErr(t, snap)
		t.Fatalf("expected pinned segment to remain, stat err=%v", err)
	}
	manifestRetiring := loadLeafGenerationManifestOrFatal(t, db.dir)
	genRetiring := findLeafGenerationByFileID(t, manifestRetiring, rawFileID1)
	if got, want := genRetiring.State, leafGenerationStateRetiring; got != want {
		closeNoErr(t, snap)
		t.Fatalf("retiring generation state=%q, want %q", got, want)
	}

	if err := snap.Close(); err != nil {
		t.Fatalf("close snapshot: %v", err)
	}
	if got := db.leafGenerationPinCountForTesting(gen1.GenerationID); got != 0 {
		t.Fatalf("pin count after close=%d, want 0", got)
	}

	stats2, err := db.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{})
	if err != nil {
		t.Fatalf("LeafGenerationGC after close: %v", err)
	}
	if got := stats2.GenerationsEligible; got < 1 {
		t.Fatalf("expected eligible generation after snapshot close, got %d", got)
	}
	if got, want := stats2.GenerationsDeleted, 1; got != want {
		t.Fatalf("GenerationsDeleted=%d, want %d (stats=%+v)", got, want, stats2)
	}
	if got := stats2.BytesDeleted; got <= 0 {
		t.Fatalf("BytesDeleted=%d, want > 0 (stats=%+v)", got, stats2)
	}
	if err := waitForPathRemoval(path1, 5*time.Second); err != nil {
		t.Fatalf("waitForPathRemoval(%s): %v", path1, err)
	}
	if _, err := os.Stat(path1); !os.IsNotExist(err) {
		t.Fatalf("expected retired segment removed, stat err=%v", err)
	}
}
