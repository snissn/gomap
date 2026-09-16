package db

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

type leafSequenceAuthorityTestLog struct {
	seq *leafLogSeqAllocator
}

func (l *leafSequenceAuthorityTestLog) AppendLeafPage([]byte) (page.LeafLogPtr, error) {
	return page.LeafLogPtr{}, errors.New("test leaf log append is unsupported")
}

func (*leafSequenceAuthorityTestLog) Flush() error { return nil }
func (*leafSequenceAuthorityTestLog) Sync() error  { return nil }

func (*leafSequenceAuthorityTestLog) ConcurrentLeafPageAppends() bool { return true }

func (l *leafSequenceAuthorityTestLog) ReserveLeafPageLogSequence(floor uint32) (uint32, error) {
	if l == nil || l.seq == nil {
		return 0, errors.New("test leaf log sequence authority unavailable")
	}
	return l.seq.ReserveLeafPageLogSequence(floor)
}

func TestLeafGenerationPackAllocatorsUseInstalledSequenceAuthority(t *testing.T) {
	live := newLeafLogSeqAllocator(39)
	database := &DB{leafPageLog: &leafSequenceAuthorityTestLog{seq: live}}

	pack, _, err := database.leafGenerationPackAllocators(39, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	packSeq, err := pack.ReserveLeafPageLogSequence(39)
	if err != nil {
		t.Fatal(err)
	}
	liveSeq, err := live.Next()
	if err != nil {
		t.Fatal(err)
	}
	if packSeq == liveSeq {
		t.Fatalf("pack and installed live owner both reserved sequence %d", packSeq)
	}
	if packSeq != 40 || liveSeq != 41 {
		t.Fatalf("pack/live sequences=(%d,%d), want (40,41)", packSeq, liveSeq)
	}
}

func TestLeafLogSequenceAllocatorConcurrentReservations(t *testing.T) {
	const (
		floor   = uint32(39)
		workers = 16
		perRun  = 128
	)
	for run := 0; run < 8; run++ {
		allocator := newLeafLogSeqAllocator(floor)
		sequences := make(chan uint32, workers*perRun)
		errs := make(chan error, workers)
		var wg sync.WaitGroup
		for worker := 0; worker < workers; worker++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < perRun; i++ {
					seq, err := allocator.ReserveLeafPageLogSequence(floor)
					if err != nil {
						errs <- err
						return
					}
					sequences <- seq
				}
			}()
		}
		wg.Wait()
		close(errs)
		close(sequences)
		for err := range errs {
			t.Fatal(err)
		}
		seen := make(map[uint32]struct{}, workers*perRun)
		for seq := range sequences {
			if seq <= floor || seq > floor+workers*perRun {
				t.Fatalf("run %d reserved out-of-range sequence %d", run, seq)
			}
			if _, exists := seen[seq]; exists {
				t.Fatalf("run %d reserved duplicate sequence %d", run, seq)
			}
			seen[seq] = struct{}{}
		}
		if len(seen) != workers*perRun {
			t.Fatalf("run %d reserved %d sequences, want %d", run, len(seen), workers*perRun)
		}
	}
}

func TestLeafLogSequenceReservationDoesNotReuseAbortedSequence(t *testing.T) {
	allocator := newLeafLogSeqAllocator(39)
	aborted, err := allocator.ReserveLeafPageLogSequence(39)
	if err != nil {
		t.Fatal(err)
	}
	next, err := allocator.ReserveLeafPageLogSequence(39)
	if err != nil {
		t.Fatal(err)
	}
	if aborted != 40 || next != 41 {
		t.Fatalf("aborted/next sequences=(%d,%d), want (40,41)", aborted, next)
	}
}

func TestLeafGenerationPackStablePrepareUnsupportedConcurrentOwnerFailsBeforeStaging(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	database.SetLeafPageLog(&unsupportedConcurrentLeafSequenceOwner{})
	stagingCalls := 0
	originalMkdirTemp := makeLeafGenerationPackStagingDirFn
	makeLeafGenerationPackStagingDirFn = func(dir, pattern string) (string, error) {
		stagingCalls++
		return originalMkdirTemp(dir, pattern)
	}
	t.Cleanup(func() { makeLeafGenerationPackStagingDirFn = originalMkdirTemp })

	closure, err := database.PrepareLeafGenerationPackStableClosure(context.Background(), [][]byte{
		buildLeafGenerationPackStablePage(t, 's'),
	})
	if closure != nil {
		_ = closure.Abandon()
		t.Fatal("unsupported concurrent owner returned a stable prepared closure")
	}
	if err == nil {
		t.Fatal("unsupported concurrent owner did not fail closed")
	}
	if stagingCalls != 0 {
		t.Fatalf("unsupported concurrent owner attempted %d staging directory creations", stagingCalls)
	}
	matches, globErr := filepath.Glob(filepath.Join(LeafLogDirPath(dir), ".leaf-pack-stable-prepare-*"))
	if globErr != nil {
		t.Fatal(globErr)
	}
	if len(matches) != 0 {
		t.Fatalf("unsupported concurrent owner created staging directories: %v", matches)
	}
}

func TestLeafGenerationPackUnsupportedConcurrentOwnerFailsBeforeCopyStaging(t *testing.T) {
	database, leafLog, dir := openLeafGenerationPackTestDB(t)
	candidate := prepareLeafGenerationPackTestCandidate(t, database, leafLog, 256)
	database.SetLeafPageLog(&unsupportedConcurrentLeafSequenceOwner{})
	stagingCalls := 0
	originalMkdirTemp := makeLeafGenerationPackStagingDirFn
	makeLeafGenerationPackStagingDirFn = func(dir, pattern string) (string, error) {
		stagingCalls++
		return originalMkdirTemp(dir, pattern)
	}
	t.Cleanup(func() { makeLeafGenerationPackStagingDirFn = originalMkdirTemp })

	_, err := database.LeafGenerationPack(context.Background(), LeafGenerationPackOptions{
		GenerationIDs: []uint64{candidate.generation.GenerationID},
		Force:         true,
	})
	if err == nil {
		t.Fatal("unsupported concurrent owner did not fail closed")
	}
	if stagingCalls != 0 {
		t.Fatalf("unsupported concurrent owner attempted %d staging directory creations", stagingCalls)
	}
	matches, globErr := filepath.Glob(filepath.Join(LeafLogDirPath(dir), ".leaf-pack-copy-*"))
	if globErr != nil {
		t.Fatal(globErr)
	}
	if len(matches) != 0 {
		t.Fatalf("unsupported concurrent owner created copy staging directories: %v", matches)
	}
}

func TestLeafGenerationPackSharedAuthorityInterleavesLiveChildAndReopens(t *testing.T) {
	requireLeafGenerationPackPromotionSupport(t)
	dir := t.TempDir()
	opts := Options{
		Dir:                        dir,
		Durability:                 DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
	}
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	leafLog := newRewriteWriter(ValueLogDirPath(dir), 0, 0, 64<<20)
	leafLog.ConfigureLeafLog(LeafLogDirPath(dir), rewriteLeafLogLaneID, 0)
	database.SetLeafPageLog(leafLog)
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = database.Close()
			_ = leafLog.Close()
		}
	})
	candidate := prepareLeafGenerationPackTestCandidate(t, database, leafLog, 256)

	liveAuthority := &leafSequenceAuthorityTestLog{seq: newLeafLogSeqAllocator(0)}
	database.SetLeafPageLog(liveAuthority)
	var stagedFileIDs []uint32
	var liveFileID uint32
	var livePath string
	var liveBefore os.FileInfo
	var hookErr error
	unregister := registerLeafGenerationPackCopyHook(func(event leafGenerationPackCopyEvent) {
		if hookErr != nil || len(stagedFileIDs) != 0 || event.Phase != leafGenerationPackCopyComplete {
			return
		}
		stagedFileIDs = append(stagedFileIDs, event.CreatedFileIDs...)
		seq, err := liveAuthority.ReserveLeafPageLogSequence(0)
		if err != nil {
			hookErr = err
			return
		}
		liveFileID, err = valuelog.EncodeFileID(rewriteLeafLogLaneID, seq)
		if err != nil {
			hookErr = err
			return
		}
		livePath = valuelog.SegmentPath(LeafLogDirPath(dir), liveFileID)
		writer, err := valuelog.NewWriter(livePath, liveFileID)
		if err != nil {
			hookErr = err
			return
		}
		if _, err = writer.Append(0, nil, 9_000_000, []byte("concurrent-live-child")); err == nil {
			err = writer.Sync()
		}
		if closeErr := writer.Close(); err == nil {
			err = closeErr
		}
		if err != nil {
			hookErr = err
			return
		}
		liveBefore, hookErr = os.Stat(livePath)
	})
	defer unregister()

	stats, err := database.LeafGenerationPack(context.Background(), LeafGenerationPackOptions{
		GenerationIDs: []uint64{candidate.generation.GenerationID},
		Force:         true,
		Sync:          true,
	})
	if err != nil {
		t.Fatalf("LeafGenerationPack: %v", err)
	}
	if hookErr != nil {
		t.Fatalf("live child interleave: %v", hookErr)
	}
	if len(stagedFileIDs) == 0 || len(stats.CreatedFileIDs) == 0 || liveFileID == 0 {
		t.Fatalf("staged=%v published=%v live=%d, want all non-empty", stagedFileIDs, stats.CreatedFileIDs, liveFileID)
	}
	for _, stagedFileID := range stagedFileIDs {
		if stagedFileID == liveFileID {
			t.Fatalf("staged and live owners both selected file ID %d", liveFileID)
		}
	}
	liveAfter, err := os.Stat(livePath)
	if err != nil {
		t.Fatalf("stat live child after promotion: %v", err)
	}
	if liveBefore == nil || !os.SameFile(liveBefore, liveAfter) {
		t.Fatal("pack promotion replaced the concurrent live child")
	}
	for i := 0; i < 256; i++ {
		want := byte('a')
		if i < 128 {
			want = 'b'
		}
		expectLeafGenerationValue(t, database, leafGenerationKey("pack-concurrent", i), want)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("Close database: %v", err)
	}
	if err := leafLog.Close(); err != nil {
		t.Fatalf("Close original leaf log: %v", err)
	}
	closed = true

	reopened, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	for i := 0; i < 256; i++ {
		want := byte('a')
		if i < 128 {
			want = 'b'
		}
		expectLeafGenerationValue(t, reopened, leafGenerationKey("pack-concurrent", i), want)
	}
}

type unsupportedConcurrentLeafSequenceOwner struct{}

func (*unsupportedConcurrentLeafSequenceOwner) AppendLeafPage([]byte) (page.LeafLogPtr, error) {
	return page.LeafLogPtr{}, errors.New("test leaf log append is unsupported")
}

func (*unsupportedConcurrentLeafSequenceOwner) Flush() error { return nil }
func (*unsupportedConcurrentLeafSequenceOwner) Sync() error  { return nil }

func (*unsupportedConcurrentLeafSequenceOwner) ConcurrentLeafPageAppends() bool { return true }

func BenchmarkLeafLogSequenceAllocation(b *testing.B) {
	const reservationsPerAllocator = 1 << 20
	allocator := newLeafLogSeqAllocator(39)
	b.ReportAllocs()
	started := time.Now()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i != 0 && i%reservationsPerAllocator == 0 {
			allocator = newLeafLogSeqAllocator(39)
		}
		if _, err := allocator.Next(); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N)/time.Since(started).Seconds(), "ops/s")
}
