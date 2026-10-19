package treedb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

// Four fresh-cut readers share each original physical basis while a disjoint
// public writer and checkpoint worker replace it. This is an ownership and
// visibility control, not a concurrency-throughput or mutex-scaling benchmark.
func TestCOWPublicRawBasisConcurrentReadersWriterCheckpoint(t *testing.T) {
	database, opts, blocked := cowPublicContractOpen(t, ProfileNoWALFast, func(o *Options) {
		o.DisableBackgroundPrune = true
		o.BackgroundCheckpointInterval = -1
		o.BackgroundCheckpointIdleDuration = -1
		o.BackgroundIndexVacuumInterval = -1
		o.MaxWALBytes = -1
		o.IndexOuterLeavesInValueLog = true
		o.ValueLog.PointerThreshold = 256
		o.ValueLog.ForcePointers = false
		o.ValueLog.Compression = ValueLogCompressionBlock
		o.ValueLog.BlockCodec = ValueLogBlockLZ4
		o.ValueLog.CompressionAutotune = AutotuneOptions{Mode: AutotuneOff}
	})
	t.Cleanup(func() {
		if !blocked.Load() {
			if err := database.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	cached := database.cached
	registry := database.backend.ValueLogIdentityPinRegistry()
	inline := bytes.Repeat([]byte("physical-inline/"), 8)
	pointer := bytes.Repeat([]byte("physical-pointer/"), 256)
	for i := 0; i < 64; i++ {
		if err := database.Set([]byte(fmt.Sprintf("a%03d", i)), inline); err != nil {
			t.Fatal(err)
		}
	}
	if err := database.Set([]byte("p"), pointer); err != nil {
		t.Fatal(err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	before, ok := database.backend.StateToken()
	if !ok {
		t.Fatal("missing original physical token")
	}
	// Observe actual producer output, without installing a callback or replacing
	// the original leaf logger. The live ordinary leaf file must be compressed LZ4.
	plan, err := database.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{Force: true})
	if err != nil {
		t.Fatal(err)
	}
	var leafFile uint32
	for _, gen := range plan.Generations {
		if gen.LivePages > 0 && len(gen.FileIDs) > 0 {
			leafFile = gen.FileIDs[0]
			break
		}
	}
	if leafFile == 0 {
		t.Fatalf("no live leaf file: %+v", plan)
	}
	lane, seq := valuelog.DecodeFileID(leafFile)
	f, err := os.Open(filepath.Join(backenddb.LeafLogDirPath(opts.Dir), fmt.Sprintf("value-l%d-%06d.log", lane, seq)))
	if err != nil {
		t.Fatal(err)
	}
	var header [valuelog.HeaderSize]byte
	_, readErr := io.ReadFull(f, header[:])
	var body []byte
	if readErr == nil {
		n := binary.LittleEndian.Uint32(header[16:20])
		if n > 2<<20 {
			closeErr := f.Close()
			t.Fatalf("oversized leaf frame=%d close=%v", n, closeErr)
		}
		body = make([]byte, int(n))
		_, readErr = io.ReadFull(f, body)
	}
	closeErr := f.Close()
	if readErr != nil || closeErr != nil {
		t.Fatalf("leaf observation read=%v close=%v", readErr, closeErr)
	}
	frame, _, _, _, err := valuelog.DecodeFrame(body)
	if err != nil || frame.DictID != 0 || frame.Flags&valuelog.FrameFlagCompressed == 0 || valuelog.BlockCodec(frame.Reserved) != valuelog.BlockCodecLZ4 {
		t.Fatalf("actual leaf frame=%+v error=%v", frame, err)
	}
	physical := database.backend.AcquireSnapshot()
	if physical == nil {
		t.Fatal("missing original pointer-resource snapshot")
	}
	entry, entryErr := physical.GetEntry([]byte("p"))
	if entryErr != nil || entry.ValuePtr.FileID == 0 {
		closeErr := physical.Close()
		t.Fatalf("actual pointer=%+v error=%v close=%v", entry.ValuePtr, entryErr, closeErr)
	}
	file, _, fileErr := physical.PinnedValueLogFile(entry.ValuePtr.FileID)
	if fileErr != nil {
		closeErr := physical.Close()
		t.Fatalf("pinned pointer file=%v close=%v", fileErr, closeErr)
	}
	shape, shapeErr := valuelog.InspectCOWRecord(file, entry.ValuePtr, valuelog.COWReadLimits{MaxRecordBytes: 64 << 20, MaxRawBytes: 64 << 20, MaxValueBytes: 64 << 20})
	file = nil
	entry.Key, entry.Value = nil, nil
	closeErr = physical.Close()
	physical = nil
	if shapeErr != nil || closeErr != nil || !shape.Compressed || shape.DictID != 0 || shape.Codec != valuelog.BlockCodecLZ4 {
		t.Fatalf("actual pointer shape=%+v error=%v close=%v", shape, shapeErr, closeErr)
	}
	old, err := cached.AcquireMVCCReadCut()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if old != nil && !blocked.Load() {
			if err := old.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	if got, err := old.Get([]byte("p")); err != nil || !bytes.Equal(got, pointer) {
		t.Fatalf("held original pointer=%d %v", len(got), err)
	}

	// Every pass owns a new original public COW cut; no retained collection cache
	// or reused logical snapshot can conceal a publication or basis replacement.
	fresh := func() (err error) {
		s := database.AcquireSnapshot()
		if s == nil {
			return errors.New("public fresh snapshot unavailable")
		}
		defer func() { err = errors.Join(err, s.Close()) }()
		successor, ok := s.(interface {
			SeekGEVersionRange([]byte, []byte) ([]byte, []byte, bool, error)
		})
		if !ok {
			return fmt.Errorf("public fresh snapshot is not the COW successor scope: %T", s)
		}
		got, err := s.Get([]byte("a000"))
		if err != nil || !bytes.Equal(got, inline) {
			return fmt.Errorf("fresh inline=%q: %w", got, err)
		}
		got[0] ^= 1
		got, err = s.Get([]byte("p"))
		if err != nil || !bytes.Equal(got, pointer) {
			return fmt.Errorf("fresh pointer length=%d: %w", len(got), err)
		}
		got[0] ^= 1
		k, got, found, err := successor.SeekGEVersionRange([]byte("p"), []byte("q"))
		if err != nil || !found || string(k) != "p" || !bytes.Equal(got, pointer) {
			return fmt.Errorf("fresh pointer successor key=%q found=%t length=%d: %w", k, found, len(got), err)
		}
		k[0], got[0] = '!', '!'
		return nil
	}
	var passes [4]int
	err = cowPublicContractComplete(t, blocked, "four readers, writer and checkpoint", func() error {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		start := make(chan struct{})
		ready := make(chan struct{}, 4)
		published := make(chan int)
		checkpointed := make(chan struct{})
		finished := make(chan struct{})
		failures := make(chan error, 6)
		var wg sync.WaitGroup
		run := func(fn func() error) {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := fn(); err != nil {
					failures <- err
					cancel()
				}
			}()
		}
		for reader := range 4 {
			run(func() error {
				if err := fresh(); err != nil {
					return fmt.Errorf("reader %d opening: %w", reader, err)
				}
				passes[reader]++
				ready <- struct{}{}
				select {
				case <-start:
				case <-ctx.Done():
					return ctx.Err()
				}
				for range 64 {
					select {
					case <-finished:
						goto after
					case <-ctx.Done():
						return ctx.Err()
					default:
					}
					if err := fresh(); err != nil {
						return fmt.Errorf("reader %d concurrent: %w", reader, err)
					}
					passes[reader]++
				}
				select {
				case <-finished:
				case <-ctx.Done():
					return ctx.Err()
				}
			after:
				if err := fresh(); err != nil {
					return fmt.Errorf("reader %d post-checkpoint: %w", reader, err)
				}
				passes[reader]++
				return nil
			})
		}
		run(func() error {
			select {
			case <-start:
			case <-ctx.Done():
				return ctx.Err()
			}
			for round := range 4 {
				if err := database.Set([]byte(fmt.Sprintf("w%03d", round)), []byte(fmt.Sprintf("publication-%d", round))); err != nil {
					return fmt.Errorf("writer %d: %w", round, err)
				}
				select {
				case published <- round:
				case <-ctx.Done():
					return ctx.Err()
				}
				select {
				case <-checkpointed:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			close(finished)
			return nil
		})
		run(func() error {
			for range 4 {
				var round int
				select {
				case round = <-published:
				case <-ctx.Done():
					return ctx.Err()
				}
				if err := database.Checkpoint(); err != nil {
					return fmt.Errorf("checkpoint %d: %w", round, err)
				}
				select {
				case checkpointed <- struct{}{}:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		})
		for range 4 {
			select {
			case <-ready:
			case <-ctx.Done():
				wg.Wait()
				return <-failures
			}
		}
		close(start)
		wg.Wait()
		close(failures)
		var result error
		for err := range failures {
			result = errors.Join(result, err)
		}
		return result
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("fresh-cut reader passes=%v", passes)
	for _, count := range passes {
		if count < 2 {
			t.Fatalf("reader did not cross publication: %v", passes)
		}
	}
	after, ok := database.backend.StateToken()
	if !ok || before.RootPageID == after.RootPageID || before.CommitSeq >= after.CommitSeq {
		t.Fatalf("physical basis did not advance: before=%+v after=%+v", before, after)
	}
	if got, err := old.Get([]byte("a000")); err != nil || !bytes.Equal(got, inline) {
		t.Fatalf("held inline after handoffs=%q %v", got, err)
	}
	k, got, found, err := old.SeekGEVersionRange([]byte("p"), []byte("q"))
	if err != nil || !found || string(k) != "p" || !bytes.Equal(got, pointer) {
		t.Fatalf("held pointer successor after handoffs=%q/%d found=%t error=%v", k, len(got), found, err)
	}
	for round := range 4 {
		key := []byte(fmt.Sprintf("w%03d", round))
		if _, err := old.Get(key); !errors.Is(err, ErrKeyNotFound) {
			t.Fatalf("held old cut observed %q: %v", key, err)
		}
		want := []byte(fmt.Sprintf("publication-%d", round))
		if got, err := database.Get(key); err != nil || !bytes.Equal(got, want) {
			t.Fatalf("latest %q=%q %v", key, got, err)
		}
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	old = nil
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if stats := cached.COWMemoryStats(); stats.TotalBytes != 0 || stats.ReservedBytes != 0 || stats.DeferredBytes != 0 || stats.Sources != 0 || stats.Views != 0 || stats.ExternalLeases != 0 {
		t.Fatalf("original COW ownership retained: %+v", stats)
	}
	if stats := registry.Stats(); stats.ActivePins != 0 || stats.ActiveIdentities != 0 || registry.CleanupError() != nil {
		t.Fatalf("physical ownership retained: %+v cleanup=%v", stats, registry.CleanupError())
	}
	reopened, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	}()
	for key, want := range map[string][]byte{"a000": inline, "p": pointer, "w000": []byte("publication-0"), "w001": []byte("publication-1"), "w002": []byte("publication-2"), "w003": []byte("publication-3")} {
		if got, err := reopened.Get([]byte(key)); err != nil || !bytes.Equal(got, want) {
			t.Fatalf("reopened %q=%d %v", key, len(got), err)
		}
	}
}
