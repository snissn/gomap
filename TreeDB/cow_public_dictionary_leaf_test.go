package treedb

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/compress/zstd"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/dictdb"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// The producer is the public pack operation, with the original side-store
// dictionary authority. This is a dictionary LEAF control, not a value-frame
// callback substitute. The raw COW reader must retain its cut-local fallback.
func TestCOWPublicPackedDictionaryLeafOldCut(t *testing.T) {
	requireLeafGenerationPackPromotionSupport(t)
	ctx := context.Background()
	database, opts, _ := cowPublicContractOpen(t, ProfileNoWALFast, func(o *Options) {
		o.DisableSideStores = false
		o.DisableBackgroundPrune = true
		o.BackgroundCheckpointInterval = -1
		o.BackgroundCheckpointIdleDuration = -1
		o.BackgroundIndexVacuumInterval = -1
		o.MaxWALBytes = -1
		o.IndexOuterLeavesInValueLog = true
		o.LeafPrefixCompression = true
		o.IndexColumnarLeaves = true
		o.IndexPackedValuePtr = true
		o.ValueLog.PointerThreshold = 1 << 20
		o.ValueLog.ForcePointers = false
		o.ValueLog.Compression = ValueLogCompressionBlock
		o.ValueLog.BlockCodec = ValueLogBlockLZ4
		o.ValueLog.CompressionAutotune = AutotuneOptions{Mode: AutotuneOff}
	})
	// Report cleanup failures on setup/activation failures too; the shared
	// constructor helper alone intentionally performs only best-effort cleanup.
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Error(err)
		}
	})
	cached := database.cached
	registry := database.backend.ValueLogIdentityPinRegistry()

	// Train from the same compact leaf representation as the maintained backend
	// TestLeafGenerationPack_UsesOuterLeafDict, then persist through actual dictdb.
	samples := make([][]byte, 3)
	for n := range samples {
		raw := make([]byte, page.PageSize)
		b := node.NewBuilderWithOptions(raw, page.PageTypeLeaf, node.BuilderOptions{
			LeafPrefixCompression: true, LeafColumnar: true, PackedValuePtr: true,
		})
		for i := 0; i < 16; i++ {
			key := []byte(fmt.Sprintf("dict-pack-%03d-%04d", n, i))
			value := bytes.Repeat([]byte("original-packed-leaf-value/"), 4)
			if err := b.AddLeafEntry(key, value, node.FlagInline, page.ValuePtr{}); err != nil {
				t.Fatal(err)
			}
		}
		b.FinishNoNode()
		compact, _, err := valuelog.MaybeCompactLeafLogPayload(raw)
		if err != nil {
			t.Fatal(err)
		}
		samples[n] = compact
	}
	definition, err := zstd.BuildDict(zstd.BuildDictOptions{
		ID: 9001, Contents: samples, History: bytes.Clone(samples[0]),
		Offsets: [3]int{1, 4, 8}, Level: zstd.SpeedFastest,
	})
	if err != nil {
		t.Fatal(err)
	}
	store := dictdb.New(database.dictdb)
	id, err := store.PutDictBytes(ctx, definition)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SetCurrentForClass(ctx, "outer_leaf", id); err != nil {
		t.Fatal(err)
	}
	if err := store.SetLeafPayloadMode(ctx, id, false); err != nil {
		t.Fatal(err)
	}
	cached.SetDictStore(store)

	want := bytes.Repeat([]byte("original-packed-leaf-value/"), 4)
	batch := database.NewBatch()
	defer func() {
		if batch != nil {
			if err := batch.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	for i := 0; i < 2048; i++ {
		if err := batch.Set([]byte(fmt.Sprintf("dict-pack-000-%04d", i)), want); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}
	if err := batch.Close(); err != nil {
		t.Fatal(err)
	}
	batch = nil
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	// This maintained maintenance fence rotates the actual cached leaf lane. No
	// replacement logger, synthetic frame or backend-only producer is installed.
	finish, err := cached.BeginValueLogMaintenanceFence(ctx)
	if err != nil {
		t.Fatal(err)
	}
	finish()
	plan, err := database.LeafGenerationPlan(ctx, LeafGenerationPlanOptions{Force: true})
	if err != nil {
		t.Fatal(err)
	}
	var generations []uint64
	for _, gen := range plan.Generations {
		if gen.Eligible && gen.LivePages > 0 {
			generations = append(generations, gen.GenerationID)
		}
	}
	if len(generations) == 0 {
		t.Fatalf("no sealed live public leaf generation: %+v", plan)
	}
	packed, err := database.LeafGenerationPack(ctx, LeafGenerationPackOptions{GenerationIDs: generations, Force: true, Sync: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(packed.CreatedFileIDs) == 0 {
		t.Fatalf("no packed leaf output: %+v", packed)
	}
	// Observe the real packed record outside reads, exactly as the maintained
	// pack fixture does. All rows below are then read from the installed root.
	for _, fileID := range packed.CreatedFileIDs {
		lane, seq := valuelog.DecodeFileID(fileID)
		path := filepath.Join(backenddb.LeafLogDirPath(opts.Dir), fmt.Sprintf("value-l%d-%06d.log", lane, seq))
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		var header [valuelog.HeaderSize]byte
		_, readErr := io.ReadFull(f, header[:])
		var body []byte
		if readErr == nil {
			size := binary.LittleEndian.Uint32(header[16:20])
			if size > 2<<20 {
				closeErr := f.Close()
				t.Fatalf("oversized packed leaf record: %d; close=%v", size, closeErr)
			}
			body = make([]byte, int(size))
			_, readErr = io.ReadFull(f, body)
		}
		closeErr := f.Close()
		if readErr != nil || closeErr != nil {
			t.Fatalf("packed record read=%v close=%v", readErr, closeErr)
		}
		frame, _, _, _, err := valuelog.DecodeFrame(body)
		if err != nil || frame.DictID != id || frame.Flags&valuelog.FrameFlagCompressed == 0 {
			t.Fatalf("actual packed leaf frame=%+v error=%v want compressed dictionary=%d", frame, err, id)
		}
	}
	key := []byte("dict-pack-000-0000")
	if got, err := database.Get(key); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("public packed Get=%q %v", got, err)
	}
	if k, got, found, err := database.SeekGEVersionRange(key, []byte("dict-pack-000-0001")); err != nil || !found || !bytes.Equal(k, key) || !bytes.Equal(got, want) {
		t.Fatalf("public packed successor=%q/%q found=%t error=%v", k, got, found, err)
	}
	// Request the existing exported admitted COW cut so its point and successor
	// reads demonstrably share the same original dictionary lifetime.
	old, err := cached.AcquireMVCCReadCut()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if old != nil {
			if err := old.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	for i := 0; i < 2048; i++ {
		if got, err := old.Get([]byte(fmt.Sprintf("dict-pack-000-%04d", i))); err != nil || !bytes.Equal(got, want) {
			t.Fatalf("old packed row %d=%q %v", i, got, err)
		}
	}
	replacement := []byte("newer-logical-winner")
	if err := database.Set(key, replacement); err != nil {
		t.Fatal(err)
	}
	if got, err := database.Get(key); err != nil || !bytes.Equal(got, replacement) {
		t.Fatalf("current pending winner=%q %v", got, err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if got, err := old.Get(key); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("old packed point after handoff=%q %v", got, err)
	}
	k, got, found, err := old.SeekGEVersionRange(key, []byte("dict-pack-000-0001"))
	if err != nil || !found || !bytes.Equal(k, key) || !bytes.Equal(got, want) {
		t.Fatalf("old packed successor after handoff=%q/%q found=%t error=%v", k, got, found, err)
	}
	k[0], got[0] = '!', '!'
	if got, err := old.Get(key); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("old output alias=%q %v", got, err)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	old = nil
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if stats := cached.COWMemoryStats(); stats.TotalBytes != 0 || stats.HistoryBytes != 0 || stats.DeferredBytes != 0 || stats.ReservedBytes != 0 || stats.ExternalLeases != 0 || stats.Sources != 0 || stats.Views != 0 {
		t.Fatalf("original COW charge retained: %+v", stats)
	}
	if stats := registry.Stats(); stats.ActivePins != 0 || stats.ActiveIdentities != 0 || registry.CleanupError() != nil {
		t.Fatalf("original physical custody retained: %+v cleanup=%v", stats, registry.CleanupError())
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
	if got, err := reopened.Get(key); err != nil || !bytes.Equal(got, replacement) {
		t.Fatalf("reopened newer winner=%q %v", got, err)
	}
	if got, err := reopened.Get([]byte("dict-pack-000-2047")); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("reopened untouched packed row=%q %v", got, err)
	}
}
