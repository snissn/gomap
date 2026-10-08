package db

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/leafrefscan"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestValueLogGCCertifiedCompressedClosureAvoidsProjection(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(Options{
		Dir: dir, IndexOuterLeavesInValueLog: true, LeafPrefixCompression: true,
		DisableBackgroundPrune: true, IndexColumnarLeaves: true, IndexPackedValuePtr: true,
		ValueLog: ValueLogOptions{Compression: ValueLogCompressionBlock},
	})
	if err != nil {
		t.Fatal(err)
	}

	leafLog, err := NewStandaloneLeafPageLog(dir, StandaloneLeafPageLogOptions{Compression: ValueLogCompressionBlock, BlockCodec: ValueLogBlockSnappy})
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	database.SetLeafPageLog(leafLog)
	defer func() { _ = database.Close(); _ = leafLog.Close() }()

	id, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(ValueLogDirPath(dir), "value-l0-000001.log")
	writer, err := valuelog.NewWriter(path, id)
	if err != nil {
		t.Fatal(err)
	}
	writer.SetBlockCompression(valuelog.BlockCodecSnappy, true)
	value := bytes.Repeat([]byte("real-compressed-persistent-value|"), 200)
	ptrs, frameStats, err := writer.AppendFrameWithStatsInto(0, nil, []valuelog.Record{{RID: 1, Value: value}}, make([]page.ValuePtr, 1))
	if err != nil {
		_ = writer.Close()
		t.Fatal(err)
	}
	if !frameStats.Kept || frameStats.StoredPayloadBytes >= frameStats.RawPayloadBytes {
		_ = writer.Close()
		t.Fatal("fixture did not physically store compressed value frame")
	}
	ptr := ptrs[0]
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	registerTestValueLogProducer(t, dir, path, id)
	batch := database.NewBatch()
	pointers := batch.(interface {
		SetPointer([]byte, page.ValuePtr) error
	})
	for i := 0; i < 512; i++ {
		if err := pointers.SetPointer([]byte(fmt.Sprintf("live/%06d", i)), ptr); err != nil {
			_ = batch.Close()
			t.Fatal(err)
		}
	}
	if err := batch.Write(); err != nil {
		_ = batch.Close()
		t.Fatal(err)
	}
	if err := batch.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	// Checkpoint alone does not replace the untouched genesis slot. Advance
	// both selectable durable slots through real public publications, keeping
	// all 512 compressed pointers live and adding only a small inline marker.
	beforeSlots := database.currentCommitSeq()
	for i := byte(1); i <= 2; i++ {
		advance := database.NewBatch()
		if err := advance.Set([]byte("fixture/slot-advance"), []byte{i}); err != nil {
			_ = advance.Close()
			t.Fatal(err)
		}
		if err := advance.WriteSync(); err != nil {
			_ = advance.Close()
			t.Fatal(err)
		}
		if err := advance.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if database.currentCommitSeq() != beforeSlots+2 {
		t.Fatal("fixture did not publish both durable slots")
	}
	snap := database.AcquireSnapshot()
	if snap == nil {
		t.Fatal("missing fixture snapshot")
	}
	leaves := 0
	err = leafrefscan.Walk(context.Background(), snap.State().RootPageID, snap.Pager().Get, nil, func(ptr page.LeafLogPtr) error {
		file := snap.State().ValueLogSet.Files[ptr.ValueLogFileID()]
		if file == nil || filepath.Base(filepath.Dir(file.Path)) != "leaf_vlog" {
			return fmt.Errorf("outer leaf lacks registered raw leaf producer: id=%d", ptr.FileID)
		}
		if _, err := os.Stat(file.Path); err != nil {
			return err
		}
		if _, err := snap.reader.ReadUnsafe(ptr.ValuePtr()); err != nil {
			return err
		}
		leaves++
		return nil
	})
	_ = snap.Close()
	if err != nil {
		t.Fatal(err)
	}
	if leaves == 0 {
		t.Fatal("fixture has no readable published outer leaf references")
	}

	fullScans := 0
	database.testValueLogMembershipBeforeFallbackHook = func() { fullScans++ }
	stats, err := database.ValueLogGC(context.Background(), ValueLogGCOptions{DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	if stats.SegmentsReferenced == 0 {
		t.Fatal("GC lost the live compressed segment")
	}
	if stats.Membership.CertifiedRoots == 0 || stats.Membership.UncoveredRoots != 0 || stats.Membership.FullRootScans != 0 || stats.Membership.PointerProjections != 0 {
		t.Fatalf("production compressed roots lack complete certification: certified=%d uncovered=%d scans=%d projections=%d reason=%s", stats.Membership.CertifiedRoots, stats.Membership.UncoveredRoots, stats.Membership.FullRootScans, stats.Membership.PointerProjections, stats.Membership.LastFallbackReason)
	}
	if fullScans != 0 {
		t.Fatalf("certified root closure repeated full logical projection: scans=%d", fullScans)
	}
	// Main ValueLogGC must not promote registered raw outer-leaf identities
	// into candidates, even when explicitly requested by the caller.
	rawSnapshot := database.AcquireSnapshot()
	var rawIDs []uint32
	for id, file := range rawSnapshot.State().ValueLogSet.Files {
		if filepath.Base(filepath.Dir(file.Path)) == "leaf_vlog" {
			rawIDs = append(rawIDs, id)
		}
	}
	_ = rawSnapshot.Close()
	rawStats, err := database.ValueLogGC(context.Background(), ValueLogGCOptions{ObservedSourcesOnly: true, ObservedSourceFileIDs: rawIDs})
	if err != nil || len(rawStats.EligibleFileIDs) != 0 || len(rawStats.ZombieMarkedFileIDs) != 0 {
		t.Fatalf("raw identities entered main GC: eligible=%v marked=%v err=%v", rawStats.EligibleFileIDs, rawStats.ZombieMarkedFileIDs, err)
	}
	for i := 0; i < 512; i++ {
		got, err := database.Get([]byte(fmt.Sprintf("live/%06d", i)))
		if err != nil || !bytes.Equal(got, value) {
			t.Fatalf("compressed live oracle key=%d: %v", i, err)
		}
	}
}
