package treedb_test

import (
	"bytes"
	"fmt"
	"io"
	"path/filepath"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func TestPublicCommandWALRawFrames_DurableGrouping(t *testing.T) {
	dir := t.TempDir()
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir)
	opts.DisableSideStores = true
	opts.IndexOuterLeavesInValueLog = false
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.BackgroundIndexVacuumInterval = -1
	opts.DisableBackgroundPrune = true
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true
	opts.ValueLog.Compression = treedb.ValueLogCompressionOff
	opts.ValueLog.BlockTargetCompressedBytes = 4096
	database, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if mode := database.Stats()["treedb.cache.redo_log.mode"]; mode != "external_command_wal" {
		t.Fatalf("redo mode=%q, want external_command_wal", mode)
	}
	const count = 32
	values := make([][]byte, count)
	batch := database.NewBatch()
	defer batch.Close()
	for i := range values {
		value := make([]byte, 4096)
		state := uint32(i + 1)
		for j := range value {
			state ^= state << 13
			state ^= state >> 17
			state ^= state << 5
			value[j] = byte(state)
		}
		values[i] = value
		if err := batch.Set([]byte(fmt.Sprintf("raw-%02d", i)), value); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.WriteSync(); err != nil {
		t.Fatal(err)
	}
	if err := batch.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	paths, err := filepath.Glob(filepath.Join(dir, "value_vlog", "value-l*.log"))
	if err != nil {
		t.Fatal(err)
	}
	readCount := 0
	for _, path := range paths {
		var lane, sequence int
		if _, err := fmt.Sscanf(filepath.Base(path), "value-l%d-%d.log", &lane, &sequence); err != nil {
			t.Fatal(err)
		}
		fileID, err := valuelog.EncodeFileID(uint32(lane), uint32(sequence))
		if err != nil {
			t.Fatal(err)
		}
		reader, err := valuelog.NewReader(path, fileID)
		if err != nil {
			t.Fatal(err)
		}
		perFrame := make(map[uint64]int)
		for {
			_, value, ptr, err := reader.ReadNext()
			if err == io.EOF {
				break
			}
			if err != nil {
				reader.Close()
				t.Fatal(err)
			}
			if len(value) != 4096 {
				reader.Close()
				t.Fatalf("persisted value bytes=%d, want 4096", len(value))
			}
			perFrame[ptr.Offset]++
			readCount++
		}
		if err := reader.Close(); err != nil {
			t.Fatal(err)
		}
		for offset, grouped := range perFrame {
			if grouped != 1 {
				t.Fatalf("command-WAL raw frame %s:%d holds %d 4 KiB values, want 1", path, offset, grouped)
			}
		}
	}
	if readCount != count {
		t.Fatalf("persisted values=%d, want %d", readCount, count)
	}
	database, err = treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	for i, want := range values {
		got, err := database.Get([]byte(fmt.Sprintf("raw-%02d", i)))
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("reopened value %d: err=%v", i, err)
		}
	}
}
