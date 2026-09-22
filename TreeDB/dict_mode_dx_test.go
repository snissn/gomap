package treedb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestOpen_DictAndAutoCompression_RequireSideStoresUnlessTrainingDisabled(t *testing.T) {
	t.Run("dict_requires_side_stores", func(t *testing.T) {
		_, err := Open(Options{
			Dir:               t.TempDir(),
			DisableSideStores: true,
			ValueLog: ValueLogOptions{
				PointerThreshold: 1,
				Compression:      ValueLogCompressionDict,
			},
		})
		if err == nil || !strings.Contains(err.Error(), "requires side stores") {
			t.Fatalf("expected side-store error, got %v", err)
		}
	})

	t.Run("auto_allows_side_stores_disabled", func(t *testing.T) {
		db, err := Open(Options{
			Dir:               t.TempDir(),
			DisableSideStores: true,
			ValueLog: ValueLogOptions{
				PointerThreshold: 1,
				Compression:      ValueLogCompressionAuto,
			},
		})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		if err := db.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	})
}

func TestValueLogCompressionAuto_DefaultsToDictTrainingAndProbesDict(t *testing.T) {
	dir := t.TempDir()
	bgErrCh := make(chan error, 16)
	opts := Options{
		Dir:            dir,
		FlushThreshold: 1 << 20,
		ValueLog: ValueLogOptions{
			PointerThreshold: 1,
			Compression:      ValueLogCompressionAuto,
			AutoPolicy:       ValueLogAutoSize,
			// Keep the test deterministic: publish the first accepted dictionary
			// without autotune dwell/gain gating.
			CompressionAutotune: AutotuneOptions{Mode: AutotuneOff},
			// Avoid default adaptive pause thresholds so publication/probing can be
			// observed reliably on slower CI hosts.
			DictAdaptiveRatio: -1,
			// Leave DictTrain.TrainBytes unset (0): auto mode should still train
			// dictionaries so dict compression can become active.
		},
		NotifyError: func(err error) {
			select {
			case bgErrCh <- err:
			default:
			}
		},
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	const valueSize = 16 << 10
	base := bytes.Repeat([]byte("compressible-"), valueSize/len("compressible-")+1)[:valueSize]

	writeBatch := func(prefix string, n int) {
		for i := 0; i < n; i++ {
			key := []byte(fmt.Sprintf("%s%06d", prefix, i))
			val := make([]byte, valueSize)
			copy(val, base)
			binary.LittleEndian.PutUint32(val[valueSize-4:], uint32(i))
			if err := db.Set(key, val); err != nil {
				t.Fatalf("Set: %v", err)
			}
		}
	}

	// Phase 1: produce enough data for the dict trainer to publish a dictionary.
	writeBatch("a", 128) // ~2MiB
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint(phase1): %v", err)
	}

	var stats map[string]string
	deadline := time.Now().Add(20 * time.Second)
	published := false
	for time.Now().Before(deadline) {
		select {
		case err := <-bgErrCh:
			t.Fatalf("background error: %v", err)
		default:
		}
		stats = db.Stats()
		if stats != nil {
			if v, ok := stats["treedb.cache.vlog_dict.last_applied_dict_id"]; ok {
				if id, err := strconv.ParseUint(v, 10, 64); err == nil && id > 0 {
					published = true
					break
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !published {
		stats = db.Stats()
		t.Fatalf("expected dict to publish under auto mode, got last_applied_dict_id=%q frames_attempted=%q",
			stats["treedb.cache.vlog_dict.last_applied_dict_id"],
			stats["treedb.cache.vlog_dict.frames_attempted"],
		)
	}
	// Phase 2: once the dict exists, write enough bytes to force an exploration
	// probe where the auto selector should try dict frames at least once.
	writeBatch("b", 512) // ~8MiB
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint(phase2): %v", err)
	}

	stats = db.Stats()
	if stats == nil {
		t.Fatalf("Stats: nil")
	}
	dictFrames, _ := strconv.ParseUint(stats["treedb.cache.vlog_auto.frames.dict"], 10, 64)
	if dictFrames == 0 {
		t.Fatalf("expected auto mode to probe dict frames, got frames.dict=%q frames.block_lz4=%q frames.block_snappy=%q frames.off=%q last_applied_dict_id=%q",
			stats["treedb.cache.vlog_auto.frames.dict"],
			stats["treedb.cache.vlog_auto.frames.block_lz4"],
			stats["treedb.cache.vlog_auto.frames.block_snappy"],
			stats["treedb.cache.vlog_auto.frames.off"],
			stats["treedb.cache.vlog_dict.last_applied_dict_id"],
		)
	}
}
