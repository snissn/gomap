package caching

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/compression"
	"github.com/snissn/gomap/TreeDB/internal/dictdb"
)

func TestValueLogDictPauseAndProbeResume(t *testing.T) {
	root := t.TempDir()
	maindbDir := filepath.Join(root, "maindb")
	dictdbDir := filepath.Join(root, "dictdb")
	if err := os.MkdirAll(maindbDir, 0755); err != nil {
		t.Fatalf("mkdir maindb: %v", err)
	}
	if err := os.MkdirAll(dictdbDir, 0755); err != nil {
		t.Fatalf("mkdir dictdb: %v", err)
	}

	dictBackend, err := db.Open(db.Options{
		Dir:                    dictdbDir,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("open dictdb: %v", err)
	}
	t.Cleanup(func() { _ = dictBackend.Close() })
	store := dictdb.New(dictBackend)

	const (
		valueSize     = 16 << 10
		batchRecords  = 8
		metricsWindow = 64 << 10
		pauseBytes    = 1 << 20
	)

	values := make([][]byte, 256)
	base := bytes.Repeat([]byte("compressible-"), 256)
	rawHistory := make([]byte, 0, 32<<10)
	for i := range values {
		v := make([]byte, valueSize)
		copy(v, base)
		binary.LittleEndian.PutUint32(v[valueSize-4:], uint32(i))
		values[i] = v
		if len(rawHistory) < cap(rawHistory) {
			need := cap(rawHistory) - len(rawHistory)
			if need > len(v) {
				need = len(v)
			}
			rawHistory = append(rawHistory, v[:need]...)
		}
	}

	dict, err := zstd.BuildDict(zstd.BuildDictOptions{
		ID:       1,
		Contents: values[:128],
		History:  rawHistory,
		Level:    zstd.SpeedFastest,
	})
	if err != nil || len(dict) == 0 {
		t.Fatalf("BuildDict failed: %v", err)
	}

	ctx := context.Background()
	dictID, err := store.PutDictBytes(ctx, dict)
	if err != nil {
		t.Fatalf("PutDictBytes: %v", err)
	}
	if err := store.SetCurrent(ctx, dictID); err != nil {
		t.Fatalf("SetCurrent: %v", err)
	}

	backend, err := db.Open(db.Options{Dir: maindbDir})
	if err != nil {
		t.Fatalf("open maindb: %v", err)
	}
	cached, err := Open(maindbDir, backend, Options{
		FlushThreshold:           8 << 20,
		ValueLogPointerThreshold: 1,
		ValueLogCompression:      uint8(vlogCompressionDict),
		ValueLogDictTrain: compression.TrainConfig{
			TrainBytes:     64 << 10,
			DictBytes:      40 << 10,
			MinRecords:     64,
			MaxRecordBytes: valueSize,
			SampleStride:   1,
			DedupWindow:    64,
		},
		ValueLogDictAdaptiveRatio:          0.98,
		ValueLogDictMetricsWindowBytes:     metricsWindow,
		ValueLogDictMetricsMinRecords:      1,
		ValueLogDictMetricsPauseBytes:      pauseBytes,
		ValueLogDictMinPayloadSavingsRatio: 0.0,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("open cached: %v", err)
	}
	t.Cleanup(func() { _ = cached.Close() })
	cached.SetDictStore(store)

	// 1) Incompressible batch: should degrade and set a pause.
	rng := rand.New(rand.NewSource(1))
	incompressible := func() []byte {
		v := make([]byte, valueSize)
		rng.Read(v)
		return v
	}

	{
		b := cached.NewBatchWithSize(batchRecords)
		for i := 0; i < batchRecords; i++ {
			key := []byte(fmt.Sprintf("i%02d", i))
			if err := b.Set(key, incompressible()); err != nil {
				_ = b.Close()
				t.Fatalf("set: %v", err)
			}
		}
		if err := b.Write(); err != nil {
			_ = b.Close()
			t.Fatalf("write: %v", err)
		}
		_ = b.Close()
	}
	pause1 := cached.valueLogDictPauseRemaining.Load()
	if pause1 == 0 {
		t.Fatalf("expected pause after incompressible batch, got 0")
	}

	// 2) First compressible batch: should still be paused (no probe yet).
	{
		b := cached.NewBatchWithSize(batchRecords)
		for i := 0; i < batchRecords; i++ {
			key := []byte(fmt.Sprintf("c1%02d", i))
			if err := b.Set(key, values[i]); err != nil {
				_ = b.Close()
				t.Fatalf("set: %v", err)
			}
		}
		if err := b.Write(); err != nil {
			_ = b.Close()
			t.Fatalf("write: %v", err)
		}
		_ = b.Close()
	}
	pause2 := cached.valueLogDictPauseRemaining.Load()
	if pause2 == 0 {
		t.Fatalf("expected pause to remain after first compressible batch")
	}
	if pause2 >= pause1 {
		t.Fatalf("expected pause to be consumed: pause1=%d pause2=%d", pause1, pause2)
	}

	// 3) Second compressible batch: should trigger a probe and clear the pause.
	{
		b := cached.NewBatchWithSize(batchRecords)
		for i := 0; i < batchRecords; i++ {
			key := []byte(fmt.Sprintf("c2%02d", i))
			if err := b.Set(key, values[i+batchRecords]); err != nil {
				_ = b.Close()
				t.Fatalf("set: %v", err)
			}
		}
		if err := b.Write(); err != nil {
			_ = b.Close()
			t.Fatalf("write: %v", err)
		}
		_ = b.Close()
	}
	if pause3 := cached.valueLogDictPauseRemaining.Load(); pause3 != 0 {
		t.Fatalf("expected pause to clear after probe success, got %d", pause3)
	}
}
