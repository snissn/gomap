package caching

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/dictdb"
)

func TestValueLogDictCompressionReducesBytes(t *testing.T) {
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

	const valueCount = 512
	const valueSize = 1024
	values := make([][]byte, valueCount)
	base := bytes.Repeat([]byte("compressible-"), 64)
	rawTotal := 0
	for i := 0; i < valueCount; i++ {
		v := make([]byte, valueSize)
		copy(v, base)
		binary.LittleEndian.PutUint32(v[valueSize-4:], uint32(i))
		values[i] = v
		rawTotal += len(v)
	}

	sampleCount := 256
	if sampleCount > len(values) {
		sampleCount = len(values)
	}
	const historyBytes = 32 << 10
	history := make([]byte, 0, historyBytes)
	for _, v := range values {
		if len(history) >= historyBytes {
			break
		}
		need := historyBytes - len(history)
		if len(v) > need {
			history = append(history, v[:need]...)
		} else {
			history = append(history, v...)
		}
	}
	if len(history) < 8 {
		t.Fatalf("history too small: %d", len(history))
	}
	dict, err := zstd.BuildDict(zstd.BuildDictOptions{
		ID:       1,
		Contents: values[:sampleCount],
		History:  history,
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
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("open cached: %v", err)
	}
	t.Cleanup(func() { _ = cached.Close() })
	cached.SetDictStore(store)

	for i, val := range values {
		key := []byte(fmt.Sprintf("k%06d", i))
		if err := cached.Set(key, val); err != nil {
			t.Fatalf("set: %v", err)
		}
	}
	if err := cached.flushValueLog(); err != nil {
		t.Fatalf("flush value log: %v", err)
	}

	valueBytes := func() int64 {
		var total int64
		for i := range cached.lanes {
			l := &cached.lanes[i]
			l.vlogMu.Lock()
			for _, sz := range l.vlogClosedSizes {
				total += sz
			}
			if l.vlog != nil {
				total += l.vlog.Size()
			}
			l.vlogMu.Unlock()
		}
		return total
	}()
	if valueBytes == 0 {
		t.Fatalf("expected value log bytes > 0")
	}
	if float64(valueBytes) >= float64(rawTotal)*0.8 {
		t.Fatalf("expected compression reduction: raw=%d stored=%d", rawTotal, valueBytes)
	}
}
