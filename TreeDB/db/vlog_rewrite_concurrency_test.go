package db

import (
	"context"
	"encoding/binary"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func concurrencyKey(i int) []byte {
	k := make([]byte, 8)
	binary.BigEndian.PutUint64(k, uint64(i))
	return k
}

func concurrencyValue(version uint64, i int, n int) []byte {
	if n < 16 {
		n = 16
	}
	v := make([]byte, n)
	binary.BigEndian.PutUint64(v[:8], version)
	binary.BigEndian.PutUint64(v[8:16], uint64(i))
	for j := 16; j < len(v); j++ {
		v[j] = byte(version>>uint(j%8*8)) ^ byte(i)
	}
	return v
}

func decodeConcurrencyValue(t *testing.T, v []byte) (version uint64, i int) {
	t.Helper()
	if len(v) < 16 {
		t.Fatalf("value too small: %dB", len(v))
	}
	version = binary.BigEndian.Uint64(v[:8])
	i = int(binary.BigEndian.Uint64(v[8:16]))
	return version, i
}

func validateConcurrencyValueBytes(t *testing.T, got []byte, version uint64, i int, n int) {
	t.Helper()
	if len(got) != n {
		t.Fatalf("unexpected value len: got=%dB want=%dB", len(got), n)
	}
	gotVer, gotI := decodeConcurrencyValue(t, got)
	if gotVer != version || gotI != i {
		t.Fatalf("unexpected header: got(ver=%d,i=%d) want(ver=%d,i=%d)", gotVer, gotI, version, i)
	}
	for j := 16; j < len(got); j++ {
		want := byte(version>>uint(j%8*8)) ^ byte(i)
		if got[j] != want {
			t.Fatalf("unexpected value byte j=%d got=%#x want=%#x", j, got[j], want)
		}
	}
}

func TestValueLogRewriteOnline_DoesNotLoseConcurrentWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in -short")
	}

	const (
		keyCount  = 20_000
		valueSize = 256
	)

	dir := t.TempDir()
	walDir := filepath.Join(dir, "value_vlog")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("mkdir wal: %v", err)
	}

	pathA := filepath.Join(walDir, "value-l0-000001.log")
	idA, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("fileidA: %v", err)
	}
	wA, err := valuelog.NewWriter(pathA, idA)
	if err != nil {
		t.Fatalf("writerA: %v", err)
	}
	wA.SetBlockCompression(valuelog.BlockCodecSnappy, false)
	ptrA := make([]page.ValuePtr, keyCount)
	for i := 0; i < keyCount; i++ {
		ptr, err := wA.Append(0, nil, uint64(i+1), concurrencyValue(1, i, valueSize))
		if err != nil {
			_ = wA.Close()
			t.Fatalf("appendA i=%d: %v", i, err)
		}
		ptrA[i] = ptr
	}
	if err := wA.Close(); err != nil {
		t.Fatalf("closeA: %v", err)
	}
	registerTestValueLogProducer(t, dir, pathA, idA)

	pathB := filepath.Join(walDir, "value-l0-000002.log")
	idB, err := valuelog.EncodeFileID(0, 2)
	if err != nil {
		t.Fatalf("fileidB: %v", err)
	}
	wB, err := valuelog.NewWriter(pathB, idB)
	if err != nil {
		t.Fatalf("writerB: %v", err)
	}
	wB.SetBlockCompression(valuelog.BlockCodecSnappy, false)
	ptrB := make([]page.ValuePtr, keyCount)
	for i := 0; i < keyCount; i++ {
		ptr, err := wB.Append(0, nil, uint64(keyCount+i+1), concurrencyValue(2, i, valueSize))
		if err != nil {
			_ = wB.Close()
			t.Fatalf("appendB i=%d: %v", i, err)
		}
		ptrB[i] = ptr
	}
	if err := wB.Close(); err != nil {
		t.Fatalf("closeB: %v", err)
	}
	registerTestValueLogProducer(t, dir, pathB, idB)

	db, err := Open(Options{
		Dir:                        dir,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: false,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
		LeafPrefixCompression:      true,
		ValueLog: ValueLogOptions{
			Compression:   ValueLogCompressionOff,
			ReadIntegrity: IntegritySkipChecksums,
		},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	seed := db.NewBatchWithSize(1024).(*Batch)
	for i := 0; i < keyCount; i++ {
		if err := seed.SetPointer(concurrencyKey(i), ptrA[i]); err != nil {
			_ = seed.Close()
			t.Fatalf("seed set i=%d: %v", i, err)
		}
		if (i+1)%1024 == 0 {
			if err := seed.Write(); err != nil {
				_ = seed.Close()
				t.Fatalf("seed write: %v", err)
			}
			seed.Reset()
		}
	}
	if err := seed.Write(); err != nil {
		_ = seed.Close()
		t.Fatalf("seed write: %v", err)
	}
	_ = seed.Close()

	expectedVer := make([]uint64, keyCount)
	for i := range expectedVer {
		expectedVer[i] = 1
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	var (
		mu      sync.Mutex
		bgErr   error
		stopCh  = make(chan struct{})
		stopped = make(chan struct{})
	)

	// Continuously overwrite random keys (with pointer swaps) while rewrite runs.
	go func() {
		defer close(stopped)
		rng := rand.New(rand.NewSource(1))
		b := db.NewBatchWithSize(32).(*Batch)
		defer b.Close()
		type pendingWrite struct {
			i   int
			ver uint64
		}
		pending := make([]pendingWrite, 0, 32)
		opsInBatch := 0
		for {
			select {
			case <-stopCh:
				if opsInBatch > 0 {
					if err := b.Write(); err != nil {
						mu.Lock()
						if bgErr == nil {
							bgErr = err
						}
						mu.Unlock()
						return
					}
					mu.Lock()
					for _, pw := range pending {
						expectedVer[pw.i] = pw.ver
					}
					mu.Unlock()
					b.Reset()
				}
				return
			default:
			}

			i := rng.Intn(keyCount)
			ver := uint64(1)
			ptr := ptrA[i]
			if rng.Intn(2) == 1 {
				ver = 2
				ptr = ptrB[i]
			}

			if err := b.SetPointer(concurrencyKey(i), ptr); err != nil {
				mu.Lock()
				if bgErr == nil {
					bgErr = err
				}
				mu.Unlock()
				return
			}

			pending = append(pending, pendingWrite{i: i, ver: ver})
			opsInBatch++

			if opsInBatch >= 32 {
				if err := b.Write(); err != nil {
					mu.Lock()
					if bgErr == nil {
						bgErr = err
					}
					mu.Unlock()
					return
				}
				mu.Lock()
				for _, pw := range pending {
					expectedVer[pw.i] = pw.ver
				}
				mu.Unlock()
				pending = pending[:0]
				opsInBatch = 0
				b.Reset()
			}

			if i%256 == 0 {
				time.Sleep(100 * time.Microsecond)
			}
		}
	}()

	protected := []string{pathA, pathB}
	for pass := 0; pass < 4; pass++ {
		stats, err := db.ValueLogRewriteOnline(ctx, ValueLogRewriteOnlineOptions{
			BatchSize:            2048,
			SyncEachBatch:        false,
			MinSegmentStaleRatio: 0,
			ProtectedPaths:       protected,
		})
		if err != nil {
			close(stopCh)
			<-stopped
			t.Fatalf("rewrite pass=%d: %v", pass, err)
		}
		if stats.RecordsCopied == 0 {
			close(stopCh)
			<-stopped
			t.Fatalf("rewrite pass=%d: expected records copied, stats=%+v", pass, stats)
		}
	}

	close(stopCh)
	<-stopped

	mu.Lock()
	err = bgErr
	finalExpected := append([]uint64(nil), expectedVer...)
	mu.Unlock()
	if err != nil {
		t.Fatalf("background writer: %v", err)
	}

	for i := 0; i < keyCount; i++ {
		got, err := db.Get(concurrencyKey(i))
		if err != nil {
			t.Fatalf("get i=%d: %v", i, err)
		}
		if got == nil {
			t.Fatalf("missing key i=%d", i)
		}
		validateConcurrencyValueBytes(t, got, finalExpected[i], i, valueSize)
	}
}
