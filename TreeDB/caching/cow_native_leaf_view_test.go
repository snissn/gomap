package caching

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
)

func TestNativeEntryAtRootBorrowedViewZeroReadAtAndZeroAlloc(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		t.Run(fmt.Sprintf("compressed=%t", compressed), func(t *testing.T) {
			_, backend, capture, rootID, want := cowNativeLeafProofFixture(t, 128, compressed)
			key := []byte("k000")

			// Warm verified leaf cache and page checksum proof.
			for i := 0; i < 3; i++ {
				entry, err := capture.NativeEntryAtRoot(rootID, key, true)
				if err != nil || !bytes.Equal(entry.Value, want) {
					t.Fatalf("warmup %d: got %q err %v", i, entry.Value, err)
				}
			}

			fallbacksBefore := cowOuterLeafStatCounter(t, backend, "treedb.vlog.mmap_read.fallback_readat")
			hitsBefore := cowLeafCacheCounter(t, backend, "page_checksum_verified_hits")

			dst := make([]byte, 0, len(want))
			for i := 0; i < 16; i++ {
				entry, err := capture.NativeEntryAtRootInto(rootID, key, dst)
				if err != nil || !bytes.Equal(entry.Value, want) {
					t.Fatalf("warm NativeEntryAtRootInto %d: got %q err %v", i, entry.Value, err)
				}
			}

			// Batch read via NativeEntriesAtRoot must also use the zero-pread borrowed view.
			keys := [][]byte{[]byte("k000"), []byte("k001"), []byte("k002")}
			batchDst := make([]node.LeafEntry, len(keys))
			found := make([]bool, len(keys))
			if err := capture.NativeEntriesAtRoot(rootID, keys, batchDst, found, true); err != nil {
				t.Fatalf("NativeEntriesAtRoot: %v", err)
			}
			for i := range keys {
				if !found[i] || !bytes.Equal(batchDst[i].Value, want) {
					t.Fatalf("NativeEntriesAtRoot[%d]: found=%t val=%q", i, found[i], batchDst[i].Value)
				}
			}

			// Secondary-index style SeekGE via NativeSeekLiveKeyAtRoot must also hit verified cache.
			gotKey, ok, err := capture.NativeSeekLiveKeyAtRoot(rootID, []byte("k000"), []byte("k005"))
			if err != nil || !ok || !bytes.Equal(gotKey, []byte("k000")) {
				t.Fatalf("NativeSeekLiveKeyAtRoot: key=%q ok=%t err=%v", gotKey, ok, err)
			}

			fallbacksAfter := cowOuterLeafStatCounter(t, backend, "treedb.vlog.mmap_read.fallback_readat")
			hitsAfter := cowLeafCacheCounter(t, backend, "page_checksum_verified_hits")
			if fallbacksAfter != fallbacksBefore {
				t.Fatalf("warm borrowed-view reads issued ReadAt syscalls: %d -> %d", fallbacksBefore, fallbacksAfter)
			}
			if hitsAfter <= hitsBefore {
				t.Fatalf("warm borrowed-view reads did not record verified cache hits: %d -> %d", hitsBefore, hitsAfter)
			}

			// Concurrent borrowed-view reads under -race.
			var wg sync.WaitGroup
			for g := 0; g < 8; g++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()
					localDst := make([]byte, 0, len(want))
					k := []byte(fmt.Sprintf("k%03d", id%8))
					for i := 0; i < 64; i++ {
						e, rErr := capture.NativeEntryAtRootInto(rootID, k, localDst)
						if rErr != nil || !bytes.Equal(e.Value, want) {
							t.Errorf("concurrent read %d: val=%q err=%v", id, e.Value, rErr)
							return
						}
					}
				}(g)
			}
			wg.Wait()
		})
	}
}
