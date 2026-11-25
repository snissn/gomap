package caching

import (
	"errors"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

// NativeEntriesAtRoot performs a batched point lookup against a settled
// physical root under a single warm read lane and single snapshot read pin.
// When copyValues is true, inline values are packed into one contiguous slab
// and materialized with exact capacity bounds after the pinned read completes.
func (c *NativeSemanticCapture) NativeEntriesAtRoot(root uint64, keys [][]byte, dst []node.LeafEntry, found []bool, copyValues bool) error {
	if c == nil || c.cut == nil || c.cache == nil {
		return backenddb.ErrClosed
	}
	if len(dst) != len(keys) || len(found) != len(keys) {
		return errors.New("caching: NativeEntriesAtRoot slice length mismatch")
	}
	if len(keys) == 0 {
		return nil
	}
	return c.cut.basis.nativeEntriesAtRoot(c.cache, c.dictionary, root, keys, dst, found, copyValues)
}

func (b *cowBackendBasis) nativeEntriesAtRoot(cache *cowCache, provider cowDictionaryReadProvider, root uint64, keys [][]byte, dst []node.LeafEntry, found []bool, copyValues bool) error {
	limit := cache.budget.Limits().MaxInFlightBytes
	limits := valuelog.COWReadLimits{MaxRecordBytes: limit, MaxRawBytes: limit, MaxValueBytes: limit}
	if mu, owner, ok := b.tryLockReadLane(cowReadStripe(keys[0])); ok {
		defer mu.Unlock()
		fresh := *owner == nil
		w, err := b.workspaceLocked(owner, cache.budget, limits)
		if err != nil {
			return err
		}
		err = b.readNativeEntriesWithWorkspace(cache, w, provider, root, keys, dst, found, copyValues)
		if err != nil && fresh && *owner == w && w.point == nil && w.projectionLease == nil && len(w.codecs) == 0 && w.allocations == nil {
			*owner = nil
			w.close()
		}
		return err
	}
	// All basis read lanes are busy: admit a bounded per-call temporary
	// workspace so concurrent readers never serialize.
	w, err := admitCOWReadWorkspace(cache.budget, limits)
	if err != nil {
		return err
	}
	w.basis = b.snapshot
	defer w.close()
	return b.readNativeEntriesWithWorkspace(cache, w, provider, root, keys, dst, found, copyValues)
}

func (b *cowBackendBasis) readNativeEntriesWithWorkspace(cache *cowCache, w *cowReadWorkspace, provider cowDictionaryReadProvider, root uint64, keys [][]byte, dst []node.LeafEntry, found []bool, copyValues bool) error {
	point, err := w.pointScratch()
	if err != nil {
		return err
	}
	reader, err := b.snapshot.NativeReaderAtRoot(root)
	if err != nil {
		return err
	}
	var independent *cowCacheDictionaryRead
	if provider != nil {
		independent = &cowCacheDictionaryRead{provider: provider, cache: cache}
	}
	readLeaf := func(ptr page.LeafLogPtr, leafDst []byte, record backenddb.PinnedCOWLeafRead) ([]byte, tree.LeafLogPageReadState, error) {
		return readInspectedPhysicalLeaf(cache, w, ptr, leafDst, true, false, independent, &record)
	}

	type valueSpan struct {
		off int
		n   int
	}
	var inlineSpans [17]valueSpan
	var spans []valueSpan
	if copyValues {
		if len(keys) <= len(inlineSpans) {
			spans = inlineSpans[:len(keys)]
		} else {
			spans = make([]valueSpan, len(keys))
		}
	}
	var slab []byte

	err = reader.WithPinnedCOWPointRead(w.limits, readLeaf, func(read func(key, keyScratch, leafScratch []byte, cache *tree.FixedScratchLeafCache) (node.LeafEntry, error)) error {
		for i, key := range keys {
			dst[i] = node.LeafEntry{}
			found[i] = false
			point.leafGen++
			// Pass cache=nil and localBlock=false (above) so dictionary
			// capability, COWReadLimits inspection, and leafPageCache hit
			// accounting are never bypassed.
			entry, readErr := read(key, point.key[:], point.leaf[:], nil)
			if errors.Is(readErr, tree.ErrKeyNotFound) {
				continue
			}
			if readErr != nil {
				return readErr
			}
			// End every page/scratch alias before storing into dst.
			entry.Key = nil
			found[i] = true
			if copyValues && entry.Flags&(node.FlagPointer|node.FlagTombstone) == 0 {
				if len(entry.Value) == 0 {
					// Preserve non-nil empty slice parity with NativeEntryAtRoot.
					entry.Value = []byte{}
				} else {
					if slab == nil {
						rem := len(keys) - i
						est := len(entry.Value)
						if rem > 1 && len(entry.Value) <= 4096 {
							est = len(entry.Value) * min(rem, 32)
						}
						slab = make([]byte, 0, est)
					}
					off := len(slab)
					n := len(entry.Value)
					slab = append(slab, entry.Value...)
					entry.Value = nil
					spans[i] = valueSpan{off: off, n: n}
				}
			} else {
				entry.Value = nil
			}
			dst[i] = entry
		}
		return nil
	})
	if err != nil {
		clear(dst)
		clear(found)
		return err
	}
	if copyValues && len(slab) > 0 {
		for i, sp := range spans {
			if sp.n > 0 {
				dst[i].Value = slab[sp.off : sp.off+sp.n : sp.off+sp.n]
			}
		}
	}
	return nil
}
