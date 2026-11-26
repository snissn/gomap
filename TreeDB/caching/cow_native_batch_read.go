package caching

import (
	"errors"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

// NativeEntriesAtRootAdmitted performs a batched point lookup on a conditional
// transaction's opening physical root under one warm read lane and one snapshot
// read pin, admitting copied inline values through admitOutput before allocation.
func (tx *nativeConditionalTxn) NativeEntriesAtRootAdmitted(root uint64, keys [][]byte, dst []node.LeafEntry, found []bool, admitOutput func(uint64) error) error {
	if tx == nil || tx.ConditionalTxn == nil {
		return backenddb.ErrConditionalTxnClosed
	}
	if err := tx.ensureOpen(); err != nil {
		return err
	}
	if len(dst) != len(keys) || len(found) != len(keys) {
		return errors.New("caching: NativeEntriesAtRootAdmitted slice length mismatch")
	}
	if len(keys) == 0 {
		return nil
	}
	provider, _ := tx.db.dictStore.(cowDictionaryReadProvider)
	return tx.snap.cowCut.basis.nativeEntriesAtRootWithAdmit(tx.db.cow, provider, root, keys, dst, found, true, admitOutput)
}

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
	return c.cut.basis.nativeEntriesAtRootWithAdmit(c.cache, c.dictionary, root, keys, dst, found, copyValues, nil)
}

func (b *cowBackendBasis) nativeEntriesAtRootWithAdmit(cache *cowCache, provider cowDictionaryReadProvider, root uint64, keys [][]byte, dst []node.LeafEntry, found []bool, copyValues bool, admitOutput func(uint64) error) error {
	limit := cache.budget.Limits().MaxInFlightBytes
	limits := valuelog.COWReadLimits{MaxRecordBytes: limit, MaxRawBytes: limit, MaxValueBytes: limit}
	if mu, owner, ok := b.tryLockReadLane(cowReadStripe(keys[0])); ok {
		defer mu.Unlock()
		fresh := *owner == nil
		w, err := b.workspaceLocked(owner, cache.budget, limits)
		if err != nil {
			return err
		}
		err = b.readNativeEntriesWithWorkspaceAdmitted(cache, w, provider, root, keys, dst, found, copyValues, admitOutput)
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
	return b.readNativeEntriesWithWorkspaceAdmitted(cache, w, provider, root, keys, dst, found, copyValues, admitOutput)
}

func (b *cowBackendBasis) readNativeEntriesWithWorkspaceAdmitted(cache *cowCache, w *cowReadWorkspace, provider cowDictionaryReadProvider, root uint64, keys [][]byte, dst []node.LeafEntry, found []bool, copyValues bool, admitOutput func(uint64) error) error {
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
		return readInspectedPhysicalLeaf(cache, w, ptr, leafDst, true, true, independent, &record)
	}

	type valueSpan struct {
		off int
		n   int
	}
	var inlineSpans [17]valueSpan
	var spans []valueSpan
	if copyValues && admitOutput == nil {
		if len(keys) <= len(inlineSpans) {
			spans = inlineSpans[:len(keys)]
		} else {
			spans = make([]valueSpan, len(keys))
		}
	}
	var slab []byte

	checkShape := func(shape valuelog.COWRecordShape) bool {
		if shape.DictID != 0 && (independent == nil || independent.provider == nil || independent.cache != cache) {
			return false
		}
		if shape.Compressed && shape.DictID == 0 && shape.Codec != valuelog.BlockCodecSnappy && shape.Codec != valuelog.BlockCodecLZ4 {
			return false
		}
		return true
	}

	err = reader.WithPinnedCOWPointReadView(w.limits, checkShape, readLeaf, func(read func(key, keyScratch, leafScratch []byte, consume func(node.LeafEntry) error) error) error {
		for i, key := range keys {
			dst[i] = node.LeafEntry{}
			found[i] = false
			point.leafGen++
			readErr := read(key, point.key[:], point.leaf[:], func(entry node.LeafEntry) error {
				// End every page/scratch alias before releasing the borrowed view.
				entry.Key = nil
				found[i] = true
				if copyValues && entry.Flags&(node.FlagPointer|node.FlagTombstone) == 0 {
					if len(entry.Value) == 0 {
						// Preserve non-nil empty slice parity with NativeEntryAtRoot.
						entry.Value = []byte{}
					} else if admitOutput != nil {
						n := len(entry.Value)
						if cap(slab)-len(slab) < n {
							rem := len(keys) - i
							est := n
							if rem > 1 && n <= 4096 {
								est = n * min(rem, 32)
							}
							if admitErr := admitOutput(memtable.COWAllocationCharge(uint64(est))); admitErr != nil {
								if est == n || !errors.Is(admitErr, memtable.ErrCOWCapacity) {
									return admitErr
								}
								est = n
								if admitErr = admitOutput(memtable.COWAllocationCharge(uint64(est))); admitErr != nil {
									return admitErr
								}
							}
							slab = make([]byte, 0, est)
						}
						off := len(slab)
						slab = append(slab, entry.Value...)
						entry.Value = slab[off : off+n : off+n]
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
				return nil
			})
			if errors.Is(readErr, tree.ErrKeyNotFound) {
				continue
			}
			if readErr != nil {
				return readErr
			}
		}
		return nil
	})
	if err != nil {
		clear(dst)
		clear(found)
		return err
	}
	if copyValues && admitOutput == nil && len(slab) > 0 {
		for i, sp := range spans {
			if sp.n > 0 {
				dst[i].Value = slab[sp.off : sp.off+sp.n : sp.off+sp.n]
			}
		}
	}
	return nil
}
