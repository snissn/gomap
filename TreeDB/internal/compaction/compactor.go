package compaction

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sort"
	"sync"

	"treedb/internal/crc"
	"treedb/internal/mvcc"
	"treedb/internal/page"
	"treedb/internal/pager"
	"treedb/internal/slab"
	"treedb/internal/tree"
)

const (
	deadRatioThreshold   = 0.5
	defaultMicroBatch    = 100
	defaultMaxCopyBPS    = int64(32 << 20) // 32MB/s
	recordHeaderLenBytes = 10             // CRC32C + KeyLen + ValueLen
)

// Update represents a pointer move from a cold slab to a target slab.
type Update struct {
	Key []byte
	Old page.ValuePtr
	New page.ValuePtr
}

// Hooks allow tests to observe phases of compaction.
type Hooks struct {
	AfterCopy func(coldID uint32, updates []Update)
}

// Compactor runs Move-and-Micro-Batch slab compaction.
type Compactor struct {
	pager    *pager.Pager
	slabs    *slab.SlabManager
	state    *mvcc.StateHolder
	grave    *mvcc.Graveyard
	pruner   *mvcc.Pruner
	writerMu *sync.Mutex

	maxCopyBPS      int64
	microBatchSize int
	hooks          *Hooks
}

// New constructs a Compactor.
func New(p *pager.Pager, slabsMgr *slab.SlabManager, st *mvcc.StateHolder, grave *mvcc.Graveyard, pruner *mvcc.Pruner, writerMu *sync.Mutex) *Compactor {
	return &Compactor{
		pager:           p,
		slabs:           slabsMgr,
		state:           st,
		grave:           grave,
		pruner:          pruner,
		writerMu:        writerMu,
		maxCopyBPS:      defaultMaxCopyBPS,
		microBatchSize: defaultMicroBatch,
	}
}

// WithHooks sets optional hooks and returns c.
func (c *Compactor) WithHooks(h *Hooks) *Compactor {
	c.hooks = h
	return c
}

// CompactAll runs a full blocking compaction cycle.
func (c *Compactor) CompactAll() error {
	if c == nil || c.pager == nil || c.slabs == nil || c.state == nil {
		return nil
	}
	// Hold a snapshot while selecting candidates to prevent page reuse via
	// pruning from racing with the scan under concurrent commits.
	snap, err := c.state.AcquireSnapshot()
	if err != nil {
		return err
	}
	st := snap.State()
	if st == nil {
		_ = snap.Close()
		return fmt.Errorf("compaction: no state")
	}

	candidates, err := c.selectCandidates(st.SystemRootPageID, st.SlabSet)
	_ = snap.Close()
	if err != nil {
		return err
	}
	if len(candidates) == 0 {
		return nil
	}

	// Obtain a fresh target slab by rotating active once.
	if c.writerMu != nil {
		c.writerMu.Lock()
	}
	if _, err := c.slabs.ForceRotate(); err != nil {
		if c.writerMu != nil {
			c.writerMu.Unlock()
		}
		return err
	}
	if c.writerMu != nil {
		c.writerMu.Unlock()
	}

	for _, id := range candidates {
		if err := c.compactOne(id); err != nil {
			return err
		}
	}
	return nil
}

func (c *Compactor) selectCandidates(systemRoot page.PageID, set *slab.SlabSet) ([]uint32, error) {
	if systemRoot == 0 {
		return nil, nil
	}
	activeID := c.slabs.ActiveID()
	var ids []uint32
	ratios := make(map[uint32]float64)
	err := walkTree(c.pager, systemRoot, func(key []byte, flags page.LeafFlags, inline []byte, _ page.ValuePtr) error {
		fileID, ok := slab.ParseStatsKey(key)
		if !ok {
			return nil
		}
		if fileID == activeID {
			return nil
		}
		if set != nil {
			if _, ok := set.Get(fileID); !ok {
				return nil
			}
		}
		if flags != page.LeafFlagInline || inline == nil {
			return nil
		}
		stats, err := slab.DecodeStatsValue(inline)
		if err != nil {
			return err
		}
		if stats.TotalBytes == 0 {
			return nil
		}
		ratio := float64(stats.DeadBytes) / float64(stats.TotalBytes)
		if ratio > deadRatioThreshold {
			ids = append(ids, fileID)
			ratios[fileID] = ratio
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(ids, func(i, j int) bool {
		ri, rj := ratios[ids[i]], ratios[ids[j]]
		if ri == rj {
			return ids[i] < ids[j]
		}
		return ri > rj
	})
	return ids, nil
}

func (c *Compactor) compactOne(coldID uint32) error {
	snap, err := c.state.AcquireSnapshot()
	if err != nil {
		return err
	}
	defer snap.Close()
	st := snap.State()
	if st == nil || st.SlabSet == nil {
		return nil
	}
	cold, ok := st.SlabSet.Get(coldID)
	if !ok || cold == nil || cold.Handle == nil {
		return nil
	}

	info, err := cold.Handle.Stat()
	if err != nil {
		return err
	}
	size := uint64(info.Size())

	ut := tree.NewUserTree(c.pager, st.UserRootPageID)
	lim := newLimiter(c.maxCopyBPS)

	var updates []Update
	var off uint64
	for off < size {
		hdr := make([]byte, recordHeaderLenBytes)
		n, rerr := cold.Handle.ReadAt(hdr, int64(off))
		if rerr != nil && !errors.Is(rerr, io.EOF) && !errors.Is(rerr, io.ErrUnexpectedEOF) {
			return rerr
		}
		if n == 0 && (errors.Is(rerr, io.EOF) || errors.Is(rerr, io.ErrUnexpectedEOF)) {
			break
		}
		if n < recordHeaderLenBytes {
			return slab.ErrRecordTruncated
		}

		wantCRC := binary.LittleEndian.Uint32(hdr[0:4])
		keyLen := binary.LittleEndian.Uint16(hdr[4:6])
		valLen := binary.LittleEndian.Uint32(hdr[6:10])
		protectedLen := uint32(2 + 4 + int(keyLen) + int(valLen))
		recordLen := uint64(4 + protectedLen)
		if recordLen == 0 || off+recordLen > size {
			return slab.ErrRecordTruncated
		}

		rec := make([]byte, recordLen)
		if _, err := cold.Handle.ReadAt(rec, int64(off)); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return slab.ErrRecordTruncated
			}
			return err
		}
		if crc.Checksum(rec[4:]) != wantCRC {
			return fmt.Errorf("compaction: slab %d record crc mismatch at %d", coldID, off)
		}

		keyStart := 10
		keyEnd := keyStart + int(keyLen)
		valStart := keyEnd
		valEnd := valStart + int(valLen)
		if valEnd > len(rec) || keyEnd > valEnd {
			return slab.ErrRecordCorrupt
		}
		key := append([]byte(nil), rec[keyStart:keyEnd]...)
		val := append([]byte(nil), rec[valStart:valEnd]...)

		oldPtr := page.ValuePtr{
			Offset: off + 4,
			Length: protectedLen,
			FileID: coldID,
		}

		// Dead-hint extension point: could consult a hint map here.

		ent, err := ut.GetRaw(key)
		if err == nil && ent.Flags == page.LeafFlagPointer && ent.Ptr == oldPtr {
			newPtr, err := c.slabs.AppendLarge(key, val)
			if err != nil {
				return err
			}
			updates = append(updates, Update{Key: key, Old: oldPtr, New: newPtr})
		}

		lim.Wait(int(recordLen))
		off += recordLen
	}

	if c.hooks != nil && c.hooks.AfterCopy != nil {
		c.hooks.AfterCopy(coldID, updates)
	}

	if len(updates) > 0 {
		if err := c.syncActiveSlab(); err != nil {
			return err
		}
		if err := c.applyMicroBatches(updates); err != nil {
			return err
		}
	}

	return c.zombieTransition(coldID)
}

func (c *Compactor) syncActiveSlab() error {
	set := c.slabs.SlabSet()
	if set == nil {
		return nil
	}
	activeID := c.slabs.ActiveID()
	f, ok := set.Get(activeID)
	if !ok || f == nil || f.Handle == nil {
		return nil
	}
	return f.Handle.Sync()
}

func (c *Compactor) applyMicroBatches(updates []Update) error {
	if len(updates) == 0 {
		return nil
	}
	bs := c.microBatchSize
	if bs <= 0 {
		bs = defaultMicroBatch
	}
	for i := 0; i < len(updates); i += bs {
		j := i + bs
		if j > len(updates) {
			j = len(updates)
		}
		if err := c.applyBatch(updates[i:j]); err != nil {
			return err
		}
		runtime.Gosched()
	}
	return nil
}

func (c *Compactor) applyBatch(batch []Update) error {
	if c.writerMu != nil {
		c.writerMu.Lock()
		defer c.writerMu.Unlock()
	}
	st := c.state.Load()
	if st == nil {
		return fmt.Errorf("compaction: no state")
	}

	userTree := tree.NewUserTree(c.pager, st.UserRootPageID)
	systemTree := tree.NewSystemTree(c.pager, st.SystemRootPageID)

	var retired []page.PageID
	applied := false
	for _, u := range batch {
		ent, err := userTree.GetRaw(u.Key)
		if err != nil || ent.Flags != page.LeafFlagPointer || ent.Ptr != u.Old {
			continue
		}
			ids, _, err := userTree.SetRaw(u.Key, tree.LeafEntry{
				Flags: page.LeafFlagPointer,
				Ptr:   u.New,
			})
		if err != nil {
			return err
		}
		retired = append(retired, ids...)
		applied = true
	}
	if !applied {
		return nil
	}

	newSeq := st.CommitSeq + 1
	meta := c.pager.ReadActiveMeta()
	meta.CommitSeq = newSeq
	meta.UserRootPageID = userTree.Root()
	meta.SystemRootPageID = systemTree.Root()
	meta.ActiveSlabID = c.slabs.ActiveID()
	meta.ActiveSlabTail = c.slabs.ActiveTail()

	metaPid := page.PageID(newSeq % 2)
	metaBuf, err := encodeMetaPage(metaPid, meta)
	if err != nil {
		return err
	}
	if err := c.pager.WritePage(metaPid, metaBuf); err != nil {
		return err
	}

	c.grave.Record(newSeq, retired)
	c.state.Publish(&mvcc.DBState{
		CommitSeq:        newSeq,
		UserRootPageID:   userTree.Root(),
		SystemRootPageID: systemTree.Root(),
		SlabSet:          c.slabs.SlabSet(),
	})
	_ = c.pruner.Prune(newSeq)
	return nil
}

func (c *Compactor) zombieTransition(coldID uint32) error {
	var removed *slab.SlabFile
	if c.writerMu != nil {
		c.writerMu.Lock()
		defer c.writerMu.Unlock()
	}
	st := c.state.Load()
	if st == nil {
		return fmt.Errorf("compaction: no state")
	}

	userTree := tree.NewUserTree(c.pager, st.UserRootPageID)
	systemTree := tree.NewSystemTree(c.pager, st.SystemRootPageID)

	var retired []page.PageID

	// Tombstone cold slab stats key if present.
		if ids, _, err := systemTree.SetRaw(slab.StatsKey(coldID), tree.LeafEntry{
			Flags: page.LeafFlagTombstone,
		}); err == nil {
		retired = append(retired, ids...)
	} else {
		return err
	}

	// Persist stats for active target slab.
	activeID := c.slabs.ActiveID()
	if set := c.slabs.SlabSet(); set != nil {
		if f, ok := set.Get(activeID); ok && f != nil {
			statsVal := slab.EncodeStatsValue(f.Stats())
				ids, _, err := systemTree.SetRaw(slab.StatsKey(activeID), tree.LeafEntry{
					Flags:       page.LeafFlagInline,
					InlineValue: statsVal,
				})
			if err != nil {
				return err
			}
			retired = append(retired, ids...)
		}
	}

	var err error
	removed, err = c.slabs.RemoveFromSet(coldID)
	if err != nil {
		return err
	}

	newSeq := st.CommitSeq + 1
	meta := c.pager.ReadActiveMeta()
	meta.CommitSeq = newSeq
	meta.UserRootPageID = userTree.Root()
	meta.SystemRootPageID = systemTree.Root()
	meta.ActiveSlabID = c.slabs.ActiveID()
	meta.ActiveSlabTail = c.slabs.ActiveTail()

	metaPid := page.PageID(newSeq % 2)
	metaBuf, err := encodeMetaPage(metaPid, meta)
	if err != nil {
		return err
	}
	if err := c.pager.WritePage(metaPid, metaBuf); err != nil {
		return err
	}

	if err := c.syncActiveSlab(); err != nil {
		return err
	}
	if err := c.pager.SyncIndex(); err != nil {
		return err
	}

	c.grave.Record(newSeq, retired)
	c.state.Publish(&mvcc.DBState{
		CommitSeq:        newSeq,
		UserRootPageID:   userTree.Root(),
		SystemRootPageID: systemTree.Root(),
		SlabSet:          c.slabs.SlabSet(),
	})
	_ = c.pruner.Prune(newSeq)

	if removed != nil {
		_ = removed.MarkZombie()
	}
	return nil
}

// walkTree performs an in-order traversal of a B+Tree rooted at pid.
func walkTree(p *pager.Pager, pid page.PageID, visit func(key []byte, flags page.LeafFlags, inline []byte, ptr page.ValuePtr) error) error {
	if pid == 0 {
		return nil
	}
	buf, err := p.ReadPage(pid)
	if err != nil {
		return err
	}
	h, body, err := page.SplitPage(buf)
	if err != nil {
		return err
	}
	if err := h.VerifyBodyCRC(body); err != nil {
		return err
	}
	switch h.Flags {
	case page.PageTypeLeaf:
		lp, err := page.OpenLeafPage(buf)
		if err != nil {
			return err
		}
		for i := 0; i < lp.Count(); i++ {
			key, flags, inline, ptr, err := lp.EntryAt(i)
			if err != nil {
				return err
			}
			if err := visit(key, flags, inline, ptr); err != nil {
				return err
			}
		}
		return nil
	case page.PageTypeInternal:
		ip, err := page.OpenInternalPage(buf)
		if err != nil {
			return err
		}
		for i := 0; i < ip.Count(); i++ {
			_, child, err := ip.EntryAt(i)
			if err != nil {
				return err
			}
			if err := walkTree(p, child, visit); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("compaction: unexpected page type %d", h.Flags)
	}
}
