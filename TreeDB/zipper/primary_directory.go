package zipper

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"sort"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// SetPrimaryDirectory enables independently complete directory output for new
// materialized roots. An existing directory remains authoritative on reopen
// regardless of this construction preference.
func (z *Zipper) SetPrimaryDirectory(enabled bool) { z.primaryDirectory = enabled }

// SetPrimaryArena selects the same independently owned component namespace for
// ordinary primary output and native publication. The caller first attaches its
// pager and owns its lifetime; DATA allocation remains materialized-base only.
func (z *Zipper) SetPrimaryArena(a *primaryarena.Arena) {
	z.primaryArena = a
	if a != nil {
		z.primaryDirectory = true
	}
}
func (z *Zipper) PrimaryArena() *primaryarena.Arena { return z.primaryArena }
func (z *Zipper) SetPrimaryConstructorV6(f func(*primaryarena.Arena, *iterator.OrdinalScanWork) (primaryarena.ReadRootConstructionV6, bool, error)) {
	z.primaryConstructorV6 = f
}
func claimPrimaryComponent(a *primaryarena.Arena, w *iterator.OrdinalScanWork) (primaryarena.Ref, error) {
	c, ok, e := a.PrepareClaim(primaryarena.Component, w)
	if !ok || e != nil {
		return primaryarena.Ref{}, e
	}
	for {
		r, done, e := c.Step(w)
		if e != nil {
			return primaryarena.Ref{}, e
		}
		if done {
			return r, nil
		}
	}
}
func primaryDataRetired(ids []uint64) []uint64 {
	out := ids[:0]
	for _, id := range ids {
		if !primaryarena.IsPage(id) {
			out = append(out, id)
		}
	}
	return out
}

func primaryEntriesFit(entries []node.PrimaryDirectoryEntry) bool {
	if len(entries) > node.PrimaryDirectoryMaxEntries {
		return false
	}
	used := node.PrimaryDirectoryHeapOffset
	for _, e := range entries {
		if len(e.Key) > page.PageSize-used {
			return false
		}
		used += len(e.Key)
	}
	return true
}

func (z *Zipper) loadPrimaryOperand(op node.PrimaryOperand, entry *node.PrimaryDirectoryEntry, metrics *adaptive.Metrics, scratch *mergeScratch) (node.Node, error) {
	n, _, buf, pooled, source, err := z.loadNodeRef(op.Ref, scratch)
	if err != nil {
		return node.Node{}, err
	}
	if pooled {
		defer releaseLeafPageScratch(scratch, buf)
	}
	recordZipperNodeLoad(metrics, op.Ref, n, source)
	// Detached component views must outlive a read/decode scratch loan.
	if pooled {
		n = *node.NewNode(bytes.Clone(n.Data()))
	}
	if !node.VerifyPrimaryOperand(op, n.Data()) {
		return node.Node{}, node.ErrPrimaryDirectory
	}
	if entry != nil {
		if err = node.ValidatePrimaryComponent(*entry, n.Data()); err != nil {
			return node.Node{}, err
		}
	} else if n.Type() != page.PageTypeLeaf && n.Type() != page.PageTypeInternal {
		return node.Node{}, node.ErrPrimaryDirectory
	}
	return n, nil
}

func (z *Zipper) writePrimaryDirectory(base node.PrimaryOperand, baseSeq uint64, entries []node.PrimaryDirectoryEntry, metrics *adaptive.Metrics, owners ...primaryarena.ReadRootConstructionV6) (uint64, error) {
	// Compaction removes classes; only that genuine construction reindexes them.
	var seen [node.PrimaryDirectoryMaxEntries]bool
	valid := true
	for _, e := range entries {
		if int(e.ClassSlot) >= len(entries) || seen[e.ClassSlot] {
			valid = false
			break
		}
		seen[e.ClassSlot] = true
	}
	if !valid {
		for i := range entries {
			entries[i].ClassSlot = uint16(i)
		}
	}
	if z.primaryArena != nil {
		return z.writePrimaryArenaDirectory(base, baseSeq, entries, metrics, owners...)
	}
	id, err := z.allocator.Alloc(0)
	if err != nil {
		return 0, err
	}
	image, err := z.pager.GetForWrite(id)
	if err != nil {
		return 0, err
	}
	if err = node.EncodePrimaryDirectory(image, id, baseSeq, base, entries); err != nil {
		return 0, err
	}
	metrics.PrimaryDirectoryPagesWritten++
	metrics.PrimaryBytesWritten += page.PageSize
	return id, nil
}

func (z *Zipper) primaryMaterialize(base node.PrimaryOperand, baseSeq uint64, entries []node.PrimaryDirectoryEntry, count int, cfg applyRunConfig, metrics *adaptive.Metrics, scratch *mergeScratch) (node.PrimaryOperand, uint64, []uint64, error) {
	if count == 0 {
		return base, baseSeq, nil, nil
	}
	if count > len(entries) {
		count = len(entries)
	}
	delta := batch.New(nil, page.PageSize)
	defer delta.Close()
	for i := 0; i < count; i++ {
		e := entries[i]
		if e.InlineAbsence() {
			if err := delta.SetOps([]batch.Entry{{Type: batch.OpDelete, Key: bytes.Clone(e.Key), Revision: e.Revision}}); err != nil {
				return node.PrimaryOperand{}, 0, nil, err
			}
			continue
		}
		n, err := z.loadPrimaryOperand(e.Operand, &e, metrics, scratch)
		if err != nil {
			return node.PrimaryOperand{}, 0, nil, err
		}
		k, v, ptr, flags, rev, err := n.GetLeafEntryViewWithRevision(0)
		if err != nil {
			return node.PrimaryOperand{}, 0, nil, err
		}
		op := batch.Entry{Type: batch.OpPut, Key: bytes.Clone(k), Value: bytes.Clone(v), ValuePtr: ptr, IsPtr: flags&node.FlagPointer != 0, Revision: rev}
		if flags&node.FlagTombstone != 0 {
			op.Type = batch.OpDelete
		}
		if err = delta.SetOps([]batch.Entry{op}); err != nil {
			return node.PrimaryOperand{}, 0, nil, err
		}
	}
	materialized := cfg
	materialized.materializedBase = true
	// Component pointers already counted in the old complete primary move into
	// the base once. Only displaced physical base pointers disappear.
	materialized.oldPointerRefs = cfg.oldPointerRefs
	materialized.oldEntriesRemoved = cfg.oldEntriesRemoved
	root, retired, work, err := z.applyWithConfig(base.Ref.Page, delta, materialized)
	mergeMetrics(metrics, &work)
	if err != nil {
		return node.PrimaryOperand{}, 0, nil, err
	}
	for i := 0; i < count; i++ {
		if entries[i].Operand.Ref.Kind == page.ChildRefPage {
			if !primaryarena.IsPage(entries[i].Operand.Ref.Page) {
				retired = append(retired, entries[i].Operand.Ref.Page)
			}
		}
	}
	image, err := z.pager.Get(root)
	if err != nil {
		return node.PrimaryOperand{}, 0, nil, err
	}
	metrics.PrimaryConsolidatedCells += uint64(count)
	// This sequence is the immutable materialized-base generation, not global
	// publication or allocator activation. Only actual materialization advances it.
	if baseSeq == ^uint64(0) {
		return node.PrimaryOperand{}, 0, nil, errors.New("primary base sequence overflow")
	}
	return node.PrimaryOperand{Ref: page.PageChildRef(root), Digest: sha256.Sum256(image)}, baseSeq + 1, retired, nil
}

// ConsolidatePrimaryDirectory is explicit caller-owned bounded progress. It
// does not borrow a pending prune, synthesize its result, or publish a root.
// The caller owns root acceptance/retention and must charge returned work.
func (z *Zipper) ConsolidatePrimaryDirectory(rootID uint64, maxCells int) (uint64, []uint64, adaptive.Metrics, error) {
	var metrics adaptive.Metrics
	if maxCells < 1 || maxCells > node.PrimaryDirectoryMaxEntries {
		return 0, nil, metrics, node.ErrPrimaryDirectory
	}
	image, err := z.pager.Get(rootID)
	if err != nil {
		return 0, nil, metrics, err
	}
	directory, err := node.DecodePrimaryDirectory(image)
	if err != nil {
		return 0, nil, metrics, err
	}
	metrics.PrimaryDirectoryPagesRead++
	entries := make([]node.PrimaryDirectoryEntry, directory.Count())
	for i := range entries {
		entries[i], _ = directory.Entry(i)
	}
	if len(entries) == 0 {
		return rootID, nil, metrics, nil
	}
	scratch := z.acquireApplyScratch()
	defer z.releaseApplyScratch(scratch)
	base, seq := directory.Base()
	if _, err = z.loadPrimaryOperand(base, nil, &metrics, scratch); err != nil {
		return 0, nil, metrics, err
	}
	count := min(maxCells, len(entries))
	base, seq, retired, err := z.primaryMaterialize(base, seq, entries, count, applyRunConfig{}, &metrics, scratch)
	if err != nil {
		return 0, nil, metrics, err
	}
	root, err := z.writePrimaryDirectory(base, seq, entries[count:], &metrics)
	if err != nil {
		return 0, nil, metrics, err
	}
	if !primaryarena.IsPage(rootID) {
		retired = append(retired, rootID)
	}
	return root, retired, metrics, nil
}

func (z *Zipper) applyPrimaryDirectory(rootID uint64, ops []batch.Entry, ranges []batch.DeleteRange, cfg applyRunConfig) (root uint64, retired []uint64, metrics adaptive.Metrics, err error) {
	// Ordinary admission charges its own allocator-retirement progress. It never
	// sees the independent queues retained by native Accepted owners.
	if z.primaryArena != nil {
		work := &iterator.OrdinalScanWork{RecordLimit: ^uint64(0), ByteLimit: ^uint64(0)}
		_, err = z.primaryArena.ProgressOrdinaryRelease(32, work)
		metrics.PrimaryBankRecords += work.Records
		metrics.PrimaryBankWorkBytes += work.Bytes
		if err != nil {
			return 0, nil, metrics, err
		}
	}
	var constructor primaryarena.ReadRootConstructionV6
	if z.primaryArena != nil && z.primaryArena.CapsuleFormatV6() && z.primaryConstructorV6 != nil {
		work := &iterator.OrdinalScanWork{RecordLimit: ^uint64(0), ByteLimit: ^uint64(0)}
		var ready bool
		constructor, ready, err = z.primaryConstructorV6(z.primaryArena, work)
		metrics.PrimaryBankRecords += work.Records
		metrics.PrimaryBankWorkBytes += work.Bytes
		if !ready || err != nil {
			return 0, nil, metrics, err
		}
		defer func() {
			if err != nil {
				_ = constructor.Abort()
			}
		}()
	}
	scratch := z.acquireApplyScratch()
	defer z.releaseApplyScratch(scratch)
	image, err := z.pager.Get(rootID)
	if err != nil {
		return 0, nil, metrics, err
	}
	base := node.PrimaryOperand{Ref: page.PageChildRef(rootID), Digest: sha256.Sum256(image)}
	baseSeq := uint64(1)
	var entries []node.PrimaryDirectoryEntry
	var privateComponents []primaryarena.Ref
	defer func() {
		work := &iterator.OrdinalScanWork{RecordLimit: ^uint64(0), ByteLimit: ^uint64(0)}
		for _, r := range privateComponents {
			_, _ = z.primaryArena.Drop(r, work)
		}
		metrics.PrimaryBankRecords += work.Records
		metrics.PrimaryBankWorkBytes += work.Bytes
	}()
	if node.NewNode(image).Type() == page.PageTypePrimaryDirectory {
		d, e := node.DecodePrimaryDirectory(image)
		if e != nil {
			return 0, nil, metrics, e
		}
		base, baseSeq = d.Base()
		metrics.PrimaryDirectoryPagesRead++
		entries = make([]node.PrimaryDirectoryEntry, d.Count())
		for i := range entries {
			entries[i], _ = d.Entry(i)
		}
		if !primaryarena.IsPage(rootID) {
			retired = append(retired, rootID)
		}
	}
	if _, err = z.loadPrimaryOperand(base, nil, &metrics, scratch); err != nil {
		return 0, nil, metrics, err
	}
	// Large ordinary batches/ranges and keys exceeding directory capacity use
	// the genuine materialized-base path. All consolidation and COW work remains
	// in returned metrics and in the same allocator/retirement packet.
	candidate := append([]node.PrimaryDirectoryEntry(nil), entries...)
	for _, op := range ops {
		i := sort.Search(len(candidate), func(i int) bool { return bytes.Compare(candidate[i].Key, op.Key) >= 0 })
		if i < len(candidate) && bytes.Equal(candidate[i].Key, op.Key) {
			candidate[i].Key = op.Key
		} else {
			candidate = append(candidate, node.PrimaryDirectoryEntry{})
			copy(candidate[i+1:], candidate[i:])
			candidate[i] = node.PrimaryDirectoryEntry{Key: op.Key}
		}
	}
	if len(ranges) > 0 || !primaryEntriesFit(candidate) {
		var reclaimed []uint64
		base, baseSeq, reclaimed, err = z.primaryMaterialize(base, baseSeq, entries, len(entries), cfg, &metrics, scratch)
		if err != nil {
			return 0, nil, metrics, err
		}
		retired = append(retired, reclaimed...)
		entries = nil
		if len(ranges) > 0 || !primaryEntriesFitKeys(ops) {
			delta := batch.New(nil, page.PageSize)
			defer delta.Close()
			if err = delta.SetOps(ops); err != nil {
				return 0, nil, metrics, err
			}
			for _, r := range ranges {
				if err = delta.DeleteRange(r.Start, r.End); err != nil {
					return 0, nil, metrics, err
				}
			}
			materialized := cfg
			materialized.materializedBase = true
			newBase, old, work, e := z.applyWithConfig(base.Ref.Page, delta, materialized)
			mergeMetrics(&metrics, &work)
			if e != nil {
				return 0, nil, metrics, e
			}
			retired = append(retired, old...)
			data, e := z.pager.Get(newBase)
			if e != nil {
				return 0, nil, metrics, e
			}
			base = node.PrimaryOperand{Ref: page.PageChildRef(newBase), Digest: sha256.Sum256(data)}
			root, e := z.writePrimaryDirectory(base, baseSeq+1, nil, &metrics, constructor)
			return root, retired, metrics, e
		}
	}
	for _, op := range ops {
		classSlot := uint16(len(entries))
		i := sort.Search(len(entries), func(i int) bool { return bytes.Compare(entries[i].Key, op.Key) >= 0 })
		if i < len(entries) && bytes.Equal(entries[i].Key, op.Key) {
			old := entries[i]
			classSlot = old.ClassSlot
			if !old.InlineAbsence() {
				n, e := z.loadPrimaryOperand(old.Operand, &old, &metrics, scratch)
				if e != nil {
					return 0, nil, metrics, e
				}
				if cfg.oldPointerRefs != nil {
					_, _, ptr, flags, _, e := n.GetLeafEntryViewWithRevision(0)
					if e != nil {
						return 0, nil, metrics, e
					}
					if flags&node.FlagPointer != 0 {
						cfg.oldPointerRefs.add(ptr.FileID, 1)
					}
				}
			}
			if cfg.oldEntriesRemoved != nil {
				*cfg.oldEntriesRemoved++
			}
			if old.Operand.Ref.Kind == page.ChildRefPage {
				if !primaryarena.IsPage(old.Operand.Ref.Page) {
					retired = append(retired, old.Operand.Ref.Page)
				}
			}
		} else {
			// Existing base cells remain physically retained, but removed logical
			// pointer counts still use the exact selected pre-mutation cell.
			if cfg.oldPointerRefs != nil || cfg.oldEntriesRemoved != nil {
				if e := z.collectPrimaryBaseOld(base.Ref, op.Key, cfg, &metrics, scratch); e != nil {
					return 0, nil, metrics, e
				}
			}
			entries = append(entries, node.PrimaryDirectoryEntry{})
			copy(entries[i+1:], entries[i:])
		}
		if op.Type == batch.OpDelete {
			if op.Revision == 0 {
				return 0, nil, metrics, node.ErrPrimaryDirectory
			}
			entries[i] = node.PrimaryDirectoryEntry{Key: op.Key, Revision: op.Revision, Kind: node.PrimaryAbsence, ClassSlot: classSlot}
			continue
		}
		var id uint64
		var data []byte
		var component primaryarena.Ref
		var e error
		bankWork := &iterator.OrdinalScanWork{RecordLimit: ^uint64(0), ByteLimit: ^uint64(0)}
		if z.primaryArena != nil {
			component, e = claimPrimaryComponent(z.primaryArena, bankWork)
			if e == nil {
				id = component.PageID
				if constructor != nil {
					ready, ownErr := constructor.OwnPrimaryComponentV6(component, bankWork)
					if !ready || ownErr != nil {
						_, _ = z.primaryArena.Drop(component, bankWork)
						return 0, nil, metrics, ownErr
					}
				} else {
					privateComponents = append(privateComponents, component)
				}
				data = make([]byte, page.PageSize)
			}
		} else {
			id, e = z.allocator.Alloc(0)
			if e == nil {
				data, e = z.pager.GetForWrite(id)
			}
		}
		if e != nil {
			return 0, nil, metrics, e
		}
		builder := node.NewBuilderWithOptions(data, page.PageTypeLeaf, node.BuilderOptions{EntryRevisions: true})
		builder.SetPageID(id)
		flags := byte(0)
		kind := node.PrimaryPut
		if op.IsPtr {
			flags = node.FlagPointer
		}
		if op.Type == batch.OpDelete {
			flags = node.FlagTombstone
			kind = node.PrimaryAbsence
		}
		revision := op.Revision
		if revision == 0 {
			return 0, nil, metrics, node.ErrPrimaryDirectory
		}
		if e = builder.AddLeafEntryWithRevision(op.Key, op.Value, flags, op.ValuePtr, revision); e != nil {
			return 0, nil, metrics, e
		}
		builder.FinishNoNode()
		if z.primaryArena != nil {
			if ok, e := z.primaryArena.SealComponent(component, data, bankWork); !ok || e != nil {
				return 0, nil, metrics, e
			}
		}
		metrics.PrimaryBankRecords += bankWork.Records
		metrics.PrimaryBankWorkBytes += bankWork.Bytes
		entries[i] = node.PrimaryDirectoryEntry{Key: op.Key, Operand: node.PrimaryOperand{Ref: page.PageChildRef(id), Digest: sha256.Sum256(data)}, Revision: revision, Kind: kind, ClassSlot: classSlot}
		metrics.PrimaryComponentPagesWritten++
		metrics.PrimaryBytesWritten += page.PageSize
	}
	metrics.ZipperApplyOps = len(ops) + len(ranges)
	root, err = z.writePrimaryDirectory(base, baseSeq, entries, &metrics, constructor)
	return root, retired, metrics, err
}
func primaryEntriesFitKeys(ops []batch.Entry) bool {
	entries := make([]node.PrimaryDirectoryEntry, len(ops))
	for i := range ops {
		entries[i].Key = ops[i].Key
	}
	return primaryEntriesFit(entries)
}
func (z *Zipper) collectPrimaryBaseOld(ref page.ChildRef, key []byte, cfg applyRunConfig, metrics *adaptive.Metrics, scratch *mergeScratch) error {
	for depth := 0; depth < 50; depth++ {
		n, _, buf, pooled, source, err := z.loadNodeRef(ref, scratch)
		if err != nil {
			return err
		}
		recordZipperNodeLoad(metrics, ref, n, source)
		if pooled {
			defer releaseLeafPageScratch(scratch, buf)
		}
		if n.Type() == page.PageTypeInternal {
			next, _, e := n.SearchInternalChildRef(key)
			if e != nil {
				return e
			}
			ref = next
			continue
		}
		if n.Type() != page.PageTypeLeaf {
			return node.ErrPrimaryDirectory
		}
		i, found, e := n.SearchLeaf(key)
		if e != nil {
			return e
		}
		if !found {
			return nil
		}
		_, _, ptr, flags, _, e := n.GetLeafEntryViewWithRevision(i)
		if e != nil {
			return e
		}
		// An overlay leaves this base cell physically reachable. Its value-log
		// dependency remains live until a charged materialization replaces it.
		_ = ptr
		_ = flags
		if cfg.oldEntriesRemoved != nil {
			*cfg.oldEntriesRemoved++
		}
		return nil
	}
	return node.ErrPrimaryDirectory
}

func (z *Zipper) writePrimaryArenaDirectory(base node.PrimaryOperand, baseSeq uint64, entries []node.PrimaryDirectoryEntry, metrics *adaptive.Metrics, owners ...primaryarena.ReadRootConstructionV6) (uint64, error) {
	a := z.primaryArena
	work := &iterator.OrdinalScanWork{RecordLimit: ^uint64(0), ByteLimit: ^uint64(0)}
	defer func() { metrics.PrimaryBankRecords += work.Records; metrics.PrimaryBankWorkBytes += work.Bytes }()
	var bundle primaryarena.PublicationBundle
	var constructor primaryarena.ReadRootConstructionV6
	var ok bool
	var e error
	if a.CapsuleFormatV6() {
		if len(owners) > 0 && owners[0] != nil {
			constructor, ok = owners[0], true
			bundle.Directory = constructor.PrimaryConstructionReferenceV6()
		} else if z.primaryConstructorV6 != nil {
			constructor, ok, e = z.primaryConstructorV6(a, work)
			if ok && e == nil {
				bundle.Directory = constructor.PrimaryConstructionReferenceV6()
			}
		} else {
			bundle.Directory, ok, e = a.PrepareReadRootV6(work)
		}
		if !ok || e != nil {
			return 0, e
		}
	} else {
		claim, ready, err := a.PrepareBundleClaim(work)
		if !ready || err != nil {
			return 0, err
		}
		for {
			bundle, ok, e = claim.Step(work)
			if e != nil {
				return 0, e
			}
			if ok {
				break
			}
		}
	}
	success := false
	defer func() {
		if !success {
			if constructor != nil {
				_ = constructor.Abort()
				return
			}
			if bundle.Record != (primaryarena.Ref{}) {
				_, _ = a.Drop(bundle.Record, work)
			}
			_, _ = a.Drop(bundle.Directory, work)
		}
	}()
	var refs [node.PrimaryDirectoryMaxEntries]primaryarena.Ref
	for _, entry := range entries {
		if entry.InlineAbsence() {
			continue
		}
		r, ok, e := a.BorrowComponent(entry.Operand.Ref.Page, work)
		if !ok || e != nil {
			return 0, e
		}
		refs[entry.ClassSlot] = r
	}
	var groups [1]*primaryarena.Group
	if len(entries) > 0 {
		groups[0], ok, e = a.NewGroup(refs, work)
		if !ok || e != nil {
			return 0, e
		}
		defer a.DropGroup(groups[0], work)
	}
	image := make([]byte, page.PageSize)
	if e = node.EncodePrimaryDirectory(image, bundle.Directory.PageID, baseSeq, base, entries); e != nil {
		return 0, e
	}
	if constructor != nil {
		ok, e = constructor.SealPrimaryConstructionV6(image, groups, work)
	} else {
		ok, e = a.SealDirectory(bundle.Directory, image, groups, work)
	}
	if !ok || e != nil {
		return 0, e
	}
	if !a.CapsuleFormatV6() {
		metrics.PrimaryDirectoryPagesWritten++
	}
	metrics.PrimaryBytesWritten += page.PageSize
	success = true
	return bundle.Directory.PageID, nil
}

// BuildPrimaryRoot wraps a materialized DATA root in a complete ordinary
// directory. Its private paired claims transfer to the DB publisher.
func (z *Zipper) BuildPrimaryRoot(baseID, baseSequence uint64) (uint64, error) {
	image, e := z.pager.Get(baseID)
	if e != nil {
		return 0, e
	}
	return z.writePrimaryDirectory(node.PrimaryOperand{Ref: page.PageChildRef(baseID), Digest: sha256.Sum256(image)}, baseSequence, nil, &adaptive.Metrics{})
}

// CopyPrimaryRoot gives a system-only ordinary publication its own complete
// directory and private paired record, without changing the DATA base.
func (z *Zipper) CopyPrimaryRoot(id uint64) (uint64, error) {
	image, e := z.pager.Get(id)
	if e != nil {
		return 0, e
	}
	d, e := node.DecodePrimaryDirectory(image)
	if e != nil {
		return 0, e
	}
	entries := make([]node.PrimaryDirectoryEntry, d.Count())
	for i := range entries {
		entries[i], e = d.Entry(i)
		if e != nil {
			return 0, e
		}
	}
	base, seq := d.Base()
	return z.writePrimaryDirectory(base, seq, entries, &adaptive.Metrics{})
}

// RebasePrimaryRoot builds a new complete ordinary directory after genuine DATA
// maintenance. Component operands remain exact immutable banks retained by the
// source snapshot until the new directory acquires independent custody.
func (z *Zipper) RebasePrimaryRoot(baseID, baseSequence uint64, entries []node.PrimaryDirectoryEntry) (uint64, error) {
	image, err := z.pager.Get(baseID)
	if err != nil {
		return 0, err
	}
	return z.writePrimaryDirectory(node.PrimaryOperand{Ref: page.PageChildRef(baseID), Digest: sha256.Sum256(image)}, baseSequence, entries, &adaptive.Metrics{})
}
