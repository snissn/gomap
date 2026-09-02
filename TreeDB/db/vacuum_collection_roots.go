package db

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/bulk"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/tree"
)

const (
	vacuumCollectionRootDescriptorPrefix        = "collections/root/"
	vacuumCollectionRootOverlayDescriptorPrefix = "collections/root-overlay/"
)

var (
	vacuumCollectionRootDescriptorPrefixBytes        = []byte(vacuumCollectionRootDescriptorPrefix)
	vacuumCollectionRootOverlayDescriptorPrefixBytes = []byte(vacuumCollectionRootOverlayDescriptorPrefix)
)

type vacuumCollectionRootDescriptor struct {
	key       []byte
	rootID    uint64
	rootIDs   []uint64
	rootIndex int
}

type vacuumCollectionRootReplacement struct {
	key   []byte
	value []byte
}

type collectionToken struct {
	indexGenerationID uint64
	systemRootPageID  uint64
	commitSeq         uint64
	publishEpoch      uint64
}

type collectionEntry struct {
	key           []byte
	sourceRootIDs []uint64
	clonedRootIDs []uint64
}

type collectionBasis struct {
	snapshot     *Snapshot
	token        collectionToken
	entries      []collectionEntry
	byKey        map[string]int
	destRefCount map[uint64]int

	// sourceToDest deduplicates aliases within one accepted basis. destPages
	// owns every destination page allocated for a cloned root so dropped clones
	// can be returned to the destination allocator during adjacent reconciliation.
	sourceToDest map[uint64]uint64
	destPages    map[uint64][]uint64
}

type collectionRootCloneFunc func(sourceRootID uint64) (destRootID uint64, allocatedPages []uint64, err error)
type collectionRootReclaimFunc func(destRootID uint64, allocatedPages []uint64) error

type vacuumCollectionRootRewriteFunc func(vacuumCollectionRootDescriptor) (uint64, error)

type collectionRootDescriptorShapeError struct {
	key     []byte
	length  int
	overlay bool
}

func (e *collectionRootDescriptorShapeError) Error() string {
	if e == nil {
		return "vacuum: malformed collection root descriptor"
	}
	if e.overlay {
		return fmt.Sprintf("vacuum: collection root overlay descriptor %q has malformed root id list length %d", string(e.key), e.length)
	}
	return fmt.Sprintf("vacuum: collection root descriptor %q has malformed root id length %d", string(e.key), e.length)
}

func isCollectionRootDescriptorShapeError(err error) bool {
	var shapeErr *collectionRootDescriptorShapeError
	return errors.As(err, &shapeErr)
}

type vacuumAllocator interface {
	Alloc(hint uint64) (uint64, error)
}

type vacuumCollectionAllocator interface {
	vacuumAllocator
	Free(id uint64) error
}

type vacuumRecordingAllocator struct {
	base        vacuumCollectionAllocator
	pages       []uint64
	inlinePages [16]uint64
}

func newVacuumRecordingAllocator(base vacuumCollectionAllocator) *vacuumRecordingAllocator {
	a := &vacuumRecordingAllocator{base: base}
	a.pages = a.inlinePages[:0]
	return a
}

func (a *vacuumRecordingAllocator) Free(id uint64) error {
	if a == nil || a.base == nil {
		return errors.New("vacuum: missing recording allocator")
	}
	return a.base.Free(id)
}

func (a *vacuumRecordingAllocator) Alloc(hint uint64) (uint64, error) {
	if a == nil || a.base == nil {
		return 0, errors.New("vacuum: missing recording allocator")
	}
	id, err := a.base.Alloc(hint)
	if err == nil {
		a.pages = append(a.pages, id)
	}
	return id, err
}

type vacuumUnsafeToReader interface {
	ReadUnsafeTo(ptr page.ValuePtr, dst []byte) ([]byte, bool, error)
}

type vacuumUnsafeAppendReader interface {
	ReadUnsafeAppend(ptr page.ValuePtr, dst []byte) ([]byte, error)
}

func vacuumCollectionRootDescriptorPrefixEnd() []byte {
	return vacuumDescriptorPrefixEnd(vacuumCollectionRootDescriptorPrefixBytes)
}

func vacuumCollectionRootOverlayDescriptorPrefixEnd() []byte {
	return vacuumDescriptorPrefixEnd(vacuumCollectionRootOverlayDescriptorPrefixBytes)
}

func vacuumDescriptorPrefixEnd(prefix []byte) []byte {
	out := append([]byte(nil), prefix...)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] != 0xff {
			out[i]++
			return out[:i+1]
		}
	}
	return nil
}

func vacuumCollectCollectionRootDescriptors(p *pager.Pager, reader tree.SlabReader, systemRootID uint64) ([]vacuumCollectionRootDescriptor, error) {
	return vacuumCollectCollectionRootDescriptorsWithContext(context.Background(), p, reader, systemRootID)
}

func vacuumCollectCollectionRootDescriptorsWithContext(ctx context.Context, p *pager.Pager, reader tree.SlabReader, systemRootID uint64) ([]vacuumCollectionRootDescriptor, error) {
	entries, err := vacuumCollectCollectionEntriesFromRoot(ctx, p, reader, systemRootID)
	if err != nil {
		return nil, err
	}
	var out []vacuumCollectionRootDescriptor
	for _, entry := range entries {
		for i, rootID := range entry.sourceRootIDs {
			out = append(out, vacuumCollectionRootDescriptor{
				key:       append([]byte(nil), entry.key...),
				rootID:    rootID,
				rootIDs:   append([]uint64(nil), entry.sourceRootIDs...),
				rootIndex: i,
			})
		}
	}
	return out, nil
}

func vacuumCollectCollectionEntries(ctx context.Context, snap *Snapshot) ([]collectionEntry, error) {
	if snap == nil || snap.idx == nil || snap.state == nil {
		return nil, errors.New("vacuum: missing collection snapshot")
	}
	return vacuumCollectCollectionEntriesFromRoot(ctx, snap.idx.pager, &snap.reader, snap.state.SystemRootPageID)
}

func vacuumCollectCollectionEntriesFromRoot(ctx context.Context, p *pager.Pager, reader tree.SlabReader, systemRootID uint64) ([]collectionEntry, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if p == nil {
		return nil, errors.New("vacuum: missing pager")
	}
	if systemRootID == 0 {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	var out []collectionEntry
	var pointerScratch []byte
	descriptorPrefixes := []struct {
		prefix    []byte
		end       []byte
		allowList bool
	}{
		{prefix: vacuumCollectionRootOverlayDescriptorPrefixBytes, end: vacuumCollectionRootOverlayDescriptorPrefixEnd(), allowList: true},
		{prefix: vacuumCollectionRootDescriptorPrefixBytes, end: vacuumCollectionRootDescriptorPrefixEnd()},
	}
	for _, descriptorPrefix := range descriptorPrefixes {
		it := tree.New(p, reader, systemRootID).IteratorWithOptions(descriptorPrefix.prefix, descriptorPrefix.end, tree.IteratorOptions{
			Mode: tree.IteratorModePointerProjection,
		})
		for it.Valid() {
			if err := ctx.Err(); err != nil {
				_ = it.Close()
				return nil, err
			}
			key := it.UnsafeKey()
			if !bytes.HasPrefix(key, descriptorPrefix.prefix) {
				break
			}
			val, ptr, flags := it.UnsafeEntry()
			var err error
			val, pointerScratch, err = vacuumCollectionRootDescriptorValue(reader, key, val, ptr, flags, pointerScratch)
			if err != nil {
				_ = it.Close()
				return nil, err
			}
			rootIDs, err := decodeCollectionRootDescriptorRootIDs(key, val, descriptorPrefix.allowList)
			if err != nil {
				_ = it.Close()
				return nil, err
			}
			out = append(out, collectionEntry{
				key:           append([]byte(nil), key...),
				sourceRootIDs: append([]uint64(nil), rootIDs...),
			})
			it.Next()
		}
		if err := it.Error(); err != nil {
			_ = it.Close()
			return nil, err
		}
		if err := it.Close(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func reconcileCollectionBasisEntries(old *collectionBasis, successorEntries []collectionEntry, token collectionToken, clone collectionRootCloneFunc, reclaim collectionRootReclaimFunc) (*collectionBasis, int, error) {
	// The caller transfers ownership of the collected catalog. Keep the exact
	// key and root-vector buffers pinned in the basis instead of copying the
	// complete catalog a second time.
	entries := successorEntries
	if !collectionEntriesSorted(entries) {
		sort.Slice(entries, func(i, j int) bool {
			return bytes.Compare(entries[i].key, entries[j].key) < 0
		})
	}
	next := &collectionBasis{
		token:        token,
		entries:      entries,
		byKey:        make(map[string]int, len(entries)),
		destRefCount: make(map[uint64]int, len(entries)),
		sourceToDest: make(map[uint64]uint64, len(entries)),
		destPages:    make(map[uint64][]uint64, len(entries)),
	}
	for i := range next.entries {
		key := collectionEntryKeyString(next.entries[i].key)
		if _, exists := next.byKey[key]; exists {
			return nil, 0, fmt.Errorf("vacuum: duplicate collection descriptor %q", key)
		}
		next.byKey[key] = i
	}

	dirty := collectionBasisDirtyDescriptorCount(old, next)
	unchanged := make([]bool, len(next.entries))
	clonedRootCount := 0
	for i := range next.entries {
		clonedRootCount += len(next.entries[i].sourceRootIDs)
	}
	clonedRoots := make([]uint64, clonedRootCount)
	clonedRootOffset := 0
	for i := range next.entries {
		count := len(next.entries[i].sourceRootIDs)
		if count > 0 {
			next.entries[i].clonedRootIDs = clonedRoots[clonedRootOffset : clonedRootOffset+count]
			clonedRootOffset += count
		}
	}
	sameSourceGeneration := old != nil && old.token.indexGenerationID == next.token.indexGenerationID
	if old != nil {
		for i := range next.entries {
			oldIndex, ok := old.byKey[collectionEntryKeyString(next.entries[i].key)]
			if !ok || oldIndex < 0 || oldIndex >= len(old.entries) {
				continue
			}
			oldEntry := old.entries[oldIndex]
			if !equalRootIDVectors(oldEntry.sourceRootIDs, next.entries[i].sourceRootIDs) {
				continue
			}
			if !sameSourceGeneration && len(next.entries[i].sourceRootIDs) > 0 {
				continue
			}
			if len(oldEntry.clonedRootIDs) != len(oldEntry.sourceRootIDs) {
				return nil, 0, fmt.Errorf("vacuum: descriptor %q has mismatched source/clone vectors", next.entries[i].key)
			}
			unchanged[i] = true
			copy(next.entries[i].clonedRootIDs, oldEntry.clonedRootIDs)
			for rootIndex, sourceRootID := range oldEntry.sourceRootIDs {
				destRootID := oldEntry.clonedRootIDs[rootIndex]
				if sourceRootID == 0 || destRootID == 0 {
					continue
				}
				if existing, ok := next.sourceToDest[sourceRootID]; ok && existing != destRootID {
					return nil, 0, fmt.Errorf("vacuum: aliased source root %d maps to destinations %d and %d", sourceRootID, existing, destRootID)
				}
				next.sourceToDest[sourceRootID] = destRootID
				next.destRefCount[destRootID]++
				if pages, ok := old.destPages[destRootID]; ok {
					next.destPages[destRootID] = pages
				}
			}
		}
	}

	newDestinations := make(map[uint64]struct{})
	cleanupNewDestinations := func() error {
		if reclaim == nil || len(newDestinations) == 0 {
			return nil
		}
		ids := make([]uint64, 0, len(newDestinations))
		for id := range newDestinations {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		var errs []error
		for _, id := range ids {
			if err := reclaim(id, next.destPages[id]); err != nil {
				errs = append(errs, err)
			}
		}
		return errors.Join(errs...)
	}
	for i := range next.entries {
		if unchanged[i] {
			continue
		}
		entry := &next.entries[i]
		for rootIndex, sourceRootID := range entry.sourceRootIDs {
			if sourceRootID == 0 {
				continue
			}
			destRootID, ok := next.sourceToDest[sourceRootID]
			if !ok {
				if clone == nil {
					return nil, 0, errors.Join(errors.New("vacuum: missing collection root cloner"), cleanupNewDestinations())
				}
				var pages []uint64
				var err error
				destRootID, pages, err = clone(sourceRootID)
				if err != nil {
					return nil, 0, errors.Join(err, cleanupNewDestinations())
				}
				if destRootID == 0 {
					return nil, 0, errors.Join(fmt.Errorf("vacuum: clone of collection root %d returned zero", sourceRootID), cleanupNewDestinations())
				}
				next.sourceToDest[sourceRootID] = destRootID
				// clone transfers ownership of its page list to this basis.
				next.destPages[destRootID] = pages
				newDestinations[destRootID] = struct{}{}
			}
			entry.clonedRootIDs[rootIndex] = destRootID
			next.destRefCount[destRootID]++
		}
	}

	if old != nil && reclaim != nil {
		unused := make([]uint64, 0, len(old.destRefCount))
		for destRootID, refs := range old.destRefCount {
			if refs > 0 && next.destRefCount[destRootID] == 0 {
				unused = append(unused, destRootID)
			}
		}
		sort.Slice(unused, func(i, j int) bool { return unused[i] < unused[j] })
		for _, destRootID := range unused {
			if err := reclaim(destRootID, old.destPages[destRootID]); err != nil {
				return nil, 0, err
			}
		}
	}
	return next, dirty, nil
}

// collectionEntryKeyString is safe because a basis owns and pins its immutable
// key buffers for at least as long as byKey can reference them.
func collectionEntryKeyString(key []byte) string {
	return unsafe.String(unsafe.SliceData(key), len(key))
}

func collectionEntriesSorted(entries []collectionEntry) bool {
	for i := 1; i < len(entries); i++ {
		if bytes.Compare(entries[i-1].key, entries[i].key) > 0 {
			return false
		}
	}
	return true
}

func collectionBasisDirtyDescriptorCount(old, next *collectionBasis) int {
	if old == nil {
		return len(next.entries)
	}
	sameSourceGeneration := old.token.indexGenerationID == next.token.indexGenerationID
	dirty := 0
	for i := range next.entries {
		oldIndex, ok := old.byKey[collectionEntryKeyString(next.entries[i].key)]
		if !ok || oldIndex < 0 || oldIndex >= len(old.entries) || !equalRootIDVectors(old.entries[oldIndex].sourceRootIDs, next.entries[i].sourceRootIDs) || (!sameSourceGeneration && len(next.entries[i].sourceRootIDs) > 0) {
			dirty++
		}
	}
	for i := range old.entries {
		if _, ok := next.byKey[collectionEntryKeyString(old.entries[i].key)]; !ok {
			dirty++
		}
	}
	return dirty
}

func equalRootIDVectors(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (basis *collectionBasis) replacements() ([]vacuumCollectionRootReplacement, error) {
	if basis == nil {
		return nil, errors.New("vacuum: missing collection basis")
	}
	replacements := make([]vacuumCollectionRootReplacement, 0, len(basis.entries))
	for _, entry := range basis.entries {
		if len(entry.sourceRootIDs) != len(entry.clonedRootIDs) {
			return nil, fmt.Errorf("vacuum: descriptor %q has mismatched source/clone vectors", entry.key)
		}
		if equalRootIDVectors(entry.sourceRootIDs, entry.clonedRootIDs) {
			continue
		}
		replacements = append(replacements, vacuumCollectionRootReplacement{
			key:   append([]byte(nil), entry.key...),
			value: encodeCollectionRootDescriptorRootIDs(entry.clonedRootIDs),
		})
	}
	return replacements, nil
}

func vacuumBuildCollectionBasis(ctx context.Context, old *collectionBasis, snap *Snapshot, token collectionToken, alloc vacuumCollectionAllocator, newPager *pager.Pager, visitSourcePage func(uint64)) (*collectionBasis, int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if snap == nil || alloc == nil || newPager == nil {
		return nil, 0, errors.New("vacuum: missing collection basis input")
	}
	entries, err := vacuumCollectCollectionEntries(ctx, snap)
	if err != nil {
		return nil, 0, err
	}
	reclaim := func(_ uint64, pages []uint64) error {
		var errs []error
		for _, pageID := range pages {
			if pageID == 0 {
				continue
			}
			if err := alloc.Free(pageID); err != nil {
				errs = append(errs, err)
			}
		}
		return errors.Join(errs...)
	}
	clone := func(sourceRootID uint64) (uint64, []uint64, error) {
		if err := ctx.Err(); err != nil {
			return 0, nil, err
		}
		recordingAlloc := newVacuumRecordingAllocator(alloc)
		destRootID, err := vacuumCopyCollectionRootWithObserver(snap.idx.pager, sourceRootID, recordingAlloc, newPager, visitSourcePage)
		if err != nil {
			return 0, nil, errors.Join(err, reclaim(0, recordingAlloc.pages))
		}
		return destRootID, recordingAlloc.pages, nil
	}
	next, dirty, err := reconcileCollectionBasisEntries(old, entries, token, clone, reclaim)
	if err != nil {
		return nil, 0, err
	}
	next.snapshot = snap
	return next, dirty, nil
}

func decodeCollectionRootDescriptorRootIDs(key, val []byte, allowList bool) ([]uint64, error) {
	if len(val) == 0 && allowList {
		return nil, nil
	}
	if !allowList {
		if len(val) != 8 {
			return nil, &collectionRootDescriptorShapeError{key: append([]byte(nil), key...), length: len(val)}
		}
		return []uint64{binary.BigEndian.Uint64(val)}, nil
	}
	if len(val)%8 != 0 {
		return nil, &collectionRootDescriptorShapeError{key: append([]byte(nil), key...), length: len(val), overlay: true}
	}
	out := make([]uint64, len(val)/8)
	for i := range out {
		out[i] = binary.BigEndian.Uint64(val[i*8 : (i+1)*8])
	}
	return out, nil
}

func vacuumCollectionRootDescriptorValue(reader tree.SlabReader, key []byte, val []byte, ptr page.ValuePtr, flags byte, scratch []byte) ([]byte, []byte, error) {
	if flags&node.FlagPointer == 0 {
		return val, scratch, nil
	}
	if reader == nil {
		return nil, scratch, fmt.Errorf("vacuum: collection root descriptor %q is pointer-backed without value reader", string(key))
	}
	if toReader, ok := reader.(vacuumUnsafeToReader); ok {
		resolved, usedDst, err := toReader.ReadUnsafeTo(ptr, scratch[:0])
		if err != nil {
			return nil, scratch, fmt.Errorf("vacuum: read pointer-backed collection root descriptor %q: %w", string(key), err)
		}
		if usedDst {
			scratch = resolved
		}
		return resolved, scratch, nil
	}
	if appender, ok := reader.(vacuumUnsafeAppendReader); ok {
		resolved, err := appender.ReadUnsafeAppend(ptr, scratch[:0])
		if err != nil {
			return nil, scratch, fmt.Errorf("vacuum: read pointer-backed collection root descriptor %q: %w", string(key), err)
		}
		return resolved, resolved, nil
	}
	resolved, err := reader.ReadUnsafe(ptr)
	if err != nil {
		return nil, scratch, fmt.Errorf("vacuum: read pointer-backed collection root descriptor %q: %w", string(key), err)
	}
	return resolved, scratch, nil
}

func vacuumRewriteCollectionRoots(oldPager *pager.Pager, reader tree.SlabReader, systemRootID uint64, alloc vacuumAllocator, newPager *pager.Pager) ([]vacuumCollectionRootReplacement, error) {
	descriptors, err := vacuumCollectCollectionRootDescriptors(oldPager, reader, systemRootID)
	if err != nil || len(descriptors) == 0 {
		return nil, err
	}

	return vacuumRewriteCollectionRootDescriptors(descriptors, func(descriptor vacuumCollectionRootDescriptor) (uint64, error) {
		return vacuumCopyCollectionRoot(oldPager, descriptor.rootID, alloc, newPager)
	}, "vacuum: copy collection root")
}

func vacuumRewriteCollectionRootDescriptors(descriptors []vacuumCollectionRootDescriptor, rewriteRoot vacuumCollectionRootRewriteFunc, errPrefix string) ([]vacuumCollectionRootReplacement, error) {
	if len(descriptors) == 0 {
		return nil, nil
	}
	if rewriteRoot == nil {
		return nil, errors.New(errPrefix + ": missing rewrite function")
	}

	rootRemap := make(map[uint64]uint64, len(descriptors))
	type descriptorRewriteState struct {
		key     []byte
		rootIDs []uint64
		changed bool
	}
	statesByKey := make(map[string]*descriptorRewriteState, len(descriptors))
	for _, descriptor := range descriptors {
		key := string(descriptor.key)
		state := statesByKey[key]
		if state == nil {
			state = &descriptorRewriteState{
				key:     append([]byte(nil), descriptor.key...),
				rootIDs: append([]uint64(nil), descriptor.rootIDs...),
			}
			statesByKey[key] = state
		}
		oldRoot := descriptor.rootID
		newRoot := oldRoot
		if oldRoot != 0 {
			if existing, ok := rootRemap[oldRoot]; ok {
				newRoot = existing
			} else {
				var err error
				newRoot, err = rewriteRoot(descriptor)
				if err != nil {
					return nil, fmt.Errorf("%s %q: %w", errPrefix, string(descriptor.key), err)
				}
				rootRemap[oldRoot] = newRoot
			}
		}
		if newRoot != oldRoot {
			if descriptor.rootIndex < 0 || descriptor.rootIndex >= len(state.rootIDs) {
				return nil, fmt.Errorf("%s %q: malformed descriptor root index %d", errPrefix, string(descriptor.key), descriptor.rootIndex)
			}
			state.rootIDs[descriptor.rootIndex] = newRoot
			state.changed = true
		}
	}
	replacements := make([]vacuumCollectionRootReplacement, 0, len(statesByKey))
	for _, state := range statesByKey {
		if !state.changed {
			continue
		}
		replacements = append(replacements, vacuumCollectionRootReplacement{
			key:   state.key,
			value: encodeCollectionRootDescriptorRootIDs(state.rootIDs),
		})
	}
	if len(replacements) == 0 {
		return nil, nil
	}
	sort.Slice(replacements, func(i, j int) bool {
		return bytes.Compare(replacements[i].key, replacements[j].key) < 0
	})
	return replacements, nil
}

func encodeCollectionRootDescriptorRootID(rootID uint64) []byte {
	encoded := make([]byte, 8)
	binary.BigEndian.PutUint64(encoded, rootID)
	return encoded
}

func encodeCollectionRootDescriptorRootIDs(rootIDs []uint64) []byte {
	if len(rootIDs) == 1 {
		return encodeCollectionRootDescriptorRootID(rootIDs[0])
	}
	encoded := make([]byte, len(rootIDs)*8)
	for i, rootID := range rootIDs {
		binary.BigEndian.PutUint64(encoded[i*8:(i+1)*8], rootID)
	}
	return encoded
}

func vacuumCopyCollectionRoot(oldPager *pager.Pager, rootID uint64, alloc vacuumAllocator, newPager *pager.Pager) (uint64, error) {
	return vacuumCopyCollectionRootWithObserver(oldPager, rootID, alloc, newPager, nil)
}

func vacuumCopyCollectionRootWithObserver(oldPager *pager.Pager, rootID uint64, alloc vacuumAllocator, newPager *pager.Pager, visitSourcePage func(uint64)) (uint64, error) {
	if rootID == 0 {
		return 0, nil
	}
	if oldPager == nil || newPager == nil || alloc == nil {
		return 0, errors.New("vacuum: missing pager/allocator")
	}

	allLeafRefs, err := vacuumTreeAllLeafRefsIfCompleteWithObserver(oldPager, rootID, visitSourcePage)
	if err != nil {
		return 0, err
	}
	if allLeafRefs {
		return vacuumBuildInternalTreeFromLeafRefsWithObserver(oldPager, rootID, newPager, alloc, false, visitSourcePage)
	}
	return vacuumClonePagerTreeWithLeafRefsWithObserver(oldPager, rootID, alloc, newPager, false, visitSourcePage)
}

func vacuumCollectLeafRefChildrenIfComplete(p *pager.Pager, rootID uint64) ([]vacuumLeafChild, bool, error) {
	if p == nil {
		return nil, false, errors.New("vacuum: missing pager")
	}
	if rootID == 0 {
		return nil, false, errors.New("vacuum: missing root id")
	}

	out := make([]vacuumLeafChild, 0, 1024)
	var walk func(uint64) (bool, error)
	walk = func(id uint64) (bool, error) {
		data, err := p.Get(id)
		if err != nil {
			return false, err
		}
		n := node.NewNode(data)
		switch n.Type() {
		case page.PageTypeInternal:
			count := n.Count()
			for i := uint16(0); i < count; i++ {
				keyView, childRef, err := n.GetInternalEntryRefView(i)
				if err != nil {
					return false, err
				}
				if childRef.Kind == page.ChildRefLeafLog {
					out = append(out, vacuumLeafChild{
						key:      append([]byte(nil), keyView...),
						childRef: childRef,
					})
					continue
				}
				allLeafRefs, err := walk(childRef.Page)
				if err != nil || !allLeafRefs {
					return allLeafRefs, err
				}
			}
			return true, nil
		case page.PageTypeLeaf:
			return false, nil
		default:
			return false, fmt.Errorf("vacuum: unexpected page type %d at page %d", n.Type(), id)
		}
	}
	allLeafRefs, err := walk(rootID)
	if err != nil || !allLeafRefs {
		return nil, allLeafRefs, err
	}
	if len(out) == 0 {
		return nil, false, nil
	}
	return out, true, nil
}

func vacuumBuildSystemRoot(oldPager *pager.Pager, reader tree.SlabReader, systemRootID uint64, alloc vacuumAllocator, newPager *pager.Pager, opts bulk.BuildOptions, replacements []vacuumCollectionRootReplacement) (uint64, error) {
	sysIter := tree.New(oldPager, reader, systemRootID).IteratorWithOptions(nil, nil, tree.IteratorOptions{
		Mode: tree.IteratorModePointerProjection,
	})
	if len(replacements) > 0 {
		sysIter = &vacuumSystemRootRewriteIterator{
			base:         sysIter,
			replacements: replacements,
		}
	}
	sysRoot, err := bulk.BuildWithOptions(sysIter, alloc, newPager, opts)
	_ = sysIter.Close()
	return sysRoot, err
}

type vacuumSystemRootRewriteIterator struct {
	base         iterator.UnsafeIterator
	replacements []vacuumCollectionRootReplacement
}

func (it *vacuumSystemRootRewriteIterator) Valid() bool {
	return it != nil && it.base != nil && it.base.Valid()
}

func (it *vacuumSystemRootRewriteIterator) Next() {
	it.base.Next()
}

func (it *vacuumSystemRootRewriteIterator) Seek(key []byte) {
	it.base.Seek(key)
}

func (it *vacuumSystemRootRewriteIterator) replacement() ([]byte, bool) {
	if it == nil || it.base == nil || len(it.replacements) == 0 || !it.base.Valid() {
		return nil, false
	}
	key := it.base.UnsafeKey()
	idx := sort.Search(len(it.replacements), func(i int) bool {
		return bytes.Compare(it.replacements[i].key, key) >= 0
	})
	if idx >= len(it.replacements) || !bytes.Equal(it.replacements[idx].key, key) {
		return nil, false
	}
	return it.replacements[idx].value, true
}

func (it *vacuumSystemRootRewriteIterator) UnsafeKey() []byte {
	return it.base.UnsafeKey()
}

func (it *vacuumSystemRootRewriteIterator) UnsafeValue() []byte {
	if val, ok := it.replacement(); ok {
		return val
	}
	return it.base.UnsafeValue()
}

func (it *vacuumSystemRootRewriteIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	if val, ok := it.replacement(); ok {
		return val, page.ValuePtr{}, node.FlagInline
	}
	return it.base.UnsafeEntry()
}

func (it *vacuumSystemRootRewriteIterator) UnsafeEntryWithRevision() ([]byte, page.ValuePtr, byte, page.EntryRevision) {
	if val, ok := it.replacement(); ok {
		return val, page.ValuePtr{}, node.FlagInline, page.LegacyEntryRevision
	}
	return iterator.UnsafeEntryWithRevision(it.base)
}

func (it *vacuumSystemRootRewriteIterator) Key() []byte {
	return it.UnsafeKey()
}

func (it *vacuumSystemRootRewriteIterator) Value() []byte {
	return it.UnsafeValue()
}

func (it *vacuumSystemRootRewriteIterator) KeyCopy(dst []byte) []byte {
	key := it.UnsafeKey()
	if key == nil {
		return nil
	}
	return append(dst[:0], key...)
}

func (it *vacuumSystemRootRewriteIterator) ValueCopy(dst []byte) []byte {
	val := it.UnsafeValue()
	if val == nil {
		return nil
	}
	return append(dst[:0], val...)
}

func (it *vacuumSystemRootRewriteIterator) IsDeleted() bool {
	return it.base.IsDeleted()
}

func (it *vacuumSystemRootRewriteIterator) Error() error {
	return it.base.Error()
}

func (it *vacuumSystemRootRewriteIterator) Close() error {
	if it == nil || it.base == nil {
		return nil
	}
	return it.base.Close()
}

func (it *vacuumSystemRootRewriteIterator) Domain() (start, end []byte) {
	return it.base.Domain()
}
