package db

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"

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

	var out []vacuumCollectionRootDescriptor
	var pointerScratch []byte
	descriptorPrefixes := []struct {
		prefix    []byte
		end       []byte
		allowList bool
	}{
		{prefix: vacuumCollectionRootDescriptorPrefixBytes, end: vacuumCollectionRootDescriptorPrefixEnd()},
		{prefix: vacuumCollectionRootOverlayDescriptorPrefixBytes, end: vacuumCollectionRootOverlayDescriptorPrefixEnd(), allowList: true},
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
			keyCopy := append([]byte(nil), key...)
			for i, rootID := range rootIDs {
				out = append(out, vacuumCollectionRootDescriptor{
					key:       keyCopy,
					rootID:    rootID,
					rootIDs:   append([]uint64(nil), rootIDs...),
					rootIndex: i,
				})
			}
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
	if rootID == 0 {
		return 0, nil
	}
	if oldPager == nil || newPager == nil || alloc == nil {
		return 0, errors.New("vacuum: missing pager/allocator")
	}

	children, allLeafRefs, err := vacuumCollectLeafRefChildrenIfComplete(oldPager, rootID)
	if err != nil {
		return 0, err
	}
	if allLeafRefs {
		return vacuumBuildInternalTreeFromChildren(newPager, alloc, children, false)
	}
	return vacuumClonePagerTreeWithLeafRefs(oldPager, rootID, alloc, newPager, false)
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
