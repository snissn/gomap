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

const vacuumCollectionRootDescriptorPrefix = "collections/root/"

var vacuumCollectionRootDescriptorPrefixBytes = []byte(vacuumCollectionRootDescriptorPrefix)

type vacuumCollectionRootDescriptor struct {
	key    []byte
	rootID uint64
}

type vacuumCollectionRootReplacement struct {
	key   []byte
	value []byte
}

type vacuumCollectionRootRewriteFunc func(vacuumCollectionRootDescriptor) (uint64, error)

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
	out := append([]byte(nil), vacuumCollectionRootDescriptorPrefixBytes...)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] != 0xff {
			out[i]++
			return out[:i+1]
		}
	}
	return nil
}

func vacuumCollectCollectionRootDescriptors(p *pager.Pager, reader tree.SlabReader, systemRootID uint64) ([]vacuumCollectionRootDescriptor, error) {
	return vacuumCollectCollectionRootDescriptorsWithContext(nil, p, reader, systemRootID)
}

func vacuumCollectCollectionRootDescriptorsWithContext(ctx context.Context, p *pager.Pager, reader tree.SlabReader, systemRootID uint64) ([]vacuumCollectionRootDescriptor, error) {
	if p == nil {
		return nil, errors.New("vacuum: missing pager")
	}
	if systemRootID == 0 {
		return nil, nil
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}

	it := tree.New(p, reader, systemRootID).IteratorWithOptions(vacuumCollectionRootDescriptorPrefixBytes, vacuumCollectionRootDescriptorPrefixEnd(), tree.IteratorOptions{
		Mode: tree.IteratorModePointerProjection,
	})
	defer func() { _ = it.Close() }()

	var out []vacuumCollectionRootDescriptor
	var pointerScratch []byte
	for it.Valid() {
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		key := it.UnsafeKey()
		if !bytes.HasPrefix(key, vacuumCollectionRootDescriptorPrefixBytes) {
			break
		}
		val, ptr, flags := it.UnsafeEntry()
		var err error
		val, pointerScratch, err = vacuumCollectionRootDescriptorValue(reader, key, val, ptr, flags, pointerScratch)
		if err != nil {
			return nil, err
		}
		if len(val) != 8 {
			return nil, fmt.Errorf("vacuum: collection root descriptor %q has malformed root id length %d", string(key), len(val))
		}
		out = append(out, vacuumCollectionRootDescriptor{
			key:    append([]byte(nil), key...),
			rootID: binary.BigEndian.Uint64(val),
		})
		it.Next()
	}
	if err := it.Error(); err != nil {
		return nil, err
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

	replacements := make([]vacuumCollectionRootReplacement, 0, len(descriptors))
	rootRemap := make(map[uint64]uint64, len(descriptors))
	for _, descriptor := range descriptors {
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
			replacements = append(replacements, vacuumCollectionRootReplacement{
				key:   descriptor.key,
				value: encodeCollectionRootDescriptorRootID(newRoot),
			})
		}
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

func vacuumCopyCollectionRoot(oldPager *pager.Pager, rootID uint64, alloc vacuumAllocator, newPager *pager.Pager) (uint64, error) {
	if rootID == 0 {
		return 0, nil
	}
	if _, ok := page.DecodeLeafRef(rootID); ok {
		return rootID, nil
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
	return vacuumClonePagerTreeWithLeafRefs(oldPager, rootID, alloc, newPager)
}

func vacuumCollectLeafRefChildrenIfComplete(p *pager.Pager, rootID uint64) ([]vacuumLeafChild, bool, error) {
	if p == nil {
		return nil, false, errors.New("vacuum: missing pager")
	}
	if rootID == 0 {
		return nil, false, errors.New("vacuum: missing root id")
	}
	if _, ok := page.DecodeLeafRef(rootID); ok {
		return nil, true, nil
	}

	out := make([]vacuumLeafChild, 0, 1024)
	var walk func(uint64) (bool, error)
	walk = func(id uint64) (bool, error) {
		if _, ok := page.DecodeLeafRef(id); ok {
			return true, nil
		}
		data, err := p.Get(id)
		if err != nil {
			return false, err
		}
		n := node.NewNode(data)
		switch n.Type() {
		case page.PageTypeInternal:
			count := n.Count()
			for i := uint16(0); i < count; i++ {
				keyView, childID, err := n.GetInternalEntryView(i)
				if err != nil {
					return false, err
				}
				if _, ok := page.DecodeLeafRef(childID); ok {
					out = append(out, vacuumLeafChild{
						key:     append([]byte(nil), keyView...),
						childID: childID,
					})
					continue
				}
				allLeafRefs, err := walk(childID)
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
	if !bytes.HasPrefix(key, vacuumCollectionRootDescriptorPrefixBytes) {
		return nil, false
	}
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
