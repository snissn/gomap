package db

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/snissn/gomap/TreeDB/internal/bulk"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/tree"
)

const vacuumCollectionRootDescriptorPrefix = "collections/root/"

type vacuumCollectionRootDescriptor struct {
	key    []byte
	rootID uint64
}

type vacuumAllocator interface {
	Alloc(hint uint64) (uint64, error)
}

func vacuumCollectionRootDescriptorPrefixEnd() []byte {
	prefix := []byte(vacuumCollectionRootDescriptorPrefix)
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
	if p == nil {
		return nil, errors.New("vacuum: missing pager")
	}
	if systemRootID == 0 {
		return nil, nil
	}

	start := []byte(vacuumCollectionRootDescriptorPrefix)
	it := tree.New(p, reader, systemRootID).IteratorWithOptions(start, vacuumCollectionRootDescriptorPrefixEnd(), tree.IteratorOptions{
		Mode: tree.IteratorModePointerProjection,
	})
	defer func() { _ = it.Close() }()

	var out []vacuumCollectionRootDescriptor
	for it.Valid() {
		key := it.UnsafeKey()
		if !bytes.HasPrefix(key, start) {
			break
		}
		val, _, flags := it.UnsafeEntry()
		if flags&node.FlagPointer != 0 {
			return nil, fmt.Errorf("vacuum: collection root descriptor %q is pointer-backed", string(key))
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

func vacuumRewriteCollectionRoots(oldPager *pager.Pager, reader tree.SlabReader, systemRootID uint64, alloc vacuumAllocator, newPager *pager.Pager) (map[string][]byte, error) {
	descriptors, err := vacuumCollectCollectionRootDescriptors(oldPager, reader, systemRootID)
	if err != nil || len(descriptors) == 0 {
		return nil, err
	}

	replacements := make(map[string][]byte, len(descriptors))
	rootRemap := make(map[uint64]uint64, len(descriptors))
	for _, descriptor := range descriptors {
		oldRoot := descriptor.rootID
		newRoot := oldRoot
		if oldRoot != 0 {
			if existing, ok := rootRemap[oldRoot]; ok {
				newRoot = existing
			} else {
				newRoot, err = vacuumCopyCollectionRoot(oldPager, oldRoot, alloc, newPager)
				if err != nil {
					return nil, fmt.Errorf("vacuum: copy collection root %q: %w", string(descriptor.key), err)
				}
				rootRemap[oldRoot] = newRoot
			}
		}
		if newRoot != oldRoot {
			encoded := make([]byte, 8)
			binary.BigEndian.PutUint64(encoded, newRoot)
			replacements[string(descriptor.key)] = encoded
		}
	}
	if len(replacements) == 0 {
		return nil, nil
	}
	return replacements, nil
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

func vacuumBuildSystemRoot(oldPager *pager.Pager, reader tree.SlabReader, systemRootID uint64, alloc vacuumAllocator, newPager *pager.Pager, opts bulk.BuildOptions, replacements map[string][]byte) (uint64, error) {
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
	replacements map[string][]byte
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
	val, ok := it.replacements[string(it.base.UnsafeKey())]
	return val, ok
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
