package db

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"

	"github.com/snissn/gomap/TreeDB/internal/bulk"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/tree"
)

var systemNamedRootKeyPrefix = []byte("sys:r:")

const namedRootDescriptorVersion = 1

type namedRootFormat struct {
	outerLeavesInValueLog bool
	leafPrefixCompression bool
	allowValues           bool
}

type namedRootDescriptor struct {
	key        []byte
	name       string
	collection string
	indexName  string
	kind       byte
	rootPageID uint64
	format     namedRootFormat
}

func decodeNamedRootDescriptor(key, raw []byte) (namedRootDescriptor, error) {
	if len(raw) < 17 {
		return namedRootDescriptor{}, errors.New("db: truncated named root descriptor")
	}
	ver := int(raw[0])
	if ver != namedRootDescriptorVersion {
		return namedRootDescriptor{}, fmt.Errorf("db: unsupported named root descriptor version %d", ver)
	}
	flags := raw[2]
	cursor := 3
	readString := func() (string, error) {
		if cursor+2 > len(raw) {
			return "", errors.New("db: truncated named root descriptor string length")
		}
		size := int(binary.BigEndian.Uint16(raw[cursor : cursor+2]))
		cursor += 2
		if cursor+size > len(raw) {
			return "", errors.New("db: truncated named root descriptor string payload")
		}
		value := string(raw[cursor : cursor+size])
		cursor += size
		return value, nil
	}

	name, err := readString()
	if err != nil {
		return namedRootDescriptor{}, err
	}
	collection, err := readString()
	if err != nil {
		return namedRootDescriptor{}, err
	}
	indexName, err := readString()
	if err != nil {
		return namedRootDescriptor{}, err
	}
	if cursor+8 != len(raw) {
		return namedRootDescriptor{}, errors.New("db: malformed named root descriptor payload")
	}
	return namedRootDescriptor{
		key:        append([]byte(nil), key...),
		name:       name,
		collection: collection,
		indexName:  indexName,
		kind:       raw[1],
		rootPageID: binary.BigEndian.Uint64(raw[cursor : cursor+8]),
		format: namedRootFormat{
			outerLeavesInValueLog: flags&(1<<0) != 0,
			leafPrefixCompression: flags&(1<<1) != 0,
			allowValues:           flags&(1<<2) != 0,
		},
	}, nil
}

func (d namedRootDescriptor) encode() ([]byte, error) {
	name := []byte(d.name)
	collection := []byte(d.collection)
	indexName := []byte(d.indexName)
	if len(name) > 65535 || len(collection) > 65535 || len(indexName) > 65535 {
		return nil, errors.New("db: named root descriptor field too long")
	}
	flags := uint8(0)
	if d.format.outerLeavesInValueLog {
		flags |= 1 << 0
	}
	if d.format.leafPrefixCompression {
		flags |= 1 << 1
	}
	if d.format.allowValues {
		flags |= 1 << 2
	}
	out := make([]byte, 0, 64+len(name)+len(collection)+len(indexName))
	out = append(out, byte(namedRootDescriptorVersion))
	out = append(out, d.kind)
	out = append(out, flags)
	out = binary.BigEndian.AppendUint16(out, uint16(len(name)))
	out = append(out, name...)
	out = binary.BigEndian.AppendUint16(out, uint16(len(collection)))
	out = append(out, collection...)
	out = binary.BigEndian.AppendUint16(out, uint16(len(indexName)))
	out = append(out, indexName...)
	out = binary.BigEndian.AppendUint64(out, d.rootPageID)
	return out, nil
}

func loadNamedRootDescriptors(p *pager.Pager, vlogs *valuelog.Set, systemRootID uint64) ([]namedRootDescriptor, error) {
	if p == nil || systemRootID == 0 {
		return nil, nil
	}
	end := append(append([]byte{}, systemNamedRootKeyPrefix...), 0xff)
	it := tree.New(p, newValueReader(vlogs), systemRootID).IteratorWithOptions(systemNamedRootKeyPrefix, end, tree.IteratorOptions{})
	defer it.Close()

	descriptors := make([]namedRootDescriptor, 0, 8)
	for ; it.Valid(); it.Next() {
		key := it.UnsafeKey()
		if !bytes.HasPrefix(key, systemNamedRootKeyPrefix) {
			continue
		}
		if it.IsDeleted() {
			continue
		}
		desc, err := decodeNamedRootDescriptor(key, it.UnsafeValue())
		if err != nil {
			return nil, err
		}
		descriptors = append(descriptors, desc)
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	sort.Slice(descriptors, func(i, j int) bool {
		return bytes.Compare(descriptors[i].key, descriptors[j].key) < 0
	})
	return descriptors, nil
}

func updateNamedRootDescriptorValue(raw []byte, newRootID uint64) ([]byte, error) {
	desc, err := decodeNamedRootDescriptor(nil, raw)
	if err != nil {
		return nil, err
	}
	desc.rootPageID = newRootID
	return desc.encode()
}

func namedRootBuildOptions(format namedRootFormat, leafLog bulk.LeafPageLog, leafColumnar, packedValuePtr, internalBaseDelta bool) bulk.BuildOptions {
	opts := bulk.BuildOptions{
		LeafPrefixCompression: format.leafPrefixCompression,
		LeafColumnar:          leafColumnar,
		PackedValuePtr:        packedValuePtr,
		InternalBaseDelta:     internalBaseDelta,
	}
	if format.outerLeavesInValueLog {
		opts.LeafPageLog = leafLog
	}
	return opts
}

type overrideIterator struct {
	inner     iteratorWithEntry
	overrides map[string][]byte
}

func (it *overrideIterator) override() ([]byte, bool) {
	if it == nil || it.inner == nil || len(it.overrides) == 0 || !it.inner.Valid() {
		return nil, false
	}
	value, ok := it.overrides[string(it.inner.UnsafeKey())]
	return value, ok
}

func (it *overrideIterator) Valid() bool { return it.inner != nil && it.inner.Valid() }
func (it *overrideIterator) Next()       { it.inner.Next() }
func (it *overrideIterator) Seek(key []byte) {
	it.inner.Seek(key)
}
func (it *overrideIterator) UnsafeKey() []byte { return it.inner.UnsafeKey() }
func (it *overrideIterator) UnsafeValue() []byte {
	if value, ok := it.override(); ok {
		return value
	}
	return it.inner.UnsafeValue()
}
func (it *overrideIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	if value, ok := it.override(); ok {
		return value, page.ValuePtr{}, 0
	}
	return it.inner.UnsafeEntry()
}
func (it *overrideIterator) Key() []byte                 { return it.inner.Key() }
func (it *overrideIterator) Value() []byte               { return it.UnsafeValue() }
func (it *overrideIterator) KeyCopy(dst []byte) []byte   { return it.inner.KeyCopy(dst) }
func (it *overrideIterator) ValueCopy(dst []byte) []byte { return append(dst[:0], it.UnsafeValue()...) }
func (it *overrideIterator) IsDeleted() bool             { return it.inner.IsDeleted() }
func (it *overrideIterator) Error() error                { return it.inner.Error() }
func (it *overrideIterator) Close() error                { return it.inner.Close() }
func (it *overrideIterator) Domain() (start, end []byte) { return it.inner.Domain() }
