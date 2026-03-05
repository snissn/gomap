package collections

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/snissn/gomap/TreeDB/batch"
)

const collectionRootDescriptorVersion = 1

type CollectionRootKind uint8

const (
	CollectionRootKindPrimary CollectionRootKind = iota + 1
	CollectionRootKindSecondaryIndex
)

type CollectionRootFormat struct {
	OuterLeavesInValueLog bool
	LeafPrefixCompression bool
	AllowValues           bool
}

type CollectionRootDescriptor struct {
	Version    uint16
	Name       string
	Collection string
	IndexName  string
	Kind       CollectionRootKind
	RootPageID uint64
	Format     CollectionRootFormat
}

func (d *CollectionRootDescriptor) normalizeAndValidate() error {
	if d == nil {
		return errors.New("collections: nil root descriptor")
	}
	if d.Version == 0 {
		d.Version = collectionRootDescriptorVersion
	}
	if d.Version != collectionRootDescriptorVersion {
		return fmt.Errorf("collections: unsupported root descriptor version %d", d.Version)
	}
	if err := ValidateRootName(d.Name); err != nil {
		return fmt.Errorf("collections: invalid root descriptor name: %w", err)
	}
	if err := ValidateCollectionName(d.Collection); err != nil {
		return fmt.Errorf("collections: invalid root descriptor collection: %w", err)
	}
	switch d.Kind {
	case CollectionRootKindPrimary:
		if d.IndexName != "" {
			return errors.New("collections: primary root descriptor cannot have index name")
		}
	case CollectionRootKindSecondaryIndex:
		if err := ValidateIndexName(d.IndexName); err != nil {
			return fmt.Errorf("collections: invalid root descriptor index name: %w", err)
		}
	default:
		return fmt.Errorf("collections: unsupported root kind %d", d.Kind)
	}
	return nil
}

func (d *CollectionRootDescriptor) Encode() ([]byte, error) {
	if err := d.normalizeAndValidate(); err != nil {
		return nil, err
	}
	name := []byte(d.Name)
	collection := []byte(d.Collection)
	indexName := []byte(d.IndexName)
	if len(name) > 65535 || len(collection) > 65535 || len(indexName) > 65535 {
		return nil, errors.New("collections: root descriptor field too long")
	}
	flags := uint8(0)
	if d.Format.OuterLeavesInValueLog {
		flags |= 1 << 0
	}
	if d.Format.LeafPrefixCompression {
		flags |= 1 << 1
	}
	if d.Format.AllowValues {
		flags |= 1 << 2
	}
	out := make([]byte, 0, 64+len(name)+len(collection)+len(indexName))
	out = append(out, byte(collectionRootDescriptorVersion))
	out = append(out, byte(d.Kind))
	out = append(out, flags)
	out = binary.BigEndian.AppendUint16(out, uint16(len(name)))
	out = append(out, name...)
	out = binary.BigEndian.AppendUint16(out, uint16(len(collection)))
	out = append(out, collection...)
	out = binary.BigEndian.AppendUint16(out, uint16(len(indexName)))
	out = append(out, indexName...)
	out = binary.BigEndian.AppendUint64(out, d.RootPageID)
	return out, nil
}

func (d *CollectionRootDescriptor) Decode(raw []byte) error {
	if len(raw) < 17 {
		return errors.New("collections: truncated root descriptor")
	}
	ver := int(raw[0])
	if ver != collectionRootDescriptorVersion {
		return fmt.Errorf("collections: unsupported root descriptor version %d", ver)
	}
	kind := CollectionRootKind(raw[1])
	flags := raw[2]
	cursor := 3

	readString := func() (string, error) {
		if cursor+2 > len(raw) {
			return "", errors.New("collections: truncated root descriptor string length")
		}
		size := int(binary.BigEndian.Uint16(raw[cursor : cursor+2]))
		cursor += 2
		if cursor+size > len(raw) {
			return "", errors.New("collections: truncated root descriptor string payload")
		}
		value := string(raw[cursor : cursor+size])
		cursor += size
		return value, nil
	}

	name, err := readString()
	if err != nil {
		return err
	}
	collection, err := readString()
	if err != nil {
		return err
	}
	indexName, err := readString()
	if err != nil {
		return err
	}
	if cursor+8 != len(raw) {
		return errors.New("collections: malformed root descriptor payload")
	}
	rootPageID := binary.BigEndian.Uint64(raw[cursor : cursor+8])

	candidate := CollectionRootDescriptor{
		Version:    uint16(ver),
		Name:       name,
		Collection: collection,
		IndexName:  indexName,
		Kind:       kind,
		RootPageID: rootPageID,
		Format: CollectionRootFormat{
			OuterLeavesInValueLog: flags&(1<<0) != 0,
			LeafPrefixCompression: flags&(1<<1) != 0,
			AllowValues:           flags&(1<<2) != 0,
		},
	}
	if err := candidate.normalizeAndValidate(); err != nil {
		return err
	}
	*d = candidate
	return nil
}

func newPrimaryCollectionRootDescriptor(meta *CollectionMeta) (*CollectionRootDescriptor, error) {
	if meta == nil {
		return nil, errors.New("collections: nil collection metadata")
	}
	if err := meta.normalizeAndValidate(); err != nil {
		return nil, err
	}
	return &CollectionRootDescriptor{
		Name:       meta.PrimaryRoot,
		Collection: meta.Name,
		Kind:       CollectionRootKindPrimary,
		Format: CollectionRootFormat{
			OuterLeavesInValueLog: meta.Options.StorageMode == CollectionStorageModeOuterLeafInValueLog,
			LeafPrefixCompression: true,
			AllowValues:           true,
		},
	}, nil
}

func newSecondaryCollectionRootDescriptor(collection string, def *IndexDefinition) (*CollectionRootDescriptor, error) {
	if def == nil {
		return nil, errors.New("collections: nil index definition")
	}
	if err := ValidateCollectionName(collection); err != nil {
		return nil, err
	}
	if err := ValidateIndexName(def.Name); err != nil {
		return nil, err
	}
	if err := ValidateRootName(def.RootName); err != nil {
		return nil, err
	}
	return &CollectionRootDescriptor{
		Name:       def.RootName,
		Collection: collection,
		IndexName:  def.Name,
		Kind:       CollectionRootKindSecondaryIndex,
		Format: CollectionRootFormat{
			OuterLeavesInValueLog: false,
			LeafPrefixCompression: true,
			AllowValues:           false,
		},
	}, nil
}

func setCollectionRootDescriptorOnBatch(b batch.Interface, desc *CollectionRootDescriptor) error {
	if b == nil {
		return errors.New("collections: nil batch")
	}
	if desc == nil {
		return errors.New("collections: nil root descriptor")
	}
	key, err := SystemCollectionRootKey(desc.Name)
	if err != nil {
		return err
	}
	encoded, err := desc.Encode()
	if err != nil {
		return err
	}
	return b.Set(key, encoded)
}
