package collections

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strings"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

const (
	typedGraphBaseControlPrefix   = "collections/typed-graph-base/v1/"
	typedGraphBaseControlHeader   = 12
	typedGraphBaseControlMaxBytes = 128 << 10
	typedGraphBaseMaxRoots        = 64
)

// Only bounded metadata is owned here. Page IDs live in ordinary collection
// root descriptors so vacuum can discover, deduplicate and remap every alias.
// The base catalog never contains another base, preventing recursive capture.
type typedGraphBaseAlias struct {
	meta  CollectionMeta
	roots map[string]uint64
}

func typedGraphBaseRootNames(meta CollectionMeta) ([]string, error) {
	if !columnStoreTypedScalarIndexesSupported(meta) || len(meta.VectorIndexes) != 1 || meta.VectorIndexes[0].Strategy != VectorIndexStrategyColumnGraph {
		return nil, fmt.Errorf("%w: typed graph lifecycle requires one column_graph index and supported typed schema", ErrColumnQueryPlanUnsupported)
	}
	if len(meta.Indexes) > typedGraphBaseMaxRoots-3 {
		return nil, errors.New("collections: typed graph base root count exceeds limit")
	}
	roots := make([]string, 0, len(meta.Indexes)+3)
	roots = append(roots, collectionPrimaryRootName(meta.Name), collectionColumnManifestRootName(meta.Name), collectionColumnRowLocatorRootName(meta.Name))
	for _, index := range meta.Indexes {
		roots = append(roots, collectionSecondaryRootName(meta.Name, index.Name))
	}
	sort.Strings(roots)
	return roots, nil
}

func typedGraphBaseAliasRootName(collection, root string) string {
	return collection + "/typed-graph-base/v1/" + strings.TrimPrefix(root, collection+"/")
}

func encodeTypedGraphBaseControl(meta CollectionMeta) ([]byte, error) {
	roots, err := typedGraphBaseRootNames(meta)
	if err != nil {
		return nil, err
	}
	raw, err := encodeNormalizedCollectionMeta(meta)
	if err != nil {
		return nil, err
	}
	if len(raw) > typedGraphBaseControlMaxBytes-typedGraphBaseControlHeader {
		return nil, errors.New("collections: typed graph base metadata exceeds limit")
	}
	out := make([]byte, typedGraphBaseControlHeader+len(raw))
	copy(out, "TGBA")
	binary.LittleEndian.PutUint16(out[4:6], 1)
	binary.LittleEndian.PutUint16(out[6:8], uint16(len(roots)))
	binary.LittleEndian.PutUint32(out[8:12], uint32(len(raw)))
	copy(out[12:], raw)
	return out, nil
}

func decodeTypedGraphBaseControl(raw []byte, collection string) (CollectionMeta, error) {
	if len(raw) < typedGraphBaseControlHeader || len(raw) > typedGraphBaseControlMaxBytes || !bytes.Equal(raw[:4], []byte("TGBA")) || binary.LittleEndian.Uint16(raw[4:6]) != 1 {
		return CollectionMeta{}, errors.New("collections: invalid typed graph base control header")
	}
	count := int(binary.LittleEndian.Uint16(raw[6:8]))
	if count < 3 || count > typedGraphBaseMaxRoots || uint64(binary.LittleEndian.Uint32(raw[8:12])) != uint64(len(raw)-typedGraphBaseControlHeader) {
		return CollectionMeta{}, errors.New("collections: invalid typed graph base control bounds")
	}
	meta, err := decodeCollectionMeta(raw[typedGraphBaseControlHeader:])
	if err != nil {
		return CollectionMeta{}, err
	}
	// The fixed input cap bounds the existing metadata decoder. Require its
	// canonical encoding too: duplicate/unknown fields cannot smuggle a second
	// interpretation or an arbitrary nested descriptor set into this record.
	canonical, err := encodeNormalizedCollectionMeta(meta)
	if err != nil || !bytes.Equal(canonical, raw[typedGraphBaseControlHeader:]) {
		return CollectionMeta{}, errors.New("collections: noncanonical typed graph base metadata")
	}
	roots, err := typedGraphBaseRootNames(meta)
	if err != nil || len(roots) != count || meta.Name != collection {
		return CollectionMeta{}, errors.New("collections: typed graph base control schema/name/root count mismatch")
	}
	cfg := meta.Options.ColumnStore
	if cfg.ActiveManifest == nil || cfg.RecoveryAuthoritativeManifest == nil || !columnManifestIdentityValueEqual(*cfg.ActiveManifest, *cfg.RecoveryAuthoritativeManifest) {
		return CollectionMeta{}, errors.New("collections: typed graph base control missing exact manifest identity")
	}
	return meta, nil
}

func typedGraphBaseSchemaMatches(base, current CollectionMeta) bool {
	if base.Options.ColumnStore == nil || current.Options.ColumnStore == nil {
		return false
	}
	baseCfg, currentCfg := base.Options.ColumnStore, current.Options.ColumnStore
	if baseCfg.ActiveManifest == nil || currentCfg.ActiveManifest == nil || baseCfg.ActiveManifest.Generation > currentCfg.ActiveManifest.Generation || baseCfg.RecoveryAuthoritativeAppliedCommandLSN > currentCfg.RecoveryAuthoritativeAppliedCommandLSN {
		return false
	}
	// These four fields describe changing physical coverage, not schema. Every
	// other normalized option/index definition remains part of the binding.
	cfg := *current.Options.ColumnStore
	cfg.ActiveManifest = base.Options.ColumnStore.ActiveManifest
	cfg.RecoveryAuthoritativeManifest = base.Options.ColumnStore.RecoveryAuthoritativeManifest
	cfg.RecoveryAuthoritativeAppliedCommandLSN = base.Options.ColumnStore.RecoveryAuthoritativeAppliedCommandLSN
	cfg.PhysicalMutationParts = base.Options.ColumnStore.PhysicalMutationParts
	current.Options.ColumnStore = &cfg
	return collectionMetaValuesEqual(base, current)
}

func loadTypedGraphBaseAlias(snap *backenddb.Snapshot, current CollectionMeta) (*typedGraphBaseAlias, error) {
	raw, found, err := getSystemValue(snap, typedGraphBaseControlPrefix+current.Name)
	if err != nil || !found {
		return nil, err
	}
	meta, err := decodeTypedGraphBaseControl(raw, current.Name)
	if err != nil {
		return nil, err
	}
	if !typedGraphBaseSchemaMatches(meta, current) {
		return nil, ErrVectorIndexSnapshotMismatch
	}
	names, err := typedGraphBaseRootNames(meta)
	if err != nil {
		return nil, err
	}
	base := &typedGraphBaseAlias{meta: meta, roots: make(map[string]uint64, len(names))}
	for _, name := range names {
		raw, found, err := getSystemValue(snap, systemCollectionRootKey(typedGraphBaseAliasRootName(meta.Name, name)))
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("collections: missing typed graph base alias for %q", name)
		}
		root, err := decodeRootID(raw)
		if err != nil {
			return nil, err
		}
		base.roots[name] = root
	}
	return base, nil
}

func (base *typedGraphBaseAlias) catalog(c *Collection, snap *backenddb.Snapshot) (*collectionCatalog, error) {
	if base == nil || snap == nil {
		return nil, ErrVectorIndexSnapshotMismatch
	}
	catalog := newCollectionCatalogWithOverlays(base.meta, base.roots, nil)
	catalog.pager = snap.Pager()
	if err := validateColumnStoreCatalogRoot(snap, catalog); err != nil {
		return nil, err
	}
	if len(base.meta.VectorIndexes) != 1 {
		return nil, ErrVectorIndexSnapshotMismatch
	}
	_, graph, _, err := c.columnVectorGraphPhysicalRowReaderSnapshotViewAtCatalog(base.meta.VectorIndexes[0].Name, snap, catalog)
	if err != nil {
		return nil, err
	}
	if graph.RowCount > 0 {
		for name, root := range base.roots {
			if root == 0 {
				return nil, fmt.Errorf("collections: nonempty typed graph base has empty root %q", name)
			}
		}
	}
	return catalog, nil
}
