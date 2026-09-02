package collections

import (
	"errors"
	"fmt"
	"math"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/typeddecode"
)

type columnVectorGraphPhysicalRowReaderOptions struct {
	// MaxDecodedBlocks is the maximum number of decoded column blocks retained
	// in the reader cache. Zero uses the underlying row reader default.
	MaxDecodedBlocks int
	// UseResourceQuantizedAssets loads scalar_u8 quantized assets through a
	// mapped-resource-backed handle so prepared collection readers can share one
	// immutable asset instead of retaining one heap copy per reader. Standalone
	// OpenVectorIndexSearcher readers leave this off to preserve the hot search
	// latency of the existing single heap-copy path on platforms where mmap-backed
	// random row access is slower.
	UseResourceQuantizedAssets bool
	// SkipQuantizedAssets lets a prepared collection search attach an already
	// validated shared scalar_u8 asset instead of re-opening one per reader.
	SkipQuantizedAssets bool
	// detachCatalog copies immutable catalog metadata for readers returned after
	// their assembly snapshot closes.
	detachCatalog bool
}

const (
	// These positions must match the ProjectedColumns order in
	// openColumnVectorGraphPhysicalRowReader.
	columnVectorGraphPhysicalRowValueVector = iota
	columnVectorGraphPhysicalRowValueInvNorm
	columnVectorGraphPhysicalRowValueAdjacency
	columnVectorGraphPhysicalRowValueCount
)

var (
	errColumnVectorGraphAdjacencyOrdinalOutOfBounds      = errors.New("column_graph adjacency ordinal outside row_count")
	errColumnVectorGraphManifestMismatch                 = errors.New("column_graph manifest mismatch")
	errNilColumnVectorGraphPhysicalRowReader             = errors.New("nil column vector graph physical row reader")
	errColumnVectorGraphPhysicalRowReaderBatchVisitorNil = errors.New("collections: column_graph physical row reader batch visitor is nil")
)

// columnVectorGraphPhysicalRowReader binds column_graph search state by ordinal.
// Current rebuilds use TVIS/base typed-column sources and may have no physical
// graph row reader; legacy compatibility records still expose graph rows through
// the generic physical row reader when a physical asset is present.
//
// The reader is not concurrency-safe. Parallel native search uses one reader
// and one scratch per worker over immutable bound graph state.
type columnVectorGraphPhysicalRowReader struct {
	def                                 VectorIndexDefinition
	graph                               columnVectorGraphManifestSnapshot
	catalog                             *collectionCatalog
	reader                              *columnPhysicalRowReader
	typedVectorSource                   *columnVectorGraphTypedColumnVectorSource
	typedVectorFallbackReason           string
	invNormSource                       *columnVectorGraphInvNormStateSource
	invNormStateUnavailable             bool
	invNormStateFallbackReason          typeddecode.Reason
	rowRefSource                        *columnVectorGraphRowRefStateSource
	rowRefStateUnavailable              bool
	rowRefStateFallbackReason           typeddecode.Reason
	documentIDSource                    *columnVectorGraphDocumentIDStateSource
	documentIDStateFallbackReason       typeddecode.Reason
	quantizedAssetStatus                map[string]columnVectorGraphQuantizedAssetLoadStatus
	useResourceQuantizedAssets          bool
	skipQuantizedAssets                 bool
	hnswSearchPack                      *columnHNSWSearchPackPreparedView
	hnswSearchPackStatus                columnHNSWSearchPackPreparedStatus
	hnswSearchPackOpenNanos             uint64
	preparedSearch                      *columnVectorGraphPreparedSearchView
	sharedPreparedSearch                *columnVectorGraphSharedPreparedSearchRef
	adjacencyLayerSources               *columnVectorGraphAdjacencyDirectSources
	layer0AdjacencySource               *columnVectorGraphLayer0AdjacencyDirectSource
	layer0AdjacencySourceUnavailable    bool
	layer0AdjacencySourceFallbackReason typeddecode.Reason
}

// columnVectorGraphPhysicalRow aliases caller-owned scratch and cached asset
// bytes. Copy ID, Vector, or Adjacency before the next fetch if retention is
// required.
type columnVectorGraphPhysicalRow struct {
	Ordinal   int
	RowIndex  int
	ID        []byte
	Vector    []float32
	InvNorm   float32
	Adjacency []uint32
}

func (c *Collection) openColumnVectorGraphPhysicalRowReader(name string, opts columnVectorGraphPhysicalRowReaderOptions) (*columnVectorGraphPhysicalRowReader, error) {
	snap, err := c.acquireColumnVectorGraphPhysicalRowReaderSnapshot()
	if err != nil {
		return nil, err
	}
	defer func() { _ = snap.Close() }()
	opts.detachCatalog = true
	return c.openColumnVectorGraphPhysicalRowReaderAtSnapshot(name, snap, opts)
}

// openColumnVectorGraphPhysicalRowReaderAtSnapshot binds the returned reader to
// the caller-owned snapshot unless opts.detachCatalog requests an owned catalog
// copy for readers returned after snapshot close.
func (c *Collection) openColumnVectorGraphPhysicalRowReaderAtSnapshot(name string, snap *backenddb.Snapshot, opts columnVectorGraphPhysicalRowReaderOptions) (*columnVectorGraphPhysicalRowReader, error) {
	def, graph, view, err := c.columnVectorGraphPhysicalRowReaderSnapshotViewAtSnapshot(name, snap)
	if err != nil {
		return nil, err
	}
	catalog := view.Catalog
	if opts.detachCatalog && catalog != nil {
		catalog = catalog.copy()
		view.Catalog = catalog
	}
	var reader *columnPhysicalRowReader
	if columnVectorGraphManifestHasPhysicalAsset(graph) {
		reader, err = newColumnPhysicalRowReaderFromSnapshotView(view, columnPhysicalRowReaderOptions{
			ProjectedColumns: []string{
				columnVectorGraphVectorColumnName,
				columnVectorGraphInvNormColumnName,
				columnVectorGraphAdjacencyColumnName,
			},
			MaxDecodedBlocks:  opts.MaxDecodedBlocks,
			RequireInsertOnly: true,
		})
		if err != nil {
			return nil, err
		}
		if got, want := reader.RowCount(), graph.RowCount; got != want {
			_ = reader.Close()
			return nil, fmt.Errorf("collections: column_graph %q manifest row_count=%d physical_row_count=%d: %w", def.Name, want, got, errColumnVectorGraphManifestMismatch)
		}
	}
	graphReader := &columnVectorGraphPhysicalRowReader{
		def:                        def,
		graph:                      graph,
		catalog:                    catalog,
		reader:                     reader,
		quantizedAssetStatus:       make(map[string]columnVectorGraphQuantizedAssetLoadStatus),
		useResourceQuantizedAssets: opts.UseResourceQuantizedAssets,
		skipQuantizedAssets:        opts.SkipQuantizedAssets,
	}
	if !columnVectorGraphManifestHasPhysicalAsset(graph) && graph.RowCount > 0 && catalog != nil && view.VectorIndexStateFound && view.AssetNamespace != "" {
		key, keyErr := columnVectorGraphSharedPreparedSearchCacheKey(catalog.meta.Name, view.AssetNamespace, def, graph, view.VectorIndexState)
		if keyErr != nil {
			_ = graphReader.Close()
			return nil, keyErr
		}
		shared, err := c.acquireColumnVectorGraphSharedPreparedSearch(key, func() (*columnVectorGraphSharedPreparedSearch, error) {
			buildReader := &columnVectorGraphPhysicalRowReader{def: def, graph: graph, catalog: catalog, quantizedAssetStatus: make(map[string]columnVectorGraphQuantizedAssetLoadStatus), skipQuantizedAssets: true}
			success := false
			defer func() {
				if !success {
					_ = buildReader.Close()
				}
			}()
			if err := c.prepareColumnVectorGraphPhysicalRowReaderSourcesAtSnapshot(buildReader, snap, view); err != nil {
				return nil, err
			}
			holder, err := newColumnVectorGraphSharedPreparedSearchFromReader(buildReader)
			if err != nil {
				return nil, err
			}
			success = true
			return holder, nil
		})
		if err != nil {
			if errors.Is(err, errColumnVectorGraphSharedPreparedSearchNotEligible) {
				if prepErr := c.prepareColumnVectorGraphPhysicalRowReaderSourcesAtSnapshot(graphReader, snap, view); prepErr != nil {
					_ = graphReader.Close()
					return nil, prepErr
				}
				return graphReader, nil
			}
			_ = graphReader.Close()
			return nil, err
		}
		if err := graphReader.attachSharedPreparedSearch(shared); err != nil {
			releaseErr := shared.release()
			_ = graphReader.Close()
			return nil, errors.Join(err, releaseErr)
		}
		if !graphReader.skipQuantizedAssets {
			c.prepareColumnVectorGraphQuantizedAssetsForReader(graphReader, view)
		}
		return graphReader, nil
	}
	if err := c.prepareColumnVectorGraphPhysicalRowReaderSourcesAtSnapshot(graphReader, snap, view); err != nil {
		_ = graphReader.Close()
		return nil, err
	}
	return graphReader, nil
}

func (c *Collection) prepareColumnVectorGraphPhysicalRowReaderSourcesAtSnapshot(graphReader *columnVectorGraphPhysicalRowReader, snap *backenddb.Snapshot, view columnPhysicalScanSnapshotView) error {
	if graphReader == nil {
		return errNilColumnVectorGraphPhysicalRowReader
	}
	catalog := graphReader.catalog
	def := graphReader.def
	graph := graphReader.graph
	if catalog != nil && catalog.meta.Options.ColumnStore != nil {
		baseCfg := catalog.meta.Options.ColumnStore
		var baseManifest columnManifestSnapshot
		var baseRecords []columnManifestRecord
		var baseManifestLoaded bool
		loadBaseManifestRecords := func() (columnManifestSnapshot, []columnManifestRecord, error) {
			if baseManifestLoaded {
				return baseManifest, baseRecords, nil
			}
			rootID := catalog.rootID(collectionColumnManifestRootName(catalog.meta.Name))
			records, err := loadColumnManifestRecordsFromRoot(snap, rootID)
			if err != nil {
				return columnManifestSnapshot{}, nil, err
			}
			manifest, err := decodeColumnManifestSnapshotForScan(records)
			if err != nil {
				return columnManifestSnapshot{}, nil, err
			}
			baseManifest = manifest
			baseRecords = records
			baseManifestLoaded = true
			return baseManifest, baseRecords, nil
		}
		state := view.VectorIndexState
		if !view.VectorIndexStateFound {
			return fmt.Errorf("collections: column_graph %q missing vector-index state record: %w", def.Name, errColumnVectorGraphManifestMismatch)
		}
		if sources, _, sourceErr := c.openColumnVectorGraphAdjacencyStateSourcesForReader(catalog.meta.Name, *baseCfg, def, graph, state); sourceErr != nil {
			return sourceErr
		} else if sources != nil {
			graphReader.adjacencyLayerSources = sources
			if len(sources.sources) > 0 {
				graphReader.layer0AdjacencySource = sources.sources[0]
			}
		} else {
			return fmt.Errorf("collections: column_graph %q missing vector-index adjacency state source: %w", def.Name, errColumnVectorGraphManifestMismatch)
		}
		if _, ok := findColumnVectorGraphInvNormStateAsset(state); ok {
			source, fallbackReason, sourceErr := c.openColumnVectorGraphInvNormStateSourceForReader(catalog.meta.Name, *baseCfg, def, graph, state)
			if sourceErr != nil {
				return sourceErr
			}
			graphReader.invNormSource = source
			graphReader.invNormStateFallbackReason = fallbackReason
		} else {
			graphReader.invNormStateUnavailable = true
		}
		if columnVectorGraphRowRefStatePresent(state) {
			_, records, recordsErr := loadBaseManifestRecords()
			if recordsErr != nil {
				return recordsErr
			}
			source, sourceErr := c.openColumnVectorGraphRowRefStateSourceForReader(catalog.meta.Name, *baseCfg, def, graph, state, records)
			if sourceErr != nil {
				return sourceErr
			}
			graphReader.rowRefSource = source
		} else {
			graphReader.rowRefStateUnavailable = true
		}
		if columnVectorGraphDocumentIDStatePresent(state) {
			source, fallbackReason, sourceErr := c.openColumnVectorGraphDocumentIDStateSourceForReader(catalog.meta.Name, *baseCfg, def, graph, state)
			if sourceErr != nil {
				graphReader.documentIDStateFallbackReason = fallbackReason
				if graphReader.documentIDStateFallbackReason == "" {
					graphReader.documentIDStateFallbackReason = typeddecode.ReasonValidationFailed
				}
			} else {
				graphReader.documentIDSource = source
			}
		}
		if _, _, typedVectorOwner, ownerErr := columnVectorGraphTypedColumnVectorField(*baseCfg, graph.Field, graph.Dimensions); ownerErr != nil {
			graphReader.typedVectorFallbackReason = ownerErr.Error()
		} else if typedVectorOwner {
			manifest, records, recordsErr := loadBaseManifestRecords()
			if recordsErr != nil {
				graphReader.typedVectorFallbackReason = recordsErr.Error()
			} else if source, fallbackReason := c.openColumnVectorGraphTypedColumnVectorSourceForReader(catalog, *baseCfg, manifest, records, graph, graphReader); source != nil {
				graphReader.typedVectorSource = source
			} else if fallbackReason != "" {
				graphReader.typedVectorFallbackReason = fallbackReason
			}
		}
		pack, packStatus, packOpenNanos, packErr := c.openColumnHNSWSearchPackPreparedViewForReader(catalog.meta.Name, *baseCfg, def, graph, state)
		graphReader.hnswSearchPack = pack
		graphReader.hnswSearchPackStatus = packStatus
		graphReader.hnswSearchPackOpenNanos = packOpenNanos
		if packErr != nil {
			graphReader.hnswSearchPackStatus = columnHNSWSearchPackPreparedStatusInvalid
		}
		if !graphReader.skipQuantizedAssets {
			c.prepareColumnVectorGraphQuantizedAssetsForReader(graphReader, view)
		}
	}
	if !columnVectorGraphManifestHasPhysicalAsset(graph) && graph.RowCount > 0 {
		if graphReader.invNormSource == nil {
			return fmt.Errorf("collections: column_graph %q missing required vector-index inverse-norm state source: %w", def.Name, errColumnVectorGraphManifestMismatch)
		}
		if graphReader.rowRefSource == nil {
			return fmt.Errorf("collections: column_graph %q missing required vector-index row-ref state source: %w", def.Name, errColumnVectorGraphManifestMismatch)
		}
		if graphReader.documentIDSource == nil {
			return fmt.Errorf("collections: column_graph %q missing required vector-index document-id state source: %w", def.Name, errColumnVectorGraphManifestMismatch)
		}
		if graphReader.typedVectorSource == nil {
			if graphReader.typedVectorFallbackReason != "" {
				return fmt.Errorf("collections: column_graph %q missing required base typed-column vector source: %s: %w", def.Name, graphReader.typedVectorFallbackReason, errColumnVectorGraphManifestMismatch)
			}
			return fmt.Errorf("collections: column_graph %q missing required base typed-column vector source: %w", def.Name, errColumnVectorGraphManifestMismatch)
		}
		if prepareErr := maybePrepareColumnVectorGraphPreparedSearchView(graphReader); prepareErr != nil {
			return fmt.Errorf("collections: column_graph %q combined prepared graph-search view: %w: %w", def.Name, prepareErr, errColumnVectorGraphManifestMismatch)
		}
	}
	return nil
}

func (c *Collection) columnVectorGraphPhysicalRowReaderSnapshotView(name string) (VectorIndexDefinition, columnVectorGraphManifestSnapshot, columnPhysicalScanSnapshotView, error) {
	snap, err := c.acquireColumnVectorGraphPhysicalRowReaderSnapshot()
	if err != nil {
		return VectorIndexDefinition{}, columnVectorGraphManifestSnapshot{}, columnPhysicalScanSnapshotView{}, err
	}
	defer func() { _ = snap.Close() }()
	return c.columnVectorGraphPhysicalRowReaderSnapshotViewAtSnapshot(name, snap)
}

func (c *Collection) acquireColumnVectorGraphPhysicalRowReaderSnapshot() (*backenddb.Snapshot, error) {
	if c == nil {
		return nil, errCollectionNil
	}
	if c.db == nil {
		return nil, errCollectionDBNil
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return nil, backenddb.ErrClosed
	}
	return snap, nil
}

func (c *Collection) columnVectorGraphPhysicalRowReaderSnapshotViewAtSnapshot(name string, snap *backenddb.Snapshot) (VectorIndexDefinition, columnVectorGraphManifestSnapshot, columnPhysicalScanSnapshotView, error) {
	if c == nil {
		return VectorIndexDefinition{}, columnVectorGraphManifestSnapshot{}, columnPhysicalScanSnapshotView{}, errCollectionNil
	}
	if c.db == nil {
		return VectorIndexDefinition{}, columnVectorGraphManifestSnapshot{}, columnPhysicalScanSnapshotView{}, errCollectionDBNil
	}
	if snap == nil {
		return VectorIndexDefinition{}, columnVectorGraphManifestSnapshot{}, columnPhysicalScanSnapshotView{}, backenddb.ErrClosed
	}
	catalog, err := c.catalogForSnapshot(snap)
	if err != nil {
		return VectorIndexDefinition{}, columnVectorGraphManifestSnapshot{}, columnPhysicalScanSnapshotView{}, err
	}
	if catalog == nil {
		return VectorIndexDefinition{}, columnVectorGraphManifestSnapshot{}, columnPhysicalScanSnapshotView{}, errCollectionNotFound
	}
	def, ok := findVectorIndex(catalog.meta.VectorIndexes, name)
	if !ok {
		return VectorIndexDefinition{}, columnVectorGraphManifestSnapshot{}, columnPhysicalScanSnapshotView{}, ErrIndexNotFound
	}
	if def.Strategy != VectorIndexStrategyColumnGraph {
		return VectorIndexDefinition{}, columnVectorGraphManifestSnapshot{}, columnPhysicalScanSnapshotView{}, fmt.Errorf("collections: vector index %q strategy=%q is not column_graph", def.Name, def.Strategy)
	}
	cfg := catalog.meta.Options.ColumnStore
	if cfg == nil || !cfg.Enabled || cfg.AssetManager == nil {
		return VectorIndexDefinition{}, columnVectorGraphManifestSnapshot{}, columnPhysicalScanSnapshotView{}, errors.New("collections: column_graph physical row reader requires physical column asset support")
	}
	if cfg.ActiveManifest == nil || cfg.RecoveryAuthoritativeManifest == nil {
		return VectorIndexDefinition{}, columnVectorGraphManifestSnapshot{}, columnPhysicalScanSnapshotView{}, errors.New("collections: column_graph physical row reader requires active and recovery-authoritative column manifests")
	}
	if !columnManifestIdentityValueEqual(*cfg.ActiveManifest, *cfg.RecoveryAuthoritativeManifest) {
		return VectorIndexDefinition{}, columnVectorGraphManifestSnapshot{}, columnPhysicalScanSnapshotView{}, errors.New("collections: column_graph physical row reader requires active recovery-authoritative column manifest")
	}
	rootID := catalog.rootID(collectionColumnManifestRootName(catalog.meta.Name))
	if rootID == 0 {
		return VectorIndexDefinition{}, columnVectorGraphManifestSnapshot{}, columnPhysicalScanSnapshotView{}, fmt.Errorf("collections: column_graph physical row reader missing manifest root %q", collectionColumnManifestRootName(catalog.meta.Name))
	}
	if err := validateColumnManifestIdentityAtRoot(snap, rootID, *cfg.ActiveManifest); err != nil {
		return VectorIndexDefinition{}, columnVectorGraphManifestSnapshot{}, columnPhysicalScanSnapshotView{}, err
	}
	records, err := loadColumnManifestRecordsFromRoot(snap, rootID)
	if err != nil {
		return VectorIndexDefinition{}, columnVectorGraphManifestSnapshot{}, columnPhysicalScanSnapshotView{}, err
	}
	manifest, err := decodeColumnManifestSnapshotForScan(records)
	if err != nil {
		return VectorIndexDefinition{}, columnVectorGraphManifestSnapshot{}, columnPhysicalScanSnapshotView{}, err
	}
	if err := validateColumnManifestSnapshot(manifest, records, *cfg, *cfg.ActiveManifest, catalog.meta.Name, "column vector graph row reader"); err != nil {
		return VectorIndexDefinition{}, columnVectorGraphManifestSnapshot{}, columnPhysicalScanSnapshotView{}, err
	}
	graphRecord, ok := findColumnVectorGraphManifestRecord(records, def.Name)
	if !ok {
		return VectorIndexDefinition{}, columnVectorGraphManifestSnapshot{}, columnPhysicalScanSnapshotView{}, fmt.Errorf("collections: column_graph %q has no published graph manifest", def.Name)
	}
	graph, err := decodeColumnVectorGraphManifestRecord(graphRecord.value)
	if err != nil {
		return VectorIndexDefinition{}, columnVectorGraphManifestSnapshot{}, columnPhysicalScanSnapshotView{}, err
	}
	baseChecksum, err := columnVectorGraphBaseManifestChecksum(manifest, records, *cfg)
	if err != nil {
		return VectorIndexDefinition{}, columnVectorGraphManifestSnapshot{}, columnPhysicalScanSnapshotView{}, fmt.Errorf("collections: column_graph %q base manifest checksum unavailable: %w: %v", def.Name, errColumnVectorGraphManifestMismatch, err)
	}
	stateRecord, ok := findColumnVectorIndexStateRecord(records, def.Name)
	if !ok {
		return VectorIndexDefinition{}, columnVectorGraphManifestSnapshot{}, columnPhysicalScanSnapshotView{}, fmt.Errorf("collections: column_graph %q missing vector-index state record: %w", def.Name, errColumnVectorGraphManifestMismatch)
	}
	vectorState, err := decodeColumnVectorIndexStateRecord(stateRecord.value)
	if err != nil {
		return VectorIndexDefinition{}, columnVectorGraphManifestSnapshot{}, columnPhysicalScanSnapshotView{}, err
	}
	if columnVectorIndexStateMatchStatusWithBaseChecksum(vectorState, def, *cfg, baseChecksum) != columnVectorIndexStateMatchLoaded || !columnVectorIndexStateMatchesGraph(vectorState, graph) {
		return VectorIndexDefinition{}, columnVectorGraphManifestSnapshot{}, columnPhysicalScanSnapshotView{}, fmt.Errorf("collections: column_graph %q vector-index state does not match graph manifest: %w", def.Name, errColumnVectorGraphManifestMismatch)
	}
	// Keep snapshot-view validation cheap: this path is also used by status/open
	// setup. The adjacency state source binder below performs the single full
	// typed-list payload validation/read when it acquires offset/value sections.
	if err := validateColumnVectorIndexStateAssetsForStatus(c.db.ColumnAssetRootDir(), catalog.meta.Name, *cfg, def, vectorState, graph); err != nil {
		return VectorIndexDefinition{}, columnVectorGraphManifestSnapshot{}, columnPhysicalScanSnapshotView{}, err
	}
	if err := validateColumnVectorGraphInvNormStateAssetIfPresent(c.db.ColumnAssetRootDir(), catalog.meta.Name, *cfg, def, graph, vectorState); err != nil {
		return VectorIndexDefinition{}, columnVectorGraphManifestSnapshot{}, columnPhysicalScanSnapshotView{}, err
	}
	if columnVectorGraphManifestMatchStatusWithBaseChecksum(catalog.meta.Name, graph, def, *cfg, baseChecksum) != columnVectorGraphManifestMatchLoaded {
		return VectorIndexDefinition{}, columnVectorGraphManifestSnapshot{}, columnPhysicalScanSnapshotView{}, fmt.Errorf("collections: column_graph %q graph manifest does not match vector index definition: %w", def.Name, errColumnVectorGraphManifestMismatch)
	}
	if columnVectorGraphManifestHasPhysicalAsset(graph) {
		if err := validateColumnVectorGraphAssetRefAvailable(c.db.ColumnAssetRootDir(), graph.AssetRef); err != nil {
			return VectorIndexDefinition{}, columnVectorGraphManifestSnapshot{}, columnPhysicalScanSnapshotView{}, err
		}
	}
	graphCfg, err := columnVectorGraphPhysicalColumnStoreConfig(catalog.meta.Name, *cfg, def)
	if err != nil {
		return VectorIndexDefinition{}, columnVectorGraphManifestSnapshot{}, columnPhysicalScanSnapshotView{}, err
	}
	graphCfg.ActiveManifest = cfg.ActiveManifest
	graphCfg.RecoveryAuthoritativeManifest = cfg.RecoveryAuthoritativeManifest
	graphCfg.RecoveryAuthoritativeAppliedCommandLSN = cfg.RecoveryAuthoritativeAppliedCommandLSN
	state, ok := snap.StateToken()
	if !ok {
		return VectorIndexDefinition{}, columnVectorGraphManifestSnapshot{}, columnPhysicalScanSnapshotView{}, backenddb.ErrClosed
	}
	assetRefs := make([]columnManifestAssetRefForScan, 0, 1)
	if columnVectorGraphManifestHasPhysicalAsset(graph) {
		assetRefs = append(assetRefs, columnManifestAssetRefForScan{
			Ref:    graph.AssetRef,
			Reason: ColumnPublishOperationInsert,
		})
	}
	view := columnPhysicalScanSnapshotView{
		CollectionName:        catalog.meta.Name,
		Catalog:               catalog,
		Config:                graphCfg,
		ColumnStoreEnabled:    true,
		CommitSeq:             state.CommitSeq,
		VectorIndexState:      vectorState,
		VectorIndexStateFound: true,
		AssetRefs:             assetRefs,
		Diagnostics: columnPhysicalScanDiagnostics{
			ManifestRoot:               rootID,
			ManifestGeneration:         cfg.ActiveManifest.Generation,
			RecoveryManifestGeneration: cfg.RecoveryAuthoritativeManifest.Generation,
			AppliedCommandLSN:          cfg.RecoveryAuthoritativeAppliedCommandLSN,
			ManifestRecords:            len(records),
			AssetRefs:                  len(assetRefs),
		},
		ColumnAssetRootDir: c.db.ColumnAssetRootDir(),
		AssetNamespace:     graphCfg.AssetManager.Namespace,
	}
	return def, graph, view, nil
}

func (r *columnVectorGraphPhysicalRowReader) Close() error {
	if r == nil {
		return nil
	}
	var closeErr error
	if r.sharedPreparedSearch != nil {
		if err := r.releaseSharedPreparedSearch(); err != nil {
			closeErr = err
		}
	}
	if r.typedVectorSource != nil {
		if err := r.typedVectorSource.Close(); err != nil {
			closeErr = err
		}
		r.typedVectorSource = nil
	}
	if r.invNormSource != nil {
		if err := r.invNormSource.Close(); closeErr == nil && err != nil {
			closeErr = err
		}
		r.invNormSource = nil
	}
	if r.rowRefSource != nil {
		if err := r.rowRefSource.Close(); closeErr == nil && err != nil {
			closeErr = err
		}
		r.rowRefSource = nil
	}
	if r.documentIDSource != nil {
		if err := r.documentIDSource.Close(); closeErr == nil && err != nil {
			closeErr = err
		}
		r.documentIDSource = nil
	}
	if r.hnswSearchPack != nil {
		if err := r.hnswSearchPack.Close(); closeErr == nil && err != nil {
			closeErr = err
		}
		r.hnswSearchPack = nil
	}
	r.preparedSearch = nil
	if r.quantizedAssetStatus != nil {
		if err := closeColumnVectorGraphQuantizedAssetLoadStatuses(r.quantizedAssetStatus); closeErr == nil && err != nil {
			closeErr = err
		}
		r.quantizedAssetStatus = nil
	}
	if r.adjacencyLayerSources != nil {
		if err := r.adjacencyLayerSources.Close(); closeErr == nil && err != nil {
			closeErr = err
		}
		r.adjacencyLayerSources = nil
		r.layer0AdjacencySource = nil
	} else if r.layer0AdjacencySource != nil {
		if err := r.layer0AdjacencySource.Close(); closeErr == nil && err != nil {
			closeErr = err
		}
		r.layer0AdjacencySource = nil
	}
	if r.reader != nil {
		if err := r.reader.Close(); closeErr == nil && err != nil {
			closeErr = err
		}
	}
	return closeErr
}

func (r *columnVectorGraphPhysicalRowReader) RowCount() int {
	if r == nil {
		return 0
	}
	if r.reader != nil {
		return r.reader.RowCount()
	}
	return r.graph.RowCount
}

func (r *columnVectorGraphPhysicalRowReader) Stats() columnPhysicalRowReaderStats {
	if r == nil || r.reader == nil {
		return columnPhysicalRowReaderStats{}
	}
	return r.reader.Stats()
}

func (r *columnVectorGraphPhysicalRowReader) rowReader() (*columnPhysicalRowReader, error) {
	if r == nil || r.reader == nil {
		return nil, errNilColumnVectorGraphPhysicalRowReader
	}
	return r.reader, nil
}

// FetchRow returns one graph row and performs fail-closed adjacency ordinal
// validation. Native search uses fetchRowUnchecked and validates each edge once
// when expanded to avoid duplicate adjacency scans on the hot path.
func (r *columnVectorGraphPhysicalRowReader) FetchRow(ordinal int, scratch *columnPhysicalRowReaderScratch) (columnVectorGraphPhysicalRow, error) {
	reader, err := r.rowReader()
	if err != nil {
		return columnVectorGraphPhysicalRow{}, err
	}
	row, err := reader.FetchRow(ordinal, scratch)
	if err != nil {
		return columnVectorGraphPhysicalRow{}, err
	}
	return r.graphRowFromPhysicalRow(row, reader.RowCount())
}

func (r *columnVectorGraphPhysicalRowReader) fetchRowUnchecked(ordinal int, scratch *columnPhysicalRowReaderScratch) (columnVectorGraphPhysicalRow, error) {
	return r.fetchNativeRowUnchecked(ordinal, scratch)
}

func (r *columnVectorGraphPhysicalRowReader) fetchNativeRowUnchecked(ordinal int, scratch *columnPhysicalRowReaderScratch) (columnVectorGraphPhysicalRow, error) {
	reader, err := r.rowReader()
	if err != nil {
		return columnVectorGraphPhysicalRow{}, err
	}
	if reader.closed {
		return columnVectorGraphPhysicalRow{}, errors.New("collections: physical column row reader is closed")
	}
	if scratch == nil {
		return columnVectorGraphPhysicalRow{}, errors.New("collections: physical column row reader requires caller-owned scratch")
	}
	reader.stats.RowFetches++
	row, err := r.nativeRowFromReaderOrdinal(reader, ordinal, scratch)
	if err != nil {
		return columnVectorGraphPhysicalRow{}, err
	}
	reader.stats.RowsFetched++
	return row, nil
}

// FetchBatch returns graph rows in the underlying reader's batch order and
// performs fail-closed adjacency ordinal validation for each row. Native search
// uses fetchBatchUnchecked for final result fetches after validating expanded
// edges during traversal.
func (r *columnVectorGraphPhysicalRowReader) FetchBatch(ordinals []int, scratch *columnPhysicalRowReaderScratch, visitor func(columnVectorGraphPhysicalRow) error) error {
	reader, err := r.rowReader()
	if err != nil {
		return err
	}
	if visitor == nil {
		return errColumnVectorGraphPhysicalRowReaderBatchVisitorNil
	}
	rowCount := reader.RowCount()
	return reader.FetchBatch(ordinals, scratch, func(row columnPhysicalRowReaderRow) error {
		graphRow, err := r.graphRowFromPhysicalRow(row, rowCount)
		if err != nil {
			return err
		}
		return visitor(graphRow)
	})
}

func (r *columnVectorGraphPhysicalRowReader) fetchBatchUnchecked(ordinals []int, scratch *columnPhysicalRowReaderScratch, visitor func(columnVectorGraphPhysicalRow) error) error {
	reader, err := r.rowReader()
	if err != nil {
		return err
	}
	if reader.closed {
		return errors.New("collections: physical column row reader is closed")
	}
	if scratch == nil {
		return errors.New("collections: physical column row reader requires caller-owned scratch")
	}
	if visitor == nil {
		return errColumnVectorGraphPhysicalRowReaderBatchVisitorNil
	}
	reader.stats.BatchFetches++
	for _, ordinal := range ordinals {
		graphRow, err := r.nativeRowFromReaderOrdinal(reader, ordinal, scratch)
		if err != nil {
			return err
		}
		reader.stats.RowsFetched++
		if err := visitor(graphRow); err != nil {
			return err
		}
	}
	return nil
}

func (r *columnVectorGraphPhysicalRowReader) nativeRowFromReaderOrdinal(reader *columnPhysicalRowReader, ordinal int, scratch *columnPhysicalRowReaderScratch) (columnVectorGraphPhysicalRow, error) {
	rangeIdx := reader.rangeIndexForOrdinal(ordinal)
	if rangeIdx < 0 {
		return columnVectorGraphPhysicalRow{}, fmt.Errorf("collections: physical column row ordinal=%d outside row_count=%d: %w", ordinal, reader.totalRows, errColumnPhysicalRowOrdinalOutOfBounds)
	}
	rowRange := reader.ranges[rangeIdx]
	block, err := reader.loadBlock(rowRange)
	if err != nil {
		return columnVectorGraphPhysicalRow{}, err
	}
	rowIndex := ordinal - rowRange.startOrdinal
	if rowIndex < 0 || rowIndex >= len(block.rowOffsets) {
		return columnVectorGraphPhysicalRow{}, fmt.Errorf("collections: physical column row index=%d outside block rows=%d", rowIndex, len(block.rowOffsets))
	}
	return r.nativeGraphRowFromBlock(block, ordinal, rowIndex, scratch)
}

func (r *columnVectorGraphPhysicalRowReader) nativeGraphRowFromBlock(block *columnPhysicalRowReaderBlock, ordinal, rowIndex int, scratch *columnPhysicalRowReaderScratch) (columnVectorGraphPhysicalRow, error) {
	cur := manifestCursor{raw: block.raw, pos: block.rowOffsets[rowIndex]}
	id := cur.bytesView()
	deleted := false
	if block.version >= columnPhysicalAssetVersionV2 {
		deleted = cur.bool()
	}
	if cur.err != nil {
		return columnVectorGraphPhysicalRow{}, cur.err
	}
	if len(id) == 0 {
		return columnVectorGraphPhysicalRow{}, fmt.Errorf("collections: column_graph %q ordinal=%d missing document id", r.def.Name, ordinal)
	}
	if deleted {
		if block.header.Operation != ColumnPublishOperationDelete {
			return columnVectorGraphPhysicalRow{}, fmt.Errorf("column physical asset %s row[%d] is marked deleted", block.header.Operation, rowIndex)
		}
		return columnVectorGraphPhysicalRow{}, fmt.Errorf("collections: column_graph %q ordinal=%d row is deleted", r.def.Name, ordinal)
	}
	if block.header.Operation == ColumnPublishOperationDelete {
		return columnVectorGraphPhysicalRow{}, fmt.Errorf("column physical asset delete row[%d] is not marked deleted", rowIndex)
	}

	scratch.Values = scratch.Values[:0]
	scratch.Float32Values = scratch.Float32Values[:0]
	scratch.Uint32Values = scratch.Uint32Values[:0]

	vector, _, _, typedVectorOK := r.typedVectorForOrdinal(ordinal)
	if typedVectorOK {
		if err := r.skipNativeGraphVector(&cur, block.version, ordinal); err != nil {
			return columnVectorGraphPhysicalRow{}, fmt.Errorf("row[%d]: %w", rowIndex, err)
		}
	} else {
		var err error
		vector, err = r.readNativeGraphVector(&cur, block.version, ordinal, scratch)
		if err != nil {
			return columnVectorGraphPhysicalRow{}, fmt.Errorf("row[%d]: %w", rowIndex, err)
		}
	}
	invNorm, err := r.readNativeGraphInvNorm(&cur, block.version, ordinal)
	if err != nil {
		return columnVectorGraphPhysicalRow{}, fmt.Errorf("row[%d]: %w", rowIndex, err)
	}
	adjacency, err := r.readNativeGraphAdjacency(&cur, block.version, ordinal, scratch)
	if err != nil {
		return columnVectorGraphPhysicalRow{}, fmt.Errorf("row[%d]: %w", rowIndex, err)
	}
	if cur.err != nil {
		return columnVectorGraphPhysicalRow{}, cur.err
	}
	if invNorm <= 0 || math.IsNaN(float64(invNorm)) || math.IsInf(float64(invNorm), 0) {
		return columnVectorGraphPhysicalRow{}, fmt.Errorf("collections: column_graph %q ordinal=%d invalid inv_norm=%v", r.def.Name, ordinal, invNorm)
	}
	return columnVectorGraphPhysicalRow{
		Ordinal:   ordinal,
		RowIndex:  rowIndex,
		ID:        id,
		Vector:    vector,
		InvNorm:   invNorm,
		Adjacency: adjacency,
	}, nil
}

func (r *columnVectorGraphPhysicalRowReader) readNativeGraphVector(cur *manifestCursor, version uint16, ordinal int, scratch *columnPhysicalRowReaderScratch) ([]float32, error) {
	if err := r.readNativeGraphValueHeader(cur, version, ordinal, 0, ColumnStoreValueFloat32Vector); err != nil {
		return nil, err
	}
	n := cur.u64()
	if cur.err != nil {
		return nil, cur.err
	}
	if n != uint64(r.def.Dimensions) {
		return nil, fmt.Errorf("collections: column_graph %q ordinal=%d vector dims=%d want %d", r.def.Name, ordinal, n, r.def.Dimensions)
	}
	byteLen, ok := cur.fixedWidthSliceByteLen(n, 4, "float32_vector")
	if !ok {
		return nil, cur.err
	}
	pos := cur.pos
	end := pos + int(byteLen)
	if int(byteLen) == 0 {
		cur.pos = end
		return nil, nil
	}
	base := len(scratch.Float32Values)
	need := base + int(n)
	if cap(scratch.Float32Values) < need {
		next := make([]float32, need)
		copy(next, scratch.Float32Values)
		scratch.Float32Values = next
	} else {
		scratch.Float32Values = scratch.Float32Values[:need]
	}
	_ = cur.raw[end-1]
	if columnPhysicalNativeLittleEndian {
		columnPhysicalCopyLittleEndianFloat32Bytes(scratch.Float32Values[base:need], cur.raw[pos:end])
		cur.pos = end
		return scratch.Float32Values[base:], nil
	}
	for i := base; i < need; i++ {
		scratch.Float32Values[i] = math.Float32frombits(uint32(cur.raw[pos]) | uint32(cur.raw[pos+1])<<8 | uint32(cur.raw[pos+2])<<16 | uint32(cur.raw[pos+3])<<24)
		pos += 4
	}
	cur.pos = end
	return scratch.Float32Values[base:], nil
}

func (r *columnVectorGraphPhysicalRowReader) skipNativeGraphVector(cur *manifestCursor, version uint16, ordinal int) error {
	if err := r.readNativeGraphValueHeader(cur, version, ordinal, 0, ColumnStoreValueFloat32Vector); err != nil {
		return err
	}
	n := cur.u64()
	if cur.err != nil {
		return cur.err
	}
	byteLen, ok := cur.fixedWidthSliceByteLen(n, 4, "float32_vector")
	if !ok {
		return cur.err
	}
	cur.pos += int(byteLen)
	return nil
}

func (r *columnVectorGraphPhysicalRowReader) readNativeGraphInvNorm(cur *manifestCursor, version uint16, ordinal int) (float32, error) {
	if err := r.readNativeGraphValueHeader(cur, version, ordinal, 1, ColumnStoreValueFloat32); err != nil {
		return 0, err
	}
	value := math.Float32frombits(cur.u32())
	return value, cur.err
}

func (r *columnVectorGraphPhysicalRowReader) readNativeGraphAdjacency(cur *manifestCursor, version uint16, ordinal int, scratch *columnPhysicalRowReaderScratch) ([]uint32, error) {
	if err := r.readNativeGraphValueHeader(cur, version, ordinal, 2, ColumnStoreValueAdjacencyList); err != nil {
		return nil, err
	}
	start := len(scratch.Uint32Values)
	var err error
	encoding := ColumnFixedWidthEncodingDefault
	if r.reader != nil && len(r.reader.view.Config.Columns) > columnVectorGraphPhysicalRowValueAdjacency {
		encoding = r.reader.view.Config.Columns[columnVectorGraphPhysicalRowValueAdjacency].FixedWidthEncoding
	}
	scratch.Uint32Values, err = cur.appendUint32SliceWithEncoding(scratch.Uint32Values, encoding)
	if err != nil {
		return nil, err
	}
	return scratch.Uint32Values[start:], nil
}

func (r *columnVectorGraphPhysicalRowReader) readNativeGraphValueHeader(cur *manifestCursor, version uint16, ordinal, colIdx int, want ColumnStoreValueType) error {
	typeBytes := cur.stringBytes()
	if cur.err != nil {
		return cur.err
	}
	if !columnPhysicalBytesEqualString(typeBytes, string(want)) {
		return fmt.Errorf("column[%d] type=%q want %q", colIdx, string(typeBytes), want)
	}
	null := cur.bool()
	if cur.err != nil {
		return cur.err
	}
	present := true
	if version >= columnPhysicalAssetVersionV3 {
		present = cur.bool()
		if cur.err != nil {
			return cur.err
		}
	}
	if !present {
		return fmt.Errorf("collections: column_graph %q ordinal=%d missing graph value", r.def.Name, ordinal)
	}
	if null {
		return fmt.Errorf("collections: column_graph %q ordinal=%d contains null graph value", r.def.Name, ordinal)
	}
	return nil
}

func (r *columnVectorGraphPhysicalRowReader) graphRowFromPhysicalRow(row columnPhysicalRowReaderRow, rowCount int) (columnVectorGraphPhysicalRow, error) {
	graphRow, err := r.graphRowFromPhysicalRowUnchecked(row)
	if err != nil {
		return columnVectorGraphPhysicalRow{}, err
	}
	if err := validateColumnVectorGraphAdjacency(r.def.Name, row.Ordinal, graphRow.Adjacency, rowCount); err != nil {
		return columnVectorGraphPhysicalRow{}, err
	}
	return graphRow, nil
}

// graphRowFromPhysicalRowUnchecked validates row shape and vector scoring
// inputs, but intentionally does not scan adjacency bounds. Native search uses
// this path and validates each edge exactly when it is expanded.
func (r *columnVectorGraphPhysicalRowReader) graphRowFromPhysicalRowUnchecked(row columnPhysicalRowReaderRow) (columnVectorGraphPhysicalRow, error) {
	if r == nil {
		return columnVectorGraphPhysicalRow{}, errNilColumnVectorGraphPhysicalRowReader
	}
	if len(row.ID) == 0 {
		return columnVectorGraphPhysicalRow{}, fmt.Errorf("collections: column_graph %q ordinal=%d missing document id", r.def.Name, row.Ordinal)
	}
	if len(row.Values) != columnVectorGraphPhysicalRowValueCount {
		return columnVectorGraphPhysicalRow{}, fmt.Errorf("collections: column_graph %q ordinal=%d values=%d want %d", r.def.Name, row.Ordinal, len(row.Values), columnVectorGraphPhysicalRowValueCount)
	}
	vector := row.Values[columnVectorGraphPhysicalRowValueVector]
	invNorm := row.Values[columnVectorGraphPhysicalRowValueInvNorm]
	adjacency := row.Values[columnVectorGraphPhysicalRowValueAdjacency]
	if invNorm.Type != ColumnStoreValueFloat32 || adjacency.Type != ColumnStoreValueAdjacencyList {
		return columnVectorGraphPhysicalRow{}, fmt.Errorf("collections: column_graph %q ordinal=%d unexpected graph value types: inv_norm=%q adjacency=%q", r.def.Name, row.Ordinal, invNorm.Type, adjacency.Type)
	}
	if !invNorm.Present || !adjacency.Present {
		return columnVectorGraphPhysicalRow{}, fmt.Errorf("collections: column_graph %q ordinal=%d missing graph value", r.def.Name, row.Ordinal)
	}
	if invNorm.Null || adjacency.Null {
		return columnVectorGraphPhysicalRow{}, fmt.Errorf("collections: column_graph %q ordinal=%d contains null graph value", r.def.Name, row.Ordinal)
	}
	vectorValues, _, _, typedVectorOK := r.typedVectorForOrdinal(row.Ordinal)
	if !typedVectorOK {
		if vector.Type != ColumnStoreValueFloat32Vector {
			return columnVectorGraphPhysicalRow{}, fmt.Errorf("collections: column_graph %q ordinal=%d unexpected graph vector type: vector=%q", r.def.Name, row.Ordinal, vector.Type)
		}
		if !vector.Present {
			return columnVectorGraphPhysicalRow{}, fmt.Errorf("collections: column_graph %q ordinal=%d missing graph value", r.def.Name, row.Ordinal)
		}
		if vector.Null {
			return columnVectorGraphPhysicalRow{}, fmt.Errorf("collections: column_graph %q ordinal=%d contains null graph value", r.def.Name, row.Ordinal)
		}
		if len(vector.Float32Vector) != r.def.Dimensions {
			return columnVectorGraphPhysicalRow{}, fmt.Errorf("collections: column_graph %q ordinal=%d vector dims=%d want %d", r.def.Name, row.Ordinal, len(vector.Float32Vector), r.def.Dimensions)
		}
		vectorValues = vector.Float32Vector
	}
	if invNorm.Float32 <= 0 || math.IsNaN(float64(invNorm.Float32)) || math.IsInf(float64(invNorm.Float32), 0) {
		return columnVectorGraphPhysicalRow{}, fmt.Errorf("collections: column_graph %q ordinal=%d invalid inv_norm=%v", r.def.Name, row.Ordinal, invNorm.Float32)
	}
	return columnVectorGraphPhysicalRow{
		Ordinal:   row.Ordinal,
		RowIndex:  row.RowIndex,
		ID:        row.ID,
		Vector:    vectorValues,
		InvNorm:   invNorm.Float32,
		Adjacency: adjacency.AdjacencyList,
	}, nil
}

func validateColumnVectorGraphAdjacencyOrdinal(graphName string, ordinal int, adjacencyIndex int, neighbor uint32, rowCount int) error {
	if uint64(neighbor) >= uint64(rowCount) {
		return fmt.Errorf("collections: column_graph %q ordinal=%d adjacency[%d]=%d outside row_count=%d: %w", graphName, ordinal, adjacencyIndex, neighbor, rowCount, errColumnVectorGraphAdjacencyOrdinalOutOfBounds)
	}
	return nil
}

func validateColumnVectorGraphAdjacency(graphName string, ordinal int, adjacency []uint32, rowCount int) error {
	maxLayer, err := columnVectorGraphAdjacencyMaxLayer(adjacency)
	if err != nil {
		return fmt.Errorf("collections: column_graph %q ordinal=%d malformed adjacency: %w", graphName, ordinal, err)
	}
	for layer := 0; layer <= maxLayer; layer++ {
		neighbors, err := columnVectorGraphAdjacencyLayer(adjacency, layer)
		if err != nil {
			return fmt.Errorf("collections: column_graph %q ordinal=%d malformed adjacency layer=%d: %w", graphName, ordinal, layer, err)
		}
		for i, neighbor := range neighbors {
			if err := validateColumnVectorGraphAdjacencyOrdinal(graphName, ordinal, i, neighbor, rowCount); err != nil {
				return err
			}
		}
	}
	return nil
}

func columnVectorGraphAdjacencyMaxLayer(adjacency []uint32) (int, error) {
	if !columnVectorGraphAdjacencyIsLayered(adjacency) {
		return 0, nil
	}
	maxLayer := int(adjacency[1])
	pos := 2
	for layer := 0; layer <= maxLayer; layer++ {
		if pos >= len(adjacency) {
			return 0, fmt.Errorf("layer=%d missing count", layer)
		}
		count := int(adjacency[pos])
		pos++
		if count < 0 || count > len(adjacency)-pos {
			return 0, fmt.Errorf("layer=%d count=%d exceeds remaining=%d", layer, count, len(adjacency)-pos)
		}
		pos += count
	}
	if pos != len(adjacency) {
		return 0, fmt.Errorf("trailing adjacency values=%d", len(adjacency)-pos)
	}
	return maxLayer, nil
}

func columnVectorGraphAdjacencyLayer(adjacency []uint32, wantLayer int) ([]uint32, error) {
	if wantLayer < 0 {
		return nil, fmt.Errorf("negative layer=%d", wantLayer)
	}
	if !columnVectorGraphAdjacencyIsLayered(adjacency) {
		if wantLayer == 0 {
			return adjacency, nil
		}
		return nil, nil
	}
	maxLayer := int(adjacency[1])
	if wantLayer > maxLayer {
		return nil, nil
	}
	pos := 2
	for layer := 0; layer <= maxLayer; layer++ {
		if pos >= len(adjacency) {
			return nil, fmt.Errorf("layer=%d missing count", layer)
		}
		count := int(adjacency[pos])
		pos++
		if count < 0 || count > len(adjacency)-pos {
			return nil, fmt.Errorf("layer=%d count=%d exceeds remaining=%d", layer, count, len(adjacency)-pos)
		}
		layerAdjacency := adjacency[pos : pos+count]
		if layer == wantLayer {
			return layerAdjacency, nil
		}
		pos += count
	}
	return nil, nil
}

func columnVectorGraphAdjacencyIsLayered(adjacency []uint32) bool {
	return len(adjacency) >= 2 && adjacency[0] == columnVectorGraphLayeredAdjacencyMagic
}
