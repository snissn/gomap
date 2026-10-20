package collections

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func (c *Collection) nativeDocumentVectors() bool {
	return c != nil && c.nativeJSON != nil && c.meta.nativeDocumentCoverageVersion == nativeDocumentVectorCoverageVersion
}

// A session owns only decoded visited records and original read scratch. Its
// read scope is borrowed; a held searcher owns that capture until Close.
type nativeDocumentGraphSession struct {
	read   *nativeDocumentGraphRead
	graph  *nativeDocumentGraph
	lease  *memtable.COWExternalLease
	charge uint64
}

func (s *nativeDocumentGraphSession) close() {
	if s == nil {
		return
	}
	s.graph = nil
	if s.read != nil {
		s.read.Close()
		s.read = nil
	}
	if s.lease != nil {
		s.lease.Close()
		s.lease = nil
	}
}

func (s *nativeDocumentGraphSession) admit(n uint64) error {
	if err := nativeTypedCheckedAdd(&s.charge, n); err != nil {
		return err
	}
	if s.lease == nil {
		var err error
		s.lease, err = s.read.scope.capture.NativeAllocator().AdmitNativeBytes(s.charge)
		return err
	}
	return s.lease.Resize(s.charge)
}

func openNativeDocumentGraphSession(scope *NativeJSONRead, name string) (out *nativeDocumentGraphSession, resultErr error) {
	if scope == nil || scope.capture == nil {
		return nil, backenddb.ErrClosed
	}
	if scope.meta.nativeDocumentCoverageVersion != nativeDocumentVectorCoverageVersion {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	def, ok := findVectorIndex(scope.meta.VectorIndexes, name)
	if !ok {
		return nil, ErrIndexNotFound
	}
	domain, _ := scope.collection.domains.Get("vector-index/" + name)
	read, err := newNativeDocumentGraphRead(scope, domain)
	if err != nil {
		return nil, err
	}
	s := &nativeDocumentGraphSession{read: read}
	defer func() {
		if resultErr != nil {
			s.close()
		}
	}()
	base, _ := scope.capture.State().(*nativeEngineVersion)
	if base == nil {
		base = scope.privateEngine
	}
	generation, err := nativeDocumentGenerationAtRead(scope, base)
	if err != nil {
		return nil, err
	}
	if err = s.admit(memtable.COWAllocationCharge(uint64(unsafe.Sizeof(*s))) + uint64(len(def.Name)+len(def.Field))*8 + 512); err != nil {
		return nil, err
	}
	schema, err := nativeDocumentGraphSchema(def)
	if err != nil {
		return nil, err
	}
	s.graph, err = newNativeDocumentGraph(def, scope.collection.incarnation, generation, read.get, s.admit)
	if err != nil {
		return nil, err
	}
	s.graph.scanNode = read.scanNode
	if domain != nil && !domain.documentGraph.valid(scope.collection.incarnation, generation, schema) {
		if err = s.graph.validateCold(read.scanNode, read.walk); err != nil {
			return nil, err
		}
		// Validation cannot bind a new authority tuple to an already published
		// domain. Only the original private constructor may bind that tuple.
		if domain.documentGraph.matches(scope.collection.incarnation, generation, schema) {
			if err = domain.documentGraph.certify(scope.collection.incarnation, generation, schema); err != nil {
				return nil, err
			}
		}
	}
	return s, nil
}

func (g *nativeDocumentGraph) stats() (VectorIndexStats, error) {
	s := g.meta.NativeDocument
	v := VectorIndexStats{Name: g.config.name, Field: g.config.field, Metric: g.config.metric, Encoding: g.config.encoding, Dimensions: g.config.dimensions, M: g.config.m, EfConstruction: g.config.efConstruction, EfSearch: g.config.efSearch, Nodes: int(s.Nodes), LiveDocs: int(s.Live), DeletedDocs: int(s.Nodes - s.Live), MaxLevel: g.meta.MaxLevel}
	if s.Nodes != 0 {
		v.DeletedRatio = float64(s.Nodes-s.Live) / float64(s.Nodes)
		v.AvgDegree = float64(s.Edges) / float64(s.Nodes)
	}
	// Preserve the original payload/edge/node-header estimate, without claiming
	// that the complete graph is decoded into a retained mutable heap.
	n, err := nativeTypedCheckedMul(s.Edges, uint64(unsafe.Sizeof(vectorIndexNeighbor{})))
	if err != nil {
		return v, err
	}
	if err = nativeTypedCheckedAdd(&n, s.VectorIDBytes); err != nil {
		return v, err
	}
	headers, err := nativeTypedCheckedMul(s.Nodes, 32)
	if err != nil {
		return v, err
	}
	if err = nativeTypedCheckedAdd(&n, headers); err != nil {
		return v, err
	}
	if n > math.MaxInt64 {
		return v, fmt.Errorf("collections: native graph stats byte overflow")
	}
	v.BytesMemory = int64(n)
	v.RebuildNeeded = v.DeletedDocs > 0 && v.DeletedRatio >= g.config.rebuildDeletedRatio
	return v, nil
}

func (c *Collection) loadNativeDocumentVector(opts VectorIndexOptions) (idx *VectorIndex, status VectorIndexLoadStatus, resultErr error) {
	idx, err := newVectorIndex(c, opts)
	if err != nil {
		return nil, status, err
	}
	scope, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		return nil, status, err
	}
	defer func() { resultErr = errors.Join(resultErr, scope.Close()) }()
	def, ok := findVectorIndex(scope.meta.VectorIndexes, idx.name)
	if !ok {
		return nil, status, ErrIndexNotFound
	}
	// Schema generation is supplied by the captured declaration, not by the
	// public Build options. Every public shape/HNSW option must still match.
	idx.schemaGeneration = def.SchemaGeneration
	if reason := idx.validateNativeSnapshotDefinition(def); reason != "" {
		return nil, status, fmt.Errorf("%w: native document graph %s", ErrVectorIndexSearchUnavailable, reason)
	}
	session, err := openNativeDocumentGraphSession(scope, idx.name)
	if err != nil {
		return nil, status, err
	}
	defer session.close()
	v, err := session.graph.stats()
	if err != nil {
		return nil, status, err
	}
	v.RebuildNeeded = v.DeletedDocs > 0 && v.DeletedRatio >= idx.rebuildDeletedRatio
	idx.nativeDocumentStats = &v
	status.Loaded = true
	status.RootName = collectionVectorIndexRootName(c.name, idx.name)
	return idx, status, nil
}

func (c *Collection) openNativeDocumentVectorSearcher(opts VectorIndexSearcherOptions) (out *VectorIndexSearcher, resultErr error) {
	if err := ValidateIndexName(opts.IndexName); err != nil {
		return nil, err
	}
	if opts.MaxDecodedBlocks < 0 {
		return nil, errors.New("collections: vector index search max_decoded_blocks cannot be negative")
	}
	scope, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		return nil, err
	}
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, scope.Close())
		}
	}()
	lease, err := scope.capture.NativeAllocator().AdmitNativeBytes(memtable.COWAllocationCharge(uint64(unsafe.Sizeof(VectorIndexSearcher{}))))
	if err != nil {
		return nil, err
	}
	defer func() {
		if resultErr != nil {
			lease.Close()
		}
	}()

	session, err := openNativeDocumentGraphSession(scope, opts.IndexName)
	if err != nil {
		return nil, err
	}
	defer session.close()
	def, _ := findVectorIndex(scope.meta.VectorIndexes, opts.IndexName)
	stats, err := session.graph.stats()
	if err != nil {
		return nil, err
	}
	out = &VectorIndexSearcher{collection: c, indexName: opts.IndexName, strategy: VectorIndexStrategyNativeRuntime, path: VectorIndexSearchPathNativeRuntime, nativeDocument: scope, nativeDocumentLease: lease, nativeDocumentBound: true, status: VectorIndexStatus{Name: opts.IndexName, Definition: def, Strategy: VectorIndexStrategyNativeRuntime, Loaded: true, NativeRootLoaded: true, RootName: collectionVectorIndexRootName(c.name, opts.IndexName), Stats: stats}}
	return out, nil
}

func (s *VectorIndexSearcher) searchNativeDocument(opts VectorIndexSearcherSearchOptions) (VectorIndexSearchResponse, error) {
	return s.searchNativeDocumentInto(opts, nil, nil, 0)
}

func (s *VectorIndexSearcher) searchNativeDocumentWithBuffer(opts VectorIndexSearcherSearchOptions, buffer *VectorIndexSearchBuffer) (VectorIndexSearchResponse, error) {
	if buffer == nil {
		return VectorIndexSearchResponse{}, errors.New("collections: nil vector index search buffer")
	}
	return s.searchNativeDocumentInto(opts, buffer, nil, 0)
}

func (s *VectorIndexSearcher) searchNativeDocumentInto(opts VectorIndexSearcherSearchOptions, buffer *VectorIndexSearchBuffer, ctx context.Context, candidateLimit int) (response VectorIndexSearchResponse, resultErr error) {
	if buffer != nil {
		buffer.Reset()
	}
	defer func() {
		if resultErr != nil {
			response.Results = nil
			if buffer != nil {
				buffer.Reset()
			}
		}
	}()
	if s == nil {
		return response, errors.New("collections: nil vector index searcher")
	}
	response.IndexName, response.Strategy, response.Path, response.Status = s.indexName, s.strategy, s.path, s.status
	if s.closed || s.nativeDocument == nil {
		return response, backenddb.ErrClosed
	}
	if err := validateVectorIndexSearchRequest(opts.TopK, opts.EfSearch); err != nil {
		return response, err
	}
	if buffer != nil && opts.IncludeDocuments {
		return response, errors.New("collections: vector index SearchWithBuffer does not support IncludeDocuments")
	}
	mode, err := normalizeVectorIndexSearchQueryMode(opts.QueryMode, opts.QuantizedIndexName, opts.QuantizedRerankCandidates, opts.TopK)
	if err != nil {
		return response, err
	}
	if mode != columnVectorGraphNativeSearchQueryModeExact || documentFetchOptionsHasProjection(opts.DocumentFetchOptions) {
		return response, backenddb.ErrNativeSemanticUnsupported
	}
	// Preserve the zero-value compatibility mode while refusing explicitly
	// requested detailed counters that this native runtime does not implement.
	if opts.StatsMode == VectorIndexSearchStatsModeFullDiagnostics {
		return response, collectionVectorIndexWithBufferUnsupportedOptionError("StatsMode=full_diagnostics", "native_runtime full-diagnostics counters are not implemented; use production/minimal mode with CPU profiles")
	}
	if opts.StatsMode == VectorIndexSearchStatsModeWorkAccounting {
		return response, collectionVectorIndexWithBufferUnsupportedOptionError("StatsMode=work_accounting", "native_runtime work-accounting counters are not implemented; use production/minimal mode with CPU profiles")
	}
	if _, err = columnVectorGraphNativeSearchStatsModeFromPublic(opts.StatsMode); err != nil {
		return response, err
	}
	if ctx != nil {
		if err = ctx.Err(); err != nil {
			return response, err
		}
	}
	session, err := openNativeDocumentGraphSession(s.nativeDocument, s.indexName)
	if err != nil {
		return response, err
	}
	defer session.close()
	g := session.graph
	if len(opts.Query) != g.config.dimensions {
		return response, fmt.Errorf("collections: vector query dimension mismatch")
	}
	if err = validateFloat32Vector(opts.Query); err != nil {
		return response, err
	}
	if vectorNormSquared(opts.Query) == 0 {
		return response, fmt.Errorf("collections: zero vector query")
	}
	response.Stats.SearchRouteNativeRuntime = 1
	response.Stats.CandidateRows = g.meta.NativeDocument.Live
	if opts.TopK == 0 || g.meta.Entry < 0 {
		return response, nil
	}
	// Each newly visited node/adjacency and traversal allocation passes these
	// original callbacks. Cancellation never changes the captured graph.
	if ctx != nil {
		get, admit := g.get, g.admit
		g.get = func(key []byte) ([]byte, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return get(key)
		}
		g.admit = func(n uint64) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			return admit(n)
		}
	}
	ef := opts.EfSearch
	if ef == 0 {
		ef = g.config.efSearch
	}
	if ef < opts.TopK {
		ef = opts.TopK
	}
	if ef < candidateLimit {
		ef = candidateLimit
	}
	candidates, err := g.currentSearchCandidates(opts.Query, ef)
	if err != nil {
		return response, err
	}
	// searchLayer decoded every scored candidate. Apply the original public
	// document-ID tie order, independently of the internal node ordinal order.
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].distance != candidates[j].distance {
			return candidates[i].distance < candidates[j].distance
		}
		return bytes.Compare(g.nodes[candidates[i].nodeID].value.documentID, g.nodes[candidates[j].nodeID].value.documentID) < 0
	})
	count, idBytes := 0, 0
	for _, candidate := range candidates {
		n := g.nodes[candidate.nodeID]
		if n.value.deleted {
			continue
		}
		if len(n.value.documentID) > int(^uint(0)>>1)-idBytes {
			return response, fmt.Errorf("collections: native graph result ID byte overflow")
		}
		idBytes += len(n.value.documentID)
		count++
		if count == opts.TopK {
			break
		}
	}
	if count == 0 {
		return response, nil
	}
	var ids []byte
	if buffer == nil {
		response.Results = make([]VectorIndexSearchResult, count)
		ids = make([]byte, idBytes)
		markVectorIndexSearchResponseOwnedResultAllocs(&response)
	} else {
		buffer.results = resizeVectorIndexSearchResultBuffer(buffer.results, count)
		buffer.idBytes = resizeVectorIndexSearchByteBuffer(buffer.idBytes, idBytes)
		response.Results, ids = buffer.results, buffer.idBytes
	}
	at, offset := 0, 0
	for _, candidate := range candidates {
		n := g.nodes[candidate.nodeID]
		if n.value.deleted {
			continue
		}
		if ctx != nil {
			if err = ctx.Err(); err != nil {
				return response, err
			}
		}
		end := offset + len(n.value.documentID)
		id := ids[offset:end:end]
		copy(id, n.value.documentID)
		hit := VectorIndexSearchResult{ID: id, Ordinal: candidate.nodeID, Score: 1 - float64(candidate.distance)}
		if opts.IncludeDocuments {
			hit.Document, err = s.nativeDocument.Get(id)
			if err != nil {
				return response, err
			}
			response.Stats.DocumentsFetched++
			response.Stats.DocumentBytes += uint64(len(hit.Document))
			response.Stats.DocumentOutputBytes += uint64(len(hit.Document))
		}
		response.Results[at] = hit
		at, offset = at+1, end
		if at == count {
			break
		}
	}
	return response, nil
}

func (c *Collection) searchNativeDocumentVector(opts VectorIndexSearchOptions, buffer *VectorIndexSearchBuffer) (VectorIndexSearchResponse, error) {
	if buffer == nil {
		return VectorIndexSearchResponse{}, errors.New("collections: nil vector index search buffer")
	}
	buffer.Reset()
	if opts.IncludeDocuments {
		return VectorIndexSearchResponse{}, errors.New("collections: vector index SearchWithBuffer does not support IncludeDocuments")
	}
	return c.searchNativeDocumentVectorUsing(opts, buffer)
}

// A nil buffer denotes response-owned output. No VectorIndex handle retains a
// cut; only an explicitly opened VectorIndexSearcher has that lifetime.
func (c *Collection) searchNativeDocumentVectorUsing(opts VectorIndexSearchOptions, buffer *VectorIndexSearchBuffer) (response VectorIndexSearchResponse, resultErr error) {
	defer func() {
		if resultErr != nil {
			response.Results = nil
			if buffer != nil {
				buffer.Reset()
			}
		}
	}()
	if opts.Filter != nil || opts.IndexRangeFilter != nil || opts.DeclaredScalarFilter != nil {
		return response, backenddb.ErrNativeSemanticUnsupported
	}
	if err := validateVectorIndexSearchRequest(opts.TopK, opts.EfSearch); err != nil {
		return response, err
	}
	if opts.Context != nil {
		if err := opts.Context.Err(); err != nil {
			return response, err
		}
	}
	defer markCollectionVectorIndexOneShotOpenSetup(&response.Stats)
	searcher, err := c.openNativeDocumentVectorSearcher(VectorIndexSearcherOptions{IndexName: opts.IndexName, MaxDecodedBlocks: opts.MaxDecodedBlocks})
	if err != nil {
		return response, err
	}
	defer func() { resultErr = errors.Join(resultErr, searcher.Close()) }()
	// Preserve the native runtime's candidate overfetch for caller-supplied
	// FetchMultiplier without permitting integer wrap or a full-graph fallback.
	candidateLimit := 0
	if opts.FetchMultiplier > 0 {
		if opts.TopK > int(^uint(0)>>1)/opts.FetchMultiplier {
			return response, fmt.Errorf("collections: vector candidate count overflow")
		}
		candidateLimit = opts.TopK * opts.FetchMultiplier
	}
	return searcher.searchNativeDocumentInto(VectorIndexSearcherSearchOptions{Query: opts.Query, QueryMode: opts.QueryMode, QuantizedIndexName: opts.QuantizedIndexName, QuantizedRerankCandidates: opts.QuantizedRerankCandidates, TopK: opts.TopK, EfSearch: opts.EfSearch, IncludeDocuments: opts.IncludeDocuments, DocumentFetchOptions: opts.DocumentFetchOptions, StatsMode: opts.StatsMode}, buffer, opts.Context, candidateLimit)
}
