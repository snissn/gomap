package collections

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"

	"github.com/snissn/gomap/TreeDB/collections/chunking"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// chunkingScanMaxDocuments is the fail-closed cap for one parent# prefix.
const chunkingScanMaxDocuments = math.MaxInt32

var errBatchChunkIngestVectorIndexed = errors.New("collections: batch chunk ingest is text-only and does not support vector-indexed collections")

// ChunkedIngestOptions tunes IngestChunkedDocument.
type ChunkedIngestOptions struct {
	// TextField names the parent document field holding the text to chunk.
	// Empty defaults to "body".
	TextField string
	hooks     *chunkedIngestHooks
}

type chunkedIngestHooks struct {
	afterDelete    func()
	afterBatchScan func()
}

func (o ChunkedIngestOptions) textField() string {
	if o.TextField == "" {
		return "body"
	}
	return o.TextField
}

func validateChunkTextField(field string) error {
	path, err := parseVectorFieldPath(field)
	if err != nil {
		return fmt.Errorf("collections: invalid chunk text field %q: %w", field, err)
	}
	switch path[0] {
	case chunking.MetaFieldParent, chunking.MetaFieldOrdinal, chunking.MetaFieldKind:
		return fmt.Errorf("collections: chunk text field %q overlaps reserved linkage root %q", field, path[0])
	default:
		return nil
	}
}

// ChunkedIngestResult reports the outcome of one chunked ingest.
type ChunkedIngestResult struct {
	parentID []byte
	// ChildIDs are the live child document IDs after the ingest, in ordinal
	// order: <parentID>#0, <parentID>#1, ...
	ChildIDs [][]byte
	// Replaced is the number of stale children tombstoned by this ingest
	// (zero for a first ingest).
	Replaced int
}

// ParentID returns the parent document ID of the ingest.
func (r ChunkedIngestResult) ParentID() []byte { return r.parentID }

// chunkChild is one planned child row: a derived ID plus its stored JSON
// document.
type chunkChild struct {
	id       []byte
	document []byte
}

func chunkPlanIDs(children []chunkChild) [][]byte {
	ids := make([][]byte, len(children))
	for i, ch := range children {
		ids[i] = ch.id
	}
	return ids
}

func chunkPlanDocs(children []chunkChild) [][]byte {
	docs := make([][]byte, len(children))
	for i, ch := range children {
		docs[i] = ch.document
	}
	return docs
}

// chunkedIngestPlan is the validated, mutation-free output of buildChunkPlan.
type chunkedIngestPlan struct {
	children []chunkChild
}

type chunkedDocumentBatchPlan struct {
	parentID []byte
	parent   []byte
	children []chunkChild
}

// buildChunkPlan derives the child rows for a chunked ingest without touching
// the collection. A parent's optional "meta" object is copied into every
// child; generated top-level chunk linkage remains authoritative even when
// meta contains same-named keys. Everything that can fail — config validation,
// text field extraction, chunking, metadata encoding, and child linkage
// validation — fails closed here, before any mutation.
func (c *Collection) buildChunkPlan(parentID []byte, parentDocument []byte, cfg chunking.Config, opts ChunkedIngestOptions) (chunkedIngestPlan, error) {
	var plan chunkedIngestPlan
	if err := chunking.ValidateParentID(string(parentID)); err != nil {
		return plan, err
	}
	if err := validateChunkTextField(opts.textField()); err != nil {
		return plan, err
	}
	if !json.Valid(parentDocument) {
		return plan, fmt.Errorf("collections: chunked ingest requires a JSON parent document")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(parentDocument, &fields); err != nil {
		return plan, fmt.Errorf("collections: chunked ingest parse parent document: %w", err)
	}
	var inheritedMeta json.RawMessage
	if rawMeta, ok := fields[ingestSourceMetaField]; ok {
		var meta map[string]json.RawMessage
		if err := json.Unmarshal(rawMeta, &meta); err != nil {
			return plan, fmt.Errorf("collections: chunked ingest field %q must be an object: %w", ingestSourceMetaField, err)
		}
		if meta == nil {
			return plan, fmt.Errorf("collections: chunked ingest field %q must be an object", ingestSourceMetaField)
		}
		inheritedMeta = append(json.RawMessage(nil), rawMeta...)
	}
	rawText, ok := fields[opts.textField()]
	if !ok {
		return plan, fmt.Errorf("collections: chunked ingest parent document has no %q field", opts.textField())
	}
	var text string
	if err := json.Unmarshal(rawText, &text); err != nil {
		return plan, fmt.Errorf("collections: chunked ingest field %q must be a string: %w", opts.textField(), err)
	}
	chunks, err := chunking.SplitChunks(string(parentID), text, cfg)
	if err != nil {
		return plan, err
	}
	plan.children = make([]chunkChild, 0, len(chunks))
	for _, ch := range chunks {
		child := map[string]any{
			opts.textField():          ch.Text,
			chunking.MetaFieldParent:  ch.ParentID,
			chunking.MetaFieldOrdinal: ch.Ordinal,
			chunking.MetaFieldKind:    chunking.KindChunk,
		}
		if inheritedMeta != nil {
			child[ingestSourceMetaField] = inheritedMeta
		}
		document, err := json.Marshal(child)
		if err != nil {
			return plan, fmt.Errorf("collections: encode chunk child %q: %w", ch.ID, err)
		}
		// Belt-and-braces: the stored child must validate against its own
		// linkage metadata before it can ever reach an index.
		if err := chunking.ValidateChunkChild(ch.ID, document); err != nil {
			return plan, fmt.Errorf("collections: chunk plan produced invalid child: %w", err)
		}
		plan.children = append(plan.children, chunkChild{id: []byte(ch.ID), document: document})
	}
	return plan, nil
}

type chunkParentLifecycleLocks struct {
	mu      sync.Mutex
	unlocks map[string]func()
}

func (locks *chunkParentLifecycleLocks) release(parentID []byte) {
	if locks == nil {
		return
	}
	locks.mu.Lock()
	unlock := locks.unlocks[string(parentID)]
	delete(locks.unlocks, string(parentID))
	locks.mu.Unlock()
	if unlock != nil {
		unlock()
	}
}

func (locks *chunkParentLifecycleLocks) releaseAll() {
	if locks == nil {
		return
	}
	locks.mu.Lock()
	unlocks := locks.unlocks
	locks.unlocks = make(map[string]func())
	locks.mu.Unlock()
	for _, unlock := range unlocks {
		unlock()
	}
}

func (c *Collection) lockChunkParentLifecycles(ctx context.Context, parentIDs [][]byte) (*chunkParentLifecycleLocks, error) {
	coord := c.collectionSchemaCoordinator()
	if coord == nil {
		return nil, fmt.Errorf("collections: chunk lifecycle coordinator unavailable")
	}
	ids := make([]string, len(parentIDs))
	for i := range parentIDs {
		ids[i] = string(parentIDs[i])
	}
	sort.Strings(ids)
	locks := &chunkParentLifecycleLocks{unlocks: make(map[string]func(), len(ids))}
	for _, id := range ids {
		unlock, err := coord.lockChunkLifecycle(ctx, id)
		if err != nil {
			locks.releaseAll()
			return nil, err
		}
		locks.unlocks[id] = unlock
	}
	return locks, nil
}

func (c *Collection) lockChunkMutation(ctx context.Context) (func(), error) {
	coord := c.collectionSchemaCoordinator()
	if coord == nil {
		return nil, fmt.Errorf("collections: chunk mutation coordinator unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	coord.chunkMutationOnce.Do(func() {
		coord.chunkMutationToken = make(chan struct{}, 1)
		coord.chunkMutationToken <- struct{}{}
	})
	select {
	case <-coord.chunkMutationToken:
		var once sync.Once
		return func() {
			once.Do(func() { coord.chunkMutationToken <- struct{}{} })
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// IngestChunkedDocument stores parentDocument under parentID and replaces its
// chunk children with the stream derived from the configured text field. When
// parentDocument contains a "meta" object, each child receives a detached copy
// under "meta"; generated top-level chunk_parent, chunk_ordinal, and chunk_kind
// fields are authoritative over same-named caller metadata.
//
// Lifecycle: parent ID, text field, metadata, and child plan validate before
// mutation. A per-parent lock shared across collection handles serializes plan
// through replacement. This direct chunk-ingest API keeps separate parent
// upsert, stale-child DeleteBatch, and replacement InsertBatch durable commits:
// each batch is atomic, but an error between boundaries is commit-ambiguous.
// The source may be old, new, or between those states; retrying converges
// because child IDs derive only from parent ID and ordinal. IngestSources uses
// the stronger one-source atomic publication path.
//
// Children are ordinary documents to the index layer: text, scalar, and vector
// indexes maintain them through the normal InsertBatch/DeleteBatch paths, so
// they resolve only live children after a re-chunk.
func (c *Collection) IngestChunkedDocument(parentID []byte, parentDocument []byte, cfg chunking.Config, opts ChunkedIngestOptions) (ChunkedIngestResult, error) {
	var result ChunkedIngestResult
	if c == nil {
		return result, errCollectionNil
	}
	if c.db == nil {
		return result, errCollectionDBNil
	}
	if err := chunking.ValidateParentID(string(parentID)); err != nil {
		return result, err
	}
	lifecycleLocks, err := c.lockChunkParentLifecycles(context.Background(), [][]byte{parentID})
	if err != nil {
		return result, err
	}
	defer lifecycleLocks.releaseAll()
	plan, err := c.buildChunkPlan(parentID, parentDocument, cfg, opts)
	if err != nil {
		return result, err
	}
	unlockMutation, err := c.lockChunkMutation(context.Background())
	if err != nil {
		return result, err
	}
	defer unlockMutation()

	if err := c.flushCollectionWriteDomainsForSchemaMutation(); err != nil {
		return result, fmt.Errorf("collections: publish chunk write domains before replacement: %w", err)
	}
	oldChildren, _, err := c.chunkChildrenUnlocked(parentID)
	if err != nil {
		return result, err
	}
	// Parent upsert, stale-child delete, and child insert remain separate
	// durable boundaries; retry repairs any reported commit ambiguity.
	if err := c.upsertParentDocument(parentID, parentDocument); err != nil {
		return result, err
	}
	if len(oldChildren) > 0 {
		if _, err := c.DeleteBatch(oldChildren); err != nil {
			return result, fmt.Errorf("collections: tombstone stale chunk children of %q: %w", parentID, err)
		}
	}
	if opts.hooks != nil && opts.hooks.afterDelete != nil {
		opts.hooks.afterDelete()
	}
	if len(plan.children) > 0 {
		if _, err := c.InsertBatch(chunkPlanIDs(plan.children), chunkPlanDocs(plan.children)); err != nil {
			return result, fmt.Errorf("collections: insert chunk children of %q: %w", parentID, err)
		}
	}
	result.parentID = append([]byte(nil), parentID...)
	result.ChildIDs = make([][]byte, len(plan.children))
	for i, ch := range plan.children {
		result.ChildIDs[i] = append([]byte(nil), ch.id...)
	}
	result.Replaced = len(oldChildren)
	return result, nil
}

// IngestChunkedDocuments stores a batch of text-only source documents through
// one durable collection-root publication. It is the batch counterpart to
// IngestChunkedDocument: child IDs remain deterministic (<parentID>#<ordinal>)
// and results retain source input order.
//
// The complete batch — IDs (including duplicates), source document encoding,
// text-field and metadata validation, and every child plan — validates before
// the collection mutates. A successful call atomically replaces every supplied
// parent and its children as one normal durable storage publication. If that
// publication reports an ambiguous commit, callers must retry the complete
// batch; it converges to the same parent/child IDs. Bounded callers may use one
// call as their atomicity unit.
//
// This deliberately has no embedding or vector-index configuration: it is the
// narrow public seam for pure text chunk ingestion and rejects collections
// with vector indexes before mutation. It uses SourceDocument so callers share
// the source-document metadata encoding contract with IngestSources.
func (c *Collection) IngestChunkedDocuments(sources []SourceDocument, cfg chunking.Config, opts ChunkedIngestOptions) ([]ChunkedIngestResult, error) {
	if c == nil {
		return nil, errCollectionNil
	}
	if c.db == nil {
		return nil, errCollectionDBNil
	}
	if len(sources) == 0 {
		return nil, nil
	}

	plans := make([]chunkedDocumentBatchPlan, len(sources))
	seen := make(map[string]struct{}, len(sources))
	for i, source := range sources {
		if err := chunking.ValidateParentID(string(source.ID)); err != nil {
			return nil, fmt.Errorf("collections: chunked ingest batch source %d (%q): %w", i, source.ID, err)
		}
		key := string(source.ID)
		if _, duplicate := seen[key]; duplicate {
			return nil, fmt.Errorf("collections: chunked ingest batch has duplicate parent ID %q at source %d", source.ID, i)
		}
		seen[key] = struct{}{}
		parent, err := buildIngestParentDocument(source)
		if err != nil {
			return nil, fmt.Errorf("collections: chunked ingest batch source %d (%q): %w", i, source.ID, err)
		}
		plan, err := c.buildChunkPlan(source.ID, parent, cfg, opts)
		if err != nil {
			return nil, fmt.Errorf("collections: chunked ingest batch source %d (%q): %w", i, source.ID, err)
		}
		plans[i] = chunkedDocumentBatchPlan{parentID: bytes.Clone(source.ID), parent: parent, children: plan.children}
	}

	parentIDs := make([][]byte, len(plans))
	for i := range plans {
		parentIDs[i] = plans[i].parentID
	}
	lifecycleLocks, err := c.lockChunkParentLifecycles(context.Background(), parentIDs)
	if err != nil {
		return nil, err
	}
	defer lifecycleLocks.releaseAll()
	unlockNativeAdmission := c.lockNativeVectorAdmissionWrite()
	defer unlockNativeAdmission()
	unlockSchema := c.lockCollectionSchemaWrite()
	defer unlockSchema()
	unlockAdHocAdmission := c.lockAdHocVectorAdmissionRead()
	defer unlockAdHocAdmission()
	if c.registeredAdHocVectorIndexCount() > 0 {
		return nil, errBatchChunkIngestVectorIndexed
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return nil, backenddb.ErrClosed
	}
	catalog, err := loadCollectionCatalog(snap, c.collectionName())
	_ = snap.Close()
	if err != nil {
		return nil, fmt.Errorf("collections: load chunked ingest catalog: %w", err)
	}
	if catalog == nil {
		return nil, errCollectionNotFound
	}
	if len(catalog.meta.VectorIndexes) > 0 {
		return nil, errBatchChunkIngestVectorIndexed
	}
	if err := c.flushCollectionWriteDomainsWithVectorAdmissionLocked(); err != nil {
		return nil, fmt.Errorf("collections: publish chunk write domains before batch replacement: %w", err)
	}
	replaced, err := c.replaceChunkedDocumentBatchLocked(plans, opts.hooks)
	if err != nil {
		return nil, err
	}

	results := make([]ChunkedIngestResult, len(plans))
	for i, plan := range plans {
		childIDs := make([][]byte, len(plan.children))
		for j, child := range plan.children {
			childIDs[j] = bytes.Clone(child.id)
		}
		results[i] = ChunkedIngestResult{
			parentID: bytes.Clone(plan.parentID),
			ChildIDs: childIDs,
			Replaced: replaced[i],
		}
	}
	return results, nil
}

// replaceChunkedDocumentBatchLocked discovers stale children from the same
// snapshot used to plan each publication attempt. The caller holds the
// collection schema write lock from the authoritative vector-index preflight
// through peer-domain flush and publication, so another handle cannot hide a
// buffered write from the snapshot or publish one between the scan and
// replacement.
func (c *Collection) replaceChunkedDocumentBatchLocked(plans []chunkedDocumentBatchPlan, hooks *chunkedIngestHooks) ([]int, error) {
	if err := c.ensureWriteDomainOpen(); err != nil {
		return nil, err
	}
	unlockMutation := c.lockMutation()
	defer unlockMutation.Unlock()
	c.vectorIndexesMu.RLock()
	defer c.vectorIndexesMu.RUnlock()
	if len(c.vectorIndexes) > 0 {
		return nil, errBatchChunkIngestVectorIndexed
	}
	if err := c.flushBufferedWritesWithVectorAdmissionLocked(); err != nil {
		return nil, fmt.Errorf("collections: publish chunk write domains before batch replacement: %w", err)
	}

	insertIDs := make([][]byte, 0, len(plans)*2)
	insertDocs := make([][]byte, 0, len(plans)*2)
	for _, plan := range plans {
		for _, child := range plan.children {
			insertIDs = append(insertIDs, child.id)
			insertDocs = append(insertDocs, child.document)
		}
		insertIDs = append(insertIDs, plan.parentID)
		insertDocs = append(insertDocs, plan.parent)
	}

	var replaced []int
	hookCalled := false

	var lastErr error
	for attempt := range maxCollectionMutationRetries {
		attemptReplaced := make([]int, len(plans))
		deletePlanner := func(snap *backenddb.Snapshot, catalog *collectionCatalog) ([][]byte, error) {
			if len(catalog.meta.VectorIndexes) > 0 {
				return nil, errBatchChunkIngestVectorIndexed
			}
			attemptDeleteIDs := make([][]byte, 0, len(plans)*2)
			for i, plan := range plans {
				oldChildren, _, err := c.chunkChildrenAtSnapshot(plan.parentID, snap, catalog)
				if err != nil {
					return nil, err
				}
				attemptReplaced[i] = len(oldChildren)
				attemptDeleteIDs = append(attemptDeleteIDs, oldChildren...)
				attemptDeleteIDs = append(attemptDeleteIDs, plan.parentID)
			}
			if !hookCalled && hooks != nil && hooks.afterBatchScan != nil {
				hookCalled = true
				hooks.afterBatchScan()
			}
			return attemptDeleteIDs, nil
		}
		plan, err := c.buildSourceReplacementPlan(nil, insertIDs, insertDocs, deletePlanner, nil, nil)
		if err != nil {
			if isRetriableCollectionMutationError(err) {
				lastErr = err
				waitBeforeCollectionMutationRetry(attempt)
				continue
			}
			return nil, fmt.Errorf("collections: publish atomic chunked ingest batch: %w", err)
		}
		replaced = attemptReplaced
		publishErr := c.publishSourceReplacementPlan(plan, nil)
		plan.close()
		if isRetriableCollectionMutationError(publishErr) {
			lastErr = publishErr
			waitBeforeCollectionMutationRetry(attempt)
			continue
		}
		if publishErr != nil {
			return nil, fmt.Errorf("collections: publish atomic chunked ingest batch: %w", publishErr)
		}
		return replaced, nil
	}
	return nil, fmt.Errorf("collections: publish atomic chunked ingest batch: %w", collectionMutationRetryExhausted(lastErr))
}

func (c *Collection) upsertParentDocument(parentID []byte, parentDocument []byte) error {
	current, err := c.Get(parentID)
	if err != nil {
		return err
	}
	if len(current) == 0 {
		_, err := c.Insert(parentID, parentDocument)
		return err
	}
	if bytes.Equal(current, parentDocument) {
		return nil
	}
	replaced, err := c.Replace(parentID, parentDocument)
	if err != nil {
		return err
	}
	if !replaced {
		return fmt.Errorf("collections: chunked ingest parent %q replace did not apply", parentID)
	}
	return nil
}

// ChunkChildrenScanStats is structural evidence for one bounded child lookup.
// ScannedPrimaryRows counts only rows inside the requested parent# range.
type ChunkChildrenScanStats struct {
	ScannedPrimaryRows     int
	ReconstructedDocuments int
	RowLocatorLookups      int
	PointRowFetches        int
}

// ChunkChildren returns the live chunk child IDs of parentID in ordinal order.
// Children are identified by the derived `<parentID>#<ordinal>` ID scheme and
// verified against their stored linkage metadata; rows carrying malformed
// chunk metadata fail closed rather than being reported as children.
func (c *Collection) ChunkChildren(parentID []byte) ([][]byte, error) {
	children, _, err := c.ChunkChildrenWithStats(parentID)
	return children, err
}

// ChunkChildrenWithStats returns ChunkChildren plus prefix-scan evidence.
func (c *Collection) ChunkChildrenWithStats(parentID []byte) ([][]byte, ChunkChildrenScanStats, error) {
	if c == nil {
		return nil, ChunkChildrenScanStats{}, errCollectionNil
	}
	if c.db == nil {
		return nil, ChunkChildrenScanStats{}, errCollectionDBNil
	}
	if err := chunking.ValidateParentID(string(parentID)); err != nil {
		return nil, ChunkChildrenScanStats{}, err
	}
	lifecycleLocks, err := c.lockChunkParentLifecycles(context.Background(), [][]byte{parentID})
	if err != nil {
		return nil, ChunkChildrenScanStats{}, err
	}
	defer lifecycleLocks.releaseAll()
	return c.chunkChildrenUnlocked(parentID)
}

func (c *Collection) chunkChildrenUnlocked(parentID []byte) ([][]byte, ChunkChildrenScanStats, error) {
	return c.chunkChildrenWithScanner(parentID, c.scanChunkDocumentsByParentPrefix)
}

func (c *Collection) chunkChildrenAtSnapshot(parentID []byte, snap *backenddb.Snapshot, catalog *collectionCatalog) ([][]byte, ChunkChildrenScanStats, error) {
	return c.chunkChildrenWithScanner(parentID, func(prefix []byte, maxDocuments int, fn func(DocumentRecord) (bool, error)) (bool, ChunkChildrenScanStats, error) {
		return c.scanChunkDocumentsByParentPrefixAtSnapshot(snap, catalog, prefix, maxDocuments, fn)
	})
}

func (c *Collection) chunkChildrenWithScanner(parentID []byte, scan func([]byte, int, func(DocumentRecord) (bool, error)) (bool, ChunkChildrenScanStats, error)) ([][]byte, ChunkChildrenScanStats, error) {
	prefix := append(append([]byte(nil), parentID...), '#')
	ordinals := map[int][]byte{}
	inspect := func(record DocumentRecord) (bool, error) {
		id := record.ID
		if !bytes.HasPrefix(id, prefix) {
			return false, fmt.Errorf("collections: chunk prefix scan escaped %q at document %q", prefix, id)
		}
		meta, err := chunking.ParseChildMeta(record.Document)
		if err != nil {
			return false, fmt.Errorf("collections: chunk child %q metadata: %w", id, err)
		}
		if meta == nil {
			return false, fmt.Errorf("collections: document %q has a chunk child ID without chunk metadata", id)
		}
		if err := chunking.ValidateChunkChild(string(id), record.Document); err != nil {
			return false, fmt.Errorf("collections: chunk child %q linkage: %w", id, err)
		}
		if _, dup := ordinals[meta.Ordinal]; dup {
			return false, fmt.Errorf("collections: duplicate chunk ordinal %d for parent %q", meta.Ordinal, parentID)
		}
		ordinals[meta.Ordinal] = bytes.Clone(id)
		return true, nil
	}
	truncated, stats, err := scan(prefix, chunkingScanMaxDocuments, inspect)
	if err != nil {
		return nil, stats, err
	}
	if truncated {
		return nil, stats, fmt.Errorf("collections: chunk child prefix scan for %q exceeded bound %d", parentID, chunkingScanMaxDocuments)
	}
	children := make([][]byte, 0, len(ordinals))
	for ordinal := 0; len(children) < len(ordinals); ordinal++ {
		id, ok := ordinals[ordinal]
		if !ok {
			return nil, stats, fmt.Errorf("collections: chunk ordinal gap %d for parent %q", ordinal, parentID)
		}
		children = append(children, id)
	}
	return children, stats, nil
}

// scanChunkDocumentsByParentPrefix walks only primary IDs in the requested
// parent# child namespace and reconstructs only those rows when column storage
// omits projected fields from the retained JSON payload.
func (c *Collection) scanChunkDocumentsByParentPrefix(prefix []byte, maxDocuments int, fn func(DocumentRecord) (bool, error)) (bool, ChunkChildrenScanStats, error) {
	var stats ChunkChildrenScanStats
	if c == nil {
		return false, stats, errCollectionNil
	}
	if c.db == nil {
		return false, stats, errCollectionDBNil
	}
	if len(prefix) == 0 || maxDocuments <= 0 || fn == nil {
		return false, stats, fmt.Errorf("collections: chunk child prefix, positive bound, and callback are required")
	}
	if err := c.flushBufferedWrites(); err != nil {
		return false, stats, err
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return false, stats, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	catalog, err := c.catalogForSnapshot(snap)
	if err != nil {
		return false, stats, err
	}
	if catalog == nil {
		return false, stats, errCollectionNotFound
	}
	return c.scanChunkDocumentsByParentPrefixAtSnapshot(snap, catalog, prefix, maxDocuments, fn)
}

func (c *Collection) scanChunkDocumentsByParentPrefixAtSnapshot(snap *backenddb.Snapshot, catalog *collectionCatalog, prefix []byte, maxDocuments int, fn func(DocumentRecord) (bool, error)) (bool, ChunkChildrenScanStats, error) {
	var stats ChunkChildrenScanStats
	if snap == nil || catalog == nil {
		return false, stats, fmt.Errorf("collections: chunk child snapshot and catalog are required")
	}
	if len(prefix) == 0 || maxDocuments <= 0 || fn == nil {
		return false, stats, fmt.Errorf("collections: chunk child prefix, positive bound, and callback are required")
	}
	it, err := collectionIteratorAtCatalogRoot(
		snap,
		catalog,
		collectionPrimaryRootName(catalog.meta.Name),
		prefix,
		prefixEnd(prefix),
		false,
	)
	if err != nil {
		return false, stats, err
	}
	if it == nil {
		return false, stats, nil
	}
	defer func() { _ = it.Close() }()
	if columnStoreCanReconstructDocument(catalog.meta) {
		var reconstruction CollectionDocumentScanStats
		truncated, err := c.scanDocumentsFuncWithColumnReconstruction(snap, catalog, it, maxDocuments, func(record DocumentRecord) (bool, error) {
			stats.ScannedPrimaryRows++
			stats.ReconstructedDocuments++
			return fn(record)
		}, &reconstruction)
		stats.RowLocatorLookups = int(reconstruction.LocatorLookups)
		stats.PointRowFetches = int(reconstruction.PointRowFetches)
		return truncated, stats, err
	}
	for it.Valid() {
		if it.IsDeleted() {
			it.Next()
			continue
		}
		if stats.ScannedPrimaryRows >= maxDocuments {
			return true, stats, nil
		}
		id := bytes.Clone(it.UnsafeKey())
		document := it.ValueCopy(nil)
		stats.ScannedPrimaryRows++
		next, err := fn(DocumentRecord{ID: id, Document: document})
		if err != nil {
			return false, stats, err
		}
		if !next {
			return false, stats, nil
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		return false, stats, err
	}
	return false, stats, nil
}

// ValidateChunkChildDocument verifies that a stored document with chunk
// metadata is a well-formed child matching the given document ID. Documents
// without chunk metadata pass trivially; partial or mismatched chunk metadata
// fails closed. This is the ingest-path guard against silently indexing
// orphaned chunks.
func ValidateChunkChildDocument(documentID []byte, document []byte) error {
	return chunking.ValidateChunkChild(string(documentID), document)
}
