package collections

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"sync"

	"github.com/snissn/gomap/TreeDB/collections/chunking"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// chunkingScanMaxDocuments is the fail-closed cap for one parent# prefix.
const chunkingScanMaxDocuments = math.MaxInt32

// ChunkedIngestOptions tunes IngestChunkedDocument.
type ChunkedIngestOptions struct {
	// TextField names the parent document field holding the text to chunk.
	// Empty defaults to "body".
	TextField string
	hooks     *chunkedIngestHooks
}

type chunkedIngestHooks struct {
	afterDelete func()
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

// buildChunkPlan derives the child rows for a chunked ingest without touching
// the collection. Everything that can fail — config validation, text field
// extraction, chunking, metadata encoding, and child linkage validation —
// fails closed here, before any mutation.
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
		document, err := json.Marshal(map[string]any{
			opts.textField():          ch.Text,
			chunking.MetaFieldParent:  ch.ParentID,
			chunking.MetaFieldOrdinal: ch.Ordinal,
			chunking.MetaFieldKind:    chunking.KindChunk,
		})
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
// chunk children with the stream derived from the configured text field.
//
// Lifecycle: parent ID, text field, and child plan validate before mutation.
// A per-parent lock shared across collection handles serializes plan through
// replacement. The parent upsert, stale-child DeleteBatch, and replacement
// InsertBatch remain separate durable commits: each batch is atomic, but an
// error between boundaries is commit-ambiguous. The source may be old, new, or
// between those states; retrying converges because child IDs derive only from
// parent ID and ordinal. Atomic durable publication is deferred to #4284.
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
	truncated, stats, err := c.scanChunkDocumentsByParentPrefix(prefix, chunkingScanMaxDocuments, inspect)
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
