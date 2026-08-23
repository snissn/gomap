package collections

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"

	"github.com/snissn/gomap/TreeDB/collections/chunking"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// chunkingScanMaxDocuments bounds full-collection child scans; collection
// scans are bounded calls, so chunk-child discovery uses a practical maximum.
const chunkingScanMaxDocuments = math.MaxInt32

// ChunkedIngestOptions tunes IngestChunkedDocument.
type ChunkedIngestOptions struct {
	// TextField names the parent document field holding the text to chunk.
	// Empty defaults to "body".
	TextField string
}

func (o ChunkedIngestOptions) textField() string {
	if o.TextField == "" {
		return "body"
	}
	return o.TextField
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
	if len(parentID) == 0 {
		return plan, fmt.Errorf("collections: chunked ingest requires a non-empty parent document ID")
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

// IngestChunkedDocument stores parentDocument under parentID and replaces its
// chunk children with the stream derived from the configured text field.
//
// Lifecycle: the child plan is built and validated first — invalid config,
// missing text field, or chunker failure leaves the collection untouched.
// Stale children (previous `<parentID>#<ordinal>` rows) are tombstoned with
// one atomic DeleteBatch, then the new children are inserted with one atomic
// InsertBatch. A crash between the batches cannot tear a child row (batch
// atomicity), leaves the parent intact with its prior or partial next child
// set, and a retry of the same ingest converges because child IDs derive from
// the parent ID and ordinal, never from content.
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
	plan, err := c.buildChunkPlan(parentID, parentDocument, cfg, opts)
	if err != nil {
		return result, err
	}

	// Upsert the parent document first: children reference it, so any crash
	// window leaves a consistent parent whose child set a retry repairs.
	if err := c.upsertParentDocument(parentID, parentDocument); err != nil {
		return result, err
	}

	oldChildren, err := c.ChunkChildren(parentID)
	if err != nil {
		return result, err
	}
	if len(oldChildren) > 0 {
		if _, err := c.DeleteBatch(oldChildren); err != nil {
			return result, fmt.Errorf("collections: tombstone stale chunk children of %q: %w", parentID, err)
		}
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

// ChunkChildren returns the live chunk child IDs of parentID in ordinal order.
// Children are identified by the derived `<parentID>#<ordinal>` ID scheme and
// verified against their stored linkage metadata; rows carrying malformed
// chunk metadata fail closed rather than being reported as children.
func (c *Collection) ChunkChildren(parentID []byte) ([][]byte, error) {
	if c == nil {
		return nil, errCollectionNil
	}
	if c.db == nil {
		return nil, errCollectionDBNil
	}
	prefix := append(append([]byte(nil), parentID...), '#')
	ordinals := map[int][]byte{}
	inspect := func(record DocumentRecord) (bool, error) {
		id := record.ID
		meta, err := chunking.ParseChildMeta(record.Document)
		if err != nil {
			return false, fmt.Errorf("collections: chunk child %q metadata: %w", id, err)
		}
		if meta == nil {
			// ID-shaped but not a linked chunk child: fail closed instead of
			// silently reporting an orphaned row.
			return false, fmt.Errorf("collections: document %q has a chunk child ID without chunk metadata", id)
		}
		if err := chunking.ValidateChunkChild(string(id), record.Document); err != nil {
			return false, fmt.Errorf("collections: chunk child %q linkage: %w", id, err)
		}
		if _, dup := ordinals[meta.Ordinal]; dup {
			return false, fmt.Errorf("collections: duplicate chunk ordinal %d for parent %q", meta.Ordinal, parentID)
		}
		ordinals[meta.Ordinal] = append([]byte(nil), id...)
		return true, nil
	}
	var truncated bool
	var err error
	if columnStoreCanReconstructDocument(c.meta) {
		truncated, err = c.ScanDocumentsFunc(chunkingScanMaxDocuments, inspect)
	} else {
		truncated, err = c.scanChunkDocumentsByParentPrefix(prefix, inspect)
	}
	if err != nil {
		return nil, err
	}
	if truncated {
		return nil, fmt.Errorf("collections: chunk child prefix scan for %q exceeded bound %d", parentID, chunkingScanMaxDocuments)
	}
	children := make([][]byte, 0, len(ordinals))
	for ordinal := 0; len(children) < len(ordinals); ordinal++ {
		id, ok := ordinals[ordinal]
		if !ok {
			return nil, fmt.Errorf("collections: chunk ordinal gap %d for parent %q", ordinal, parentID)
		}
		children = append(children, id)
	}
	return children, nil
}

// scanChunkDocumentsByParentPrefix walks only primary IDs in the requested
// parent# child namespace. The bounded range avoids a full collection scan for
// every source while retaining snapshot-consistent document validation.
func (c *Collection) scanChunkDocumentsByParentPrefix(prefix []byte, fn func(DocumentRecord) (bool, error)) (bool, error) {
	if c == nil {
		return false, errCollectionNil
	}
	if c.db == nil {
		return false, errCollectionDBNil
	}
	if len(prefix) == 0 || fn == nil {
		return false, fmt.Errorf("collections: chunk child prefix and callback are required")
	}
	if err := c.flushBufferedWrites(); err != nil {
		return false, err
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return false, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	catalog, err := c.catalogForSnapshot(snap)
	if err != nil {
		return false, err
	}
	if catalog == nil {
		return false, errCollectionNotFound
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
		return false, err
	}
	if it == nil {
		return false, nil
	}
	defer func() { _ = it.Close() }()
	scanned := 0
	for it.Valid() {
		if it.IsDeleted() {
			it.Next()
			continue
		}
		if scanned >= chunkingScanMaxDocuments {
			return true, nil
		}
		record := DocumentRecord{
			ID:       bytes.Clone(it.UnsafeKey()),
			Document: it.ValueCopy(nil),
		}
		scanned++
		next, err := fn(record)
		if err != nil {
			return false, err
		}
		if !next {
			return false, nil
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		return false, err
	}
	return false, nil
}

// ValidateChunkChildDocument verifies that a stored document with chunk
// metadata is a well-formed child matching the given document ID. Documents
// without chunk metadata pass trivially; partial or mismatched chunk metadata
// fails closed. This is the ingest-path guard against silently indexing
// orphaned chunks.
func ValidateChunkChildDocument(documentID []byte, document []byte) error {
	return chunking.ValidateChunkChild(string(documentID), document)
}
