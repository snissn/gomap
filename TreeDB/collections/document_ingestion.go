package collections

// IngestSources composes the built-in chunker (C2 seam, IngestChunkedDocument
// machinery) with the embedder seam (C3 seam, EmbedForIngest gates) into a
// one-call RAG ingestion pipeline: source documents in, chunked + embedded +
// indexed collection state out.
//
// # Lifecycle (fail closed, then bounded concurrent execution)
//
//  1. Batch validation runs before ANY mutation: config validation, embedder
//     resolution against the named vector index, and the full chunk plan for
//     every source. Any invalid source aborts the whole batch with a typed
//     chunk-stage error naming that source; the collection is untouched.
//  2. Sources execute through a bounded worker pool. Per source: embed the
//     planned children (pure, no mutation), then apply mutations in this
//     order — tombstone stale children (atomic DeleteBatch), insert new
//     children (atomic InsertBatch), upsert the parent document last so an
//     interrupted first ingest of a source leaves it fully absent.
//  3. The first failure cancels all remaining work. Sources whose batches had
//     not started are untouched; sources that completed stay intact.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/snissn/gomap/TreeDB/collections/chunking"
	"github.com/snissn/gomap/TreeDB/collections/embedding"
)

// SourceDocument is one parent document to ingest. Fields holds the document
// fields (the configured TextField must carry a string); Meta is optional
// metadata stored under the reserved "meta" field of the parent document.
type SourceDocument struct {
	ID     []byte
	Fields map[string]any
	Meta   map[string]any
}

const ingestSourceMetaField = "meta"

func buildIngestParentDocument(sd SourceDocument) ([]byte, error) {
	if len(sd.ID) == 0 {
		return nil, errors.New("collections: ingest requires a non-empty source document ID")
	}
	if sd.Fields == nil {
		return nil, fmt.Errorf("collections: ingest source %q requires non-nil fields", sd.ID)
	}
	doc := make(map[string]any, len(sd.Fields)+1)
	for k, v := range sd.Fields {
		if k == ingestSourceMetaField && len(sd.Meta) > 0 {
			return nil, fmt.Errorf("collections: ingest source %q field %q collides with Meta", sd.ID, k)
		}
		doc[k] = v
	}
	if len(sd.Meta) > 0 {
		doc[ingestSourceMetaField] = sd.Meta
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("collections: encode ingest source %q: %w", sd.ID, err)
	}
	return raw, nil
}

// IngestStage classifies where a per-source ingestion failure happened.
type IngestStage int

const (
	// IngestStageChunk covers plan-time failures: invalid batch config,
	// missing or non-string text field, chunker rejection, child encoding.
	IngestStageChunk IngestStage = iota + 1
	// IngestStageEmbed covers embedder resolution and EmbedBatch failures,
	// including context cancellation during embedding.
	IngestStageEmbed
	// IngestStageStorage covers collection mutation failures. A storage-stage
	// error at a batch boundary is commit-ambiguous for the failing source:
	// see the IngestSources durability contract.
	IngestStageStorage
)

func (s IngestStage) String() string {
	switch s {
	case IngestStageChunk:
		return "chunk"
	case IngestStageEmbed:
		return "embed"
	case IngestStageStorage:
		return "storage"
	default:
		return fmt.Sprintf("IngestStage(%d)", int(s))
	}
}

// IngestError is the typed per-source failure. Callers test with errors.As
// (*IngestError) plus Stage/SourceID inspection, and unwrap Err with errors.Is.
type IngestError struct {
	// SourceID is the ID of the failed source document.
	SourceID []byte
	// SourceIndex is the failed source's position in the input slice.
	SourceIndex int
	// Stage names the pipeline stage that failed.
	Stage IngestStage
	// Err is the underlying cause, wrapped for errors.Is chains.
	Err error
}

func (e *IngestError) Error() string {
	return fmt.Sprintf("collections: ingest source %q (index %d) failed at %s stage: %v",
		e.SourceID, e.SourceIndex, e.Stage, e.Err)
}

func (e *IngestError) Unwrap() error { return e.Err }

// SourceIngestOutcome reports one fully committed source.
type SourceIngestOutcome struct {
	// ID is the parent source document ID.
	ID []byte
	// ChildIDs are the live chunk child IDs after ingest, ordinal order.
	ChildIDs [][]byte
	// Replaced is the number of stale children tombstoned by this ingest.
	Replaced int
}

// IngestResult reports the outcome of an IngestSources call.
type IngestResult struct {
	// Ingested lists every source known to be fully applied, ascending input
	// order. On error it contains only sources whose batches fully committed;
	// the failed source and every unstarted source are absent.
	Ingested []SourceIngestOutcome
	// ChunkNanos, EmbedNanos, and IndexNanos accumulate wall time spent in the
	// chunk-planning, embedding, and index-mutation stages across all sources.
	ChunkNanos int64
	EmbedNanos int64
	IndexNanos int64
}

// IngestSourcesProgress is delivered to Config.Progress after each committed
// source.
type IngestSourcesProgress struct {
	SourcesCompleted int
	SourcesTotal     int
	SourceID         []byte
}

// IngestSourcesConfig configures IngestSources. Chunking and Embedding follow
// their respective package contracts; both fail closed via Validate.
type IngestSourcesConfig struct {
	// Chunking selects the C2 chunk strategy for every source.
	Chunking chunking.Config
	// Embedding selects the C3 embedder provider and declares dimensions;
	// they must match the VectorIndexName definition exactly.
	Embedding embedding.Config
	// VectorIndexName names the collection vector index definition children
	// are embedded into. Required.
	VectorIndexName string
	// TextField names the parent field holding text to chunk. Empty defaults
	// to "body" (matching ChunkedIngestOptions).
	TextField string
	// Concurrency bounds the source worker pool. Zero defaults to 4; values
	// larger than the source count clamp down. Calls to the one shared Embedder
	// instance remain serialized because providers need not be thread-safe.
	Concurrency int
	// Progress is invoked after each committed source. Callbacks are serialized
	// so callers can update a single progress display without their own lock.
	Progress func(IngestSourcesProgress)

	// hooks are package-test fault-injection points at the batch boundaries.
	hooks *ingestFaultHooks
}

// ingestFaultHooks injects failures at per-source mutation boundaries without
// touching production behavior. Tests construct it directly; nil callbacks
// never fire.
type ingestFaultHooks struct {
	// beforeSource fires before any mutation of source i.
	beforeSource func(i int) error
	// beforeInsert fires before either mutation boundary, simulating a storage
	// failure without leaving an orphaned child set.
	beforeInsert func(i int) error
}

// textField resolves the effective text field name.
func (cfg IngestSourcesConfig) textField() string {
	if cfg.TextField == "" {
		return "body"
	}
	return cfg.TextField
}

// validate runs the fail-closed checks that do not need collection state.
func (cfg *IngestSourcesConfig) validate() error {
	if err := cfg.Chunking.Validate(); err != nil {
		return fmt.Errorf("collections: ingest chunking config: %w", err)
	}
	if err := cfg.Embedding.Validate(); err != nil {
		return fmt.Errorf("collections: ingest embedding config: %w", err)
	}
	if cfg.VectorIndexName == "" {
		return errors.New("collections: ingest requires VectorIndexName")
	}
	return nil
}

// ingestPlan is the validated, mutation-free per-source work unit.
type ingestPlan struct {
	source   SourceDocument
	parentID []byte
	parent   []byte
	children []chunkChild
	texts    [][]byte
}

// IngestSources chunks, embeds, and indexes sources in one call.
//
// # Atomicity And Durability Contract
//
// Planning and embedding happen strictly before mutation. Therefore chunk or
// embed failures leave the entire batch untouched (for planning failures) or
// the failed source absent (for per-source embed failures); prior committed
// sources remain intact. Each child insert/delete batch is itself atomic and
// maintains text, scalar, and vector indexes together.
//
// A source replacement uses separate stale-child delete and new-child insert
// commits because the collection API exposes those mutation boundaries
// independently. A storage error after one boundary is therefore
// commit-ambiguous for that source: it may retain the old child set, have the
// new child set, or be between those states. The error identifies the source
// and storage stage; callers should retry, and deterministic child IDs make a
// retry converge without duplicates. The normal fault-injection hook runs
// before either boundary and leaves the source unchanged.
//
// # Idempotency
//
// Re-ingesting an unchanged source is a clean replace: deterministic child IDs
// (`<parentID>#<ordinal>`) converge to one live child per ordinal. Changed
// sources similarly replace stale children on a fault-free retry.
//
// On success every source appears in Result.Ingested. On the first failure
// remaining work is canceled and the typed *IngestError is returned alongside
// the partially populated result.
func (c *Collection) IngestSources(ctx context.Context, sources []SourceDocument, cfg IngestSourcesConfig) (IngestResult, error) {
	var result IngestResult
	if c == nil {
		return result, errCollectionNil
	}
	if c.db == nil {
		return result, errCollectionDBNil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if err := cfg.Chunking.Validate(); err != nil {
		return result, &IngestError{Stage: IngestStageChunk, SourceIndex: 0, Err: fmt.Errorf("collections: ingest chunking config: %w", err)}
	}
	if err := cfg.Embedding.Validate(); err != nil {
		return result, &IngestError{Stage: IngestStageEmbed, SourceIndex: 0, Err: fmt.Errorf("collections: ingest embedding config: %w", err)}
	}
	if cfg.VectorIndexName == "" {
		return result, &IngestError{Stage: IngestStageEmbed, SourceIndex: 0, Err: errors.New("collections: ingest requires VectorIndexName")}
	}
	emb, err := c.EmbedderForIngest(cfg.VectorIndexName, cfg.Embedding)
	if err != nil {
		return result, &IngestError{Stage: IngestStageEmbed, SourceIndex: 0, Err: err}
	}
	def, ok := findVectorIndex(c.meta.VectorIndexes, cfg.VectorIndexName)
	if !ok || def.Field == "" {
		return result, &IngestError{
			Stage:       IngestStageEmbed,
			SourceIndex: 0,
			Err:         fmt.Errorf("collections: ingest into vector index %q: missing target field", cfg.VectorIndexName),
		}
	}
	vectorField := def.Field
	textPath, err := parseVectorFieldPath(cfg.textField())
	if err != nil {
		return result, &IngestError{
			Stage:       IngestStageChunk,
			SourceIndex: 0,
			Err:         fmt.Errorf("collections: invalid ingest TextField %q: %w", cfg.textField(), err),
		}
	}
	vectorPath, err := parseVectorFieldPath(vectorField)
	if err != nil {
		return result, &IngestError{
			Stage:       IngestStageEmbed,
			SourceIndex: 0,
			Err:         fmt.Errorf("collections: invalid vector index field %q: %w", vectorField, err),
		}
	}
	for _, reserved := range []string{
		chunking.MetaFieldParent,
		chunking.MetaFieldOrdinal,
		chunking.MetaFieldKind,
	} {
		if vectorPath[0] == reserved {
			return result, &IngestError{
				Stage:       IngestStageChunk,
				SourceIndex: 0,
				Err:         fmt.Errorf("collections: vector index field %q overlaps reserved chunk linkage metadata field %q", vectorField, reserved),
			}
		}
	}
	overlap := len(textPath) <= len(vectorPath)
	if overlap {
		for i := range textPath {
			if textPath[i] != vectorPath[i] {
				overlap = false
				break
			}
		}
	}
	if !overlap && len(vectorPath) <= len(textPath) {
		overlap = true
		for i := range vectorPath {
			if vectorPath[i] != textPath[i] {
				overlap = false
				break
			}
		}
	}
	if overlap {
		return result, &IngestError{
			Stage:       IngestStageChunk,
			SourceIndex: 0,
			Err:         fmt.Errorf("collections: ingest TextField %q overlaps vector index field %q", cfg.textField(), vectorField),
		}
	}

	// Phase 1: plan every source up front. Nothing below can mutate the
	// collection until every plan validates, so plan failures leave the batch
	// fully absent.
	chunkStart := time.Now()
	plans := make([]ingestPlan, len(sources))
	seen := make(map[string]struct{}, len(sources))
	for i, sd := range sources {
		if err := ctx.Err(); err != nil {
			result.ChunkNanos = time.Since(chunkStart).Nanoseconds()
			return result, err
		}
		parent, err := buildIngestParentDocument(sd)
		if err == nil {
			plan, planErr := c.buildChunkPlan(sd.ID, parent, cfg.Chunking, ChunkedIngestOptions{TextField: cfg.TextField})
			if planErr == nil {
				texts, textErr := extractChunkTexts(plan.children, cfg.textField())
				if textErr == nil {
					plans[i] = ingestPlan{source: sd, parentID: append([]byte(nil), sd.ID...), parent: parent, children: plan.children, texts: texts}
				} else {
					err = textErr
				}
			} else {
				err = planErr
			}
		}
		if err == nil {
			key := string(sd.ID)
			if _, dup := seen[key]; dup {
				err = fmt.Errorf("duplicate source document ID %q in batch", sd.ID)
			} else {
				seen[key] = struct{}{}
			}
		}
		if err != nil {
			result.ChunkNanos = time.Since(chunkStart).Nanoseconds()
			return result, &IngestError{SourceID: append([]byte(nil), sd.ID...), SourceIndex: i, Stage: IngestStageChunk, Err: err}
		}
	}
	for i := range plans {
		for j := range i {
			previous := plans[j].parentID
			current := plans[i].parentID
			if ingestParentInChildNamespace(previous, current) || ingestParentInChildNamespace(current, previous) {
				result.ChunkNanos = time.Since(chunkStart).Nanoseconds()
				return result, &IngestError{
					SourceID:    append([]byte(nil), current...),
					SourceIndex: i,
					Stage:       IngestStageChunk,
					Err:         fmt.Errorf("collections: source parent ID %q overlaps parent %q child namespace", current, previous),
				}
			}
		}
	}
	namespace := make(map[string]string)
	for i := range plans {
		plan := &plans[i]
		addID := func(id []byte, kind string) *IngestError {
			key := string(id)
			if previous, exists := namespace[key]; exists {
				return &IngestError{
					SourceID:    append([]byte(nil), plan.parentID...),
					SourceIndex: i,
					Stage:       IngestStageChunk,
					Err:         fmt.Errorf("collections: %s ID %q collides with %s", kind, id, previous),
				}
			}
			namespace[key] = fmt.Sprintf("%s from source %q", kind, plan.parentID)
			return nil
		}
		if collision := addID(plan.parentID, "source parent"); collision != nil {
			result.ChunkNanos = time.Since(chunkStart).Nanoseconds()
			return result, collision
		}
		for _, child := range plan.children {
			if collision := addID(child.id, "planned child"); collision != nil {
				result.ChunkNanos = time.Since(chunkStart).Nanoseconds()
				return result, collision
			}
		}
	}

	result.ChunkNanos = time.Since(chunkStart).Nanoseconds()

	if len(plans) == 0 {
		return result, nil
	}

	// Phase 2: bounded worker pool over plans.
	workers := cfg.Concurrency
	if workers <= 0 {
		workers = 4
	}
	if workers > len(plans) {
		workers = len(plans)
	}
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		mu         sync.Mutex // guards firstErr, committed, outcomes, and stage counters
		progressMu sync.Mutex // serializes user callbacks without holding mu
		mutationMu sync.Mutex // collection mutation/index internals are not concurrent
		embedMu    sync.Mutex // one public embedder instance may not be concurrency-safe
		firstErr   error
		next       atomic.Int64
		outcomes   = make([]SourceIngestOutcome, len(plans))
		committed  = make([]bool, len(plans))
	)
	fail := func(i int, stageErr *IngestError) {
		mu.Lock()
		if firstErr == nil {
			firstErr = stageErr
		}
		mu.Unlock()
		cancel()
	}

	runOne := func(i int) *IngestError {
		plan := &plans[i]
		if err := wctx.Err(); err != nil {
			return &IngestError{SourceID: plan.parentID, SourceIndex: i, Stage: IngestStageEmbed,
				Err: fmt.Errorf("collections: provider %q embed for vector index %q: %w", cfg.Embedding.Provider, def.Name, err)}
		}
		if cfg.hooks != nil && cfg.hooks.beforeSource != nil {
			if err := cfg.hooks.beforeSource(i); err != nil {
				return &IngestError{SourceID: plan.parentID, SourceIndex: i, Stage: IngestStageStorage,
					Err: fmt.Errorf("collections: before source %q: %w", plan.parentID, err)}
			}
		}
		embedMu.Lock()
		if err := wctx.Err(); err != nil {
			embedMu.Unlock()
			return &IngestError{SourceID: plan.parentID, SourceIndex: i, Stage: IngestStageEmbed,
				Err: fmt.Errorf("collections: provider %q embed for vector index %q: %w", cfg.Embedding.Provider, def.Name, err)}
		}
		embedStart := time.Now()
		vectors, embedErr := c.embedIngestChildren(wctx, emb, plan, cfg.Embedding.Provider, def.Name)
		embedMu.Unlock()
		mu.Lock()
		result.EmbedNanos += time.Since(embedStart).Nanoseconds()
		mu.Unlock()
		if embedErr != nil {
			return &IngestError{SourceID: plan.parentID, SourceIndex: i, Stage: IngestStageEmbed, Err: embedErr}
		}
		if err := validateEmbeddingOutput(vectors, len(plan.texts), def); err != nil {
			return &IngestError{
				SourceID:    plan.parentID,
				SourceIndex: i,
				Stage:       IngestStageEmbed,
				Err:         fmt.Errorf("collections: validate provider %q output for vector index %q: %w", cfg.Embedding.Provider, def.Name, err),
			}
		}
		if err := wctx.Err(); err != nil {
			return &IngestError{SourceID: plan.parentID, SourceIndex: i, Stage: IngestStageEmbed,
				Err: fmt.Errorf("collections: provider %q embed for vector index %q: %w", cfg.Embedding.Provider, def.Name, err)}
		}
		childDocs, attachErr := attachVectorsToChildren(plan.children, vectors, vectorField)
		if attachErr != nil {
			return &IngestError{SourceID: plan.parentID, SourceIndex: i, Stage: IngestStageEmbed, Err: attachErr}
		}

		indexStart := time.Now()
		// Fault injection is deliberately before either mutation boundary. This
		// keeps the normal test fault path free of orphaned child rows.
		if cfg.hooks != nil && cfg.hooks.beforeInsert != nil {
			if err := cfg.hooks.beforeInsert(i); err != nil {
				return &IngestError{SourceID: plan.parentID, SourceIndex: i, Stage: IngestStageStorage,
					Err: fmt.Errorf("collections: before source %q insert: %w", plan.parentID, err)}
			}
		}
		if err := wctx.Err(); err != nil {
			return &IngestError{SourceID: plan.parentID, SourceIndex: i, Stage: IngestStageStorage, Err: err}
		}
		apply := func() ([][]byte, *IngestError) {
			mutationMu.Lock()
			defer mutationMu.Unlock()
			oldChildren, err := c.ChunkChildren(plan.parentID)
			if err != nil {
				return nil, storageIngestError(plan.parentID, i, "enumerate stale chunk children", err)
			}
			if err := wctx.Err(); err != nil {
				return nil, &IngestError{SourceID: plan.parentID, SourceIndex: i, Stage: IngestStageStorage, Err: err}
			}
			if len(oldChildren) > 0 {
				if _, err := c.DeleteBatch(oldChildren); err != nil {
					return nil, storageIngestError(plan.parentID, i, "tombstone stale chunk children", err)
				}
			}
			if len(plan.children) > 0 {
				if _, err := c.InsertBatch(chunkPlanIDs(plan.children), childDocs); err != nil {
					return nil, storageIngestError(plan.parentID, i, "insert chunk children", err)
				}
			}
			if err := c.upsertParentDocument(plan.parentID, plan.parent); err != nil {
				return nil, storageIngestError(plan.parentID, i, "upsert parent document", err)
			}
			return oldChildren, nil
		}
		oldChildren, applyErr := apply()
		if applyErr != nil {
			return applyErr
		}
		indexNanos := time.Since(indexStart).Nanoseconds()

		childIDs := make([][]byte, len(plan.children))
		for j, ch := range plan.children {
			childIDs[j] = append([]byte(nil), ch.id...)
		}
		outcome := SourceIngestOutcome{ID: append([]byte(nil), plan.parentID...), ChildIDs: childIDs, Replaced: len(oldChildren)}
		if cfg.Progress != nil {
			// Assign the completion number and deliver its callback under the
			// same lock. A worker that commits later cannot overtake an
			// earlier callback while retaining a smaller SourcesCompleted.
			progressMu.Lock()
		}
		mu.Lock()
		result.IndexNanos += indexNanos
		outcomes[i] = outcome
		committed[i] = true
		completedNow := 0
		for _, done := range committed {
			if done {
				completedNow++
			}
		}
		progress := IngestSourcesProgress{
			SourcesCompleted: completedNow,
			SourcesTotal:     len(plans),
			SourceID:         append([]byte(nil), outcome.ID...),
		}
		mu.Unlock()
		if cfg.Progress != nil {
			cfg.Progress(progress)
			progressMu.Unlock()
		}
		return nil
	}

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				// Stop on external cancellation or a sibling's failure:
				// unstarted sources remain untouched between sources.
				mu.Lock()
				localErr := firstErr
				mu.Unlock()
				if localErr != nil || wctx.Err() != nil {
					return
				}
				i := int(next.Add(1)) - 1
				if i >= len(plans) {
					return
				}
				if stageErr := runOne(i); stageErr != nil {
					fail(i, stageErr)
				}
			}
		}()
	}
	wg.Wait()

	mu.Lock()
	err = firstErr
	mu.Unlock()
	if err == nil && ctx.Err() != nil {
		err = ctx.Err()
	}

	for i := range plans {
		if committed[i] {
			result.Ingested = append(result.Ingested, outcomes[i])
		}
	}
	return result, err
}

func ingestParentInChildNamespace(parent, candidate []byte) bool {
	return len(candidate) > len(parent) &&
		bytes.HasPrefix(candidate, parent) &&
		candidate[len(parent)] == '#'
}

func storageIngestError(sourceID []byte, index int, action string, err error) *IngestError {
	return &IngestError{
		SourceID:    append([]byte(nil), sourceID...),
		SourceIndex: index,
		Stage:       IngestStageStorage,
		Err:         fmt.Errorf("collections: %s of %q: %w", action, sourceID, err),
	}
}

// embedIngestChildren runs the C3 embed seam over a plan's child texts.
func (c *Collection) embedIngestChildren(ctx context.Context, emb embedding.Embedder, plan *ingestPlan, provider, vectorIndexName string) ([][]float32, error) {
	if len(plan.texts) == 0 {
		return nil, nil
	}
	vectors, err := emb.EmbedBatch(ctx, plan.texts)
	if err != nil {
		return nil, fmt.Errorf("collections: provider %q embed chunk children of %q for vector index %q: %w",
			provider, plan.parentID, vectorIndexName, err)
	}
	return vectors, nil
}

func validateEmbeddingOutput(vectors [][]float32, wantCount int, def VectorIndexDefinition) error {
	if len(vectors) != wantCount {
		return fmt.Errorf("%w: got %d vectors for %d texts", embedding.ErrInvalidOutput, len(vectors), wantCount)
	}
	if err := validateIngestVectors(vectors, def); err != nil {
		return fmt.Errorf("%w: %w", embedding.ErrInvalidOutput, err)
	}
	return nil
}

// validateIngestVectors applies the target index's document-vector contract
// before a replacement can reach either mutation boundary.
func validateIngestVectors(vectors [][]float32, def VectorIndexDefinition) error {
	if len(vectors) == 0 {
		return nil
	}
	if def.Dimensions <= 0 {
		return fmt.Errorf("vector index %q has invalid dimensions %d", def.Name, def.Dimensions)
	}
	metric, err := normalizeVectorMetric(def.Metric)
	if err != nil {
		return err
	}
	for i, vector := range vectors {
		if len(vector) != def.Dimensions {
			return fmt.Errorf("vector[%d] dimensions=%d want %d: %w", i, len(vector), def.Dimensions, embedding.ErrDimensionMismatch)
		}
		if err := validateFloat32Vector(vector); err != nil {
			return fmt.Errorf("vector[%d]: %w", i, err)
		}
		if metric == VectorMetricCosine && vectorNormSquared(vector) == 0 {
			return fmt.Errorf("vector[%d]: cosine vector cannot have zero magnitude", i)
		}
	}
	return nil
}

// extractChunkTexts pulls each planned child's text back out of its stored
// document JSON so the embedder consumes exactly what will be indexed.
func extractChunkTexts(children []chunkChild, textField string) ([][]byte, error) {
	texts := make([][]byte, len(children))
	for i, ch := range children {
		var doc map[string]json.RawMessage
		if err := json.Unmarshal(ch.document, &doc); err != nil {
			return nil, fmt.Errorf("collections: decode planned chunk child %q: %w", ch.id, err)
		}
		raw, ok := doc[textField]
		if !ok {
			return nil, fmt.Errorf("collections: planned chunk child %q lost its %q field", ch.id, textField)
		}
		var text string
		if err := json.Unmarshal(raw, &text); err != nil {
			return nil, fmt.Errorf("collections: planned chunk child %q field %q must be a string: %w", ch.id, textField, err)
		}
		texts[i] = []byte(text)
	}
	return texts, nil
}

// setIngestJSONRawPath writes value through parsed object components rather
// than creating a literal dotted key.
func setIngestJSONRawPath(doc map[string]json.RawMessage, path []string, value json.RawMessage) error {
	if len(path) == 0 {
		return errors.New("collections: empty ingest JSON path")
	}
	if len(path) == 1 {
		doc[path[0]] = value
		return nil
	}
	var child map[string]json.RawMessage
	if raw, ok := doc[path[0]]; ok {
		if err := json.Unmarshal(raw, &child); err != nil {
			return fmt.Errorf("path %q has non-object ancestor %q", strings.Join(path, "."), path[0])
		}
		if child == nil {
			return fmt.Errorf("path %q has non-object ancestor %q", strings.Join(path, "."), path[0])
		}
	} else {
		child = make(map[string]json.RawMessage)
	}
	if err := setIngestJSONRawPath(child, path[1:], value); err != nil {
		return err
	}
	raw, err := json.Marshal(child)
	if err != nil {
		return err
	}
	doc[path[0]] = raw
	return nil
}

// attachVectorsToChildren re-encodes each planned child document with its
// embedded vector under the vector index's target field.
func attachVectorsToChildren(children []chunkChild, vectors [][]float32, vectorField string) ([][]byte, error) {
	fieldPath, err := parseVectorFieldPath(vectorField)
	if err != nil {
		return nil, err
	}
	docs := make([][]byte, len(children))
	for i, ch := range children {
		var doc map[string]json.RawMessage
		if err := json.Unmarshal(ch.document, &doc); err != nil {
			return nil, fmt.Errorf("collections: decode child %q for vector attachment: %w", ch.id, err)
		}
		if i >= len(vectors) || vectors[i] == nil {
			return nil, fmt.Errorf("collections: missing vector for child %q", ch.id)
		}
		raw, err := json.Marshal(vectors[i])
		if err != nil {
			return nil, fmt.Errorf("collections: encode vector for child %q: %w", ch.id, err)
		}
		if err := setIngestJSONRawPath(doc, fieldPath, raw); err != nil {
			return nil, fmt.Errorf("collections: attach vector to child %q: %w", ch.id, err)
		}
		encoded, err := json.Marshal(doc)
		if err != nil {
			return nil, fmt.Errorf("collections: encode child %q with vector: %w", ch.id, err)
		}
		docs[i] = encoded
	}
	return docs, nil
}
