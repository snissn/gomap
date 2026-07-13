package typedcolumn

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// QueryReadyGenerationBase identifies an ordinary QRBG in an open file set.
// The non-zero values are the QRDG format kinds declared in M2.
const QueryReadyGenerationBase QueryReadyGenerationKind = 0

// QueryReadyGenerationFileOpenSupported reports whether this platform can
// retain query-ready generation files as read-only mapped direct views.
func QueryReadyGenerationFileOpenSupported() bool {
	return queryReadyBaseMmapSupported()
}

// QueryReadyOpenState is the externally observable state of one
// generation-scoped open cache. The cache is deliberately not a publication
// registry, root selector, or reclamation owner.
type QueryReadyOpenState string

const (
	QueryReadyOpenAbsentRebuildable  QueryReadyOpenState = "absent_rebuildable"
	QueryReadyOpenValidating         QueryReadyOpenState = "validating"
	QueryReadyOpenReady              QueryReadyOpenState = "ready"
	QueryReadyOpenUnsupportedOrStale QueryReadyOpenState = "unsupported_or_stale"
	QueryReadyOpenCorrupt            QueryReadyOpenState = "corrupt"
)

// QueryReadyGenerationOpenKey binds prepared state to the query-independent
// generation/schema identity and the caller's authoritative manifest identity.
// Collection integration should derive ManifestHash from its existing
// ColumnStoreCacheIdentity rather than from query arguments.
type QueryReadyGenerationOpenKey struct {
	Identity     QueryReadyBaseIdentity
	ManifestHash [sha256.Size]byte
}

type QueryReadyGenerationFile struct {
	Path     string
	Offset   int64
	Length   int64
	Identity QueryReadyBaseIdentity
	Kind     QueryReadyGenerationKind
}

// QueryReadyGenerationOpenFiles is an already-selected, non-authoritative
// physical file set. Selection and reachability remain with the collection
// publication/lifecycle owners; open only validates and prepares these files.
type QueryReadyGenerationOpenFiles struct {
	Key                QueryReadyGenerationOpenKey
	Base               QueryReadyGenerationFile
	Deltas             []QueryReadyGenerationFile
	SnapshotGeneration uint64
	Bound              QueryReadyDeltaBoundPolicy
}

type QueryReadyGenerationOpenStats struct {
	State                      QueryReadyOpenState
	OpenAttempts               int
	ColdOpens                  int
	Published                  int
	MappedFiles                int
	MappedBytes                int64
	LogicalImageBytes          int64
	StructuresValidated        int
	BytesValidated             int64
	PayloadBytesDecoded        int64
	PayloadBytesCopied         int64
	PartsDecoded               int
	DomainsConstructed         int
	RanksConstructed           int
	OffsetsConstructed         int
	WholePartDecodesDuringOpen int
	WholePartDecodesAfterOpen  int
	Rebuilds                   int
	Waits                      int
	CacheHits                  int
	ValidationTime             time.Duration
	OpenTime                   time.Duration
	WaitTime                   time.Duration
}

type QueryReadyGenerationOpenError struct {
	State QueryReadyOpenState
	Err   error
}

func (e *QueryReadyGenerationOpenError) Error() string {
	if e == nil {
		return "typedcolumn: query-ready generation open failed"
	}
	return fmt.Sprintf("typedcolumn: query-ready generation open state=%s: %v", e.State, e.Err)
}

func (e *QueryReadyGenerationOpenError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// QueryReadyPreparedGeneration is immutable after construction. Its query
// methods only read the query-independent visibility/domain state built during
// explicit Open; they do not lazily construct query-shaped state.
type QueryReadyPreparedGeneration struct {
	key          QueryReadyGenerationOpenKey
	base         *QueryReadyBaseGeneration
	consolidated *QueryReadyDeltaGeneration
	deltas       []*QueryReadyDeltaGeneration
	parts        []QueryReadyPreparedPartView
	tombstones   []Tombstone
	execution    *QueryReadyBaseDeltaReader
	closeOnce    sync.Once
	closeErr     error
	closed       atomic.Bool
}

type QueryReadyPreparedPartView struct {
	Role       PartRole
	Generation uint64
	View       QueryReadyBasePartView
}

func (p *QueryReadyPreparedGeneration) Key() QueryReadyGenerationOpenKey {
	if p == nil {
		return QueryReadyGenerationOpenKey{}
	}
	return p.key
}

func (p *QueryReadyPreparedGeneration) PartCount() int {
	if p == nil {
		return 0
	}
	return len(p.parts)
}

func (p *QueryReadyPreparedGeneration) Part(index int) (QueryReadyPreparedPartView, bool) {
	if p == nil || index < 0 || index >= len(p.parts) {
		return QueryReadyPreparedPartView{}, false
	}
	return p.parts[index], true
}

func (p *QueryReadyPreparedGeneration) DeltaCount() int {
	if p == nil {
		return 0
	}
	return len(p.deltas)
}

func (p *QueryReadyPreparedGeneration) TombstoneCount() int {
	if p == nil {
		return 0
	}
	return len(p.tombstones)
}

func (p *QueryReadyPreparedGeneration) Tombstone(index int) (Tombstone, bool) {
	if p == nil || index < 0 || index >= len(p.tombstones) {
		return Tombstone{}, false
	}
	return p.tombstones[index], true
}

func (p *QueryReadyPreparedGeneration) Closed() bool {
	return p == nil || p.closed.Load()
}

func (p *QueryReadyPreparedGeneration) Close() error {
	if p == nil {
		return nil
	}
	p.closeOnce.Do(func() {
		p.closed.Store(true)
		var errs []error
		for i := len(p.deltas) - 1; i >= 0; i-- {
			errs = append(errs, p.deltas[i].Close())
		}
		if p.consolidated != nil {
			errs = append(errs, p.consolidated.Close())
		}
		if p.base != nil {
			errs = append(errs, p.base.Close())
		}
		p.closeErr = errors.Join(errs...)
		p.parts = nil
		p.tombstones = nil
		p.execution = nil
	})
	return p.closeErr
}

// QueryReadyGenerationOpenCache is a single generation/manifest scoped
// singleflight. It does not discover assets or retain multiple history roots.
// Callers create a new cache when their existing authoritative identity changes
// and close the old cache only after readers are quiescent.
type QueryReadyGenerationOpenCache struct {
	mu           sync.Mutex
	cond         *sync.Cond
	key          QueryReadyGenerationOpenKey
	state        QueryReadyOpenState
	prepared     *QueryReadyPreparedGeneration
	lastErr      error
	stats        QueryReadyGenerationOpenStats
	bound        QueryReadyDeltaBoundPolicy
	needsRebuild bool
	closed       bool
}

func NewQueryReadyGenerationOpenCache(key QueryReadyGenerationOpenKey) *QueryReadyGenerationOpenCache {
	c := &QueryReadyGenerationOpenCache{key: key, state: QueryReadyOpenAbsentRebuildable}
	c.cond = sync.NewCond(&c.mu)
	c.stats.State = c.state
	return c
}

// Open returns a borrowed immutable pointer owned by this cache. Direct callers
// own lifetime coordination: they must keep the cache open while the pointer is
// in use, quiesce readers before Cache.Close, and must not close the published
// prepared generation themselves. The production collection integration wraps
// this pointer in a reader lease instead of exposing the raw lifetime.
func (c *QueryReadyGenerationOpenCache) Open(files QueryReadyGenerationOpenFiles) (*QueryReadyPreparedGeneration, error) {
	if c == nil {
		return nil, &QueryReadyGenerationOpenError{State: QueryReadyOpenUnsupportedOrStale, Err: errors.New("nil open cache")}
	}
	c.mu.Lock()
	for {
		if c.closed {
			err := &QueryReadyGenerationOpenError{State: QueryReadyOpenUnsupportedOrStale, Err: errors.New("open cache is closed")}
			c.mu.Unlock()
			return nil, err
		}
		switch c.state {
		case QueryReadyOpenReady:
			if files.Key != c.key || files.SnapshotGeneration != c.key.Identity.Generation {
				err := &QueryReadyGenerationOpenError{State: QueryReadyOpenUnsupportedOrStale, Err: errors.New("query-ready warm open identity is stale")}
				c.mu.Unlock()
				return nil, err
			}
			if files.Bound != c.bound {
				if err := validateQueryReadyGenerationOpenFiles(c.key, files); err != nil {
					wrapped := &QueryReadyGenerationOpenError{State: classifyQueryReadyGenerationOpenError(err), Err: err}
					c.mu.Unlock()
					return nil, wrapped
				}
				if err := validateQueryReadyPreparedGenerationBound(c.prepared, files.SnapshotGeneration, files.Bound); err != nil {
					wrapped := &QueryReadyGenerationOpenError{State: classifyQueryReadyGenerationOpenError(err), Err: err}
					c.mu.Unlock()
					return nil, wrapped
				}
			}
			c.stats.CacheHits++
			prepared := c.prepared
			c.mu.Unlock()
			return prepared, nil
		case QueryReadyOpenValidating:
			started := time.Now()
			c.stats.Waits++
			c.cond.Wait()
			c.stats.WaitTime += time.Since(started)
			continue
		case QueryReadyOpenUnsupportedOrStale, QueryReadyOpenCorrupt:
			err := c.lastErr
			c.mu.Unlock()
			return nil, err
		case QueryReadyOpenAbsentRebuildable:
			// Missing rebuildable assets are retried on the next call.
		}
		break
	}
	c.stats.OpenAttempts++
	c.state = QueryReadyOpenValidating
	c.stats.State = c.state
	c.mu.Unlock()

	started := time.Now()
	prepared, openStats, err := openQueryReadyPreparedGeneration(c.key, files)
	openStats.OpenTime = time.Since(started)

	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil {
		state := classifyQueryReadyGenerationOpenError(err)
		wrapped := &QueryReadyGenerationOpenError{State: state, Err: err}
		var boundErr *QueryReadyDeltaBoundError
		if errors.As(err, &boundErr) {
			// Bounds belong to the caller, not the physical generation. Keep
			// the cache retryable so a concurrent or later permissive request
			// can validate and publish the same otherwise valid assets.
			c.state, c.stats.State, c.lastErr = QueryReadyOpenAbsentRebuildable, QueryReadyOpenAbsentRebuildable, nil
			c.cond.Broadcast()
			return nil, wrapped
		}
		c.state, c.stats.State, c.lastErr = state, state, wrapped
		if state == QueryReadyOpenAbsentRebuildable {
			c.needsRebuild = true
		}
		c.cond.Broadcast()
		return nil, wrapped
	}
	if c.closed {
		_ = prepared.Close()
		wrapped := &QueryReadyGenerationOpenError{State: QueryReadyOpenUnsupportedOrStale, Err: errors.New("open cache closed during validation")}
		c.state, c.stats.State, c.lastErr = QueryReadyOpenUnsupportedOrStale, QueryReadyOpenUnsupportedOrStale, wrapped
		c.cond.Broadcast()
		return nil, wrapped
	}
	c.prepared = prepared
	c.bound = files.Bound
	c.state, c.stats.State = QueryReadyOpenReady, QueryReadyOpenReady
	c.stats.ColdOpens++
	c.stats.Published++
	if c.needsRebuild {
		c.stats.Rebuilds++
		c.needsRebuild = false
	}
	mergeQueryReadyGenerationOpenStats(&c.stats, openStats)
	c.cond.Broadcast()
	return prepared, nil
}

func (c *QueryReadyGenerationOpenCache) Stats() QueryReadyGenerationOpenStats {
	if c == nil {
		return QueryReadyGenerationOpenStats{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stats
}

func (c *QueryReadyGenerationOpenCache) Close() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	for c.state == QueryReadyOpenValidating {
		c.cond.Wait()
	}
	if c.closed {
		err := c.prepared
		c.mu.Unlock()
		if err != nil {
			return err.Close()
		}
		return nil
	}
	c.closed = true
	prepared := c.prepared
	c.prepared = nil
	c.state, c.stats.State = QueryReadyOpenUnsupportedOrStale, QueryReadyOpenUnsupportedOrStale
	c.mu.Unlock()
	if prepared != nil {
		return prepared.Close()
	}
	return nil
}

func openQueryReadyPreparedGeneration(key QueryReadyGenerationOpenKey, files QueryReadyGenerationOpenFiles) (*QueryReadyPreparedGeneration, QueryReadyGenerationOpenStats, error) {
	if err := validateQueryReadyGenerationOpenFiles(key, files); err != nil {
		return nil, QueryReadyGenerationOpenStats{}, err
	}
	var stats QueryReadyGenerationOpenStats
	validationStarted := time.Now()
	var base *QueryReadyBaseGeneration
	var consolidated *QueryReadyDeltaGeneration
	var err error
	switch files.Base.Kind {
	case QueryReadyGenerationBase:
		base, err = OpenQueryReadyBaseGenerationFileRange(files.Base.Path, files.Base.Offset, files.Base.Length, files.Base.Identity)
	case QueryReadyGenerationConsolidatedBase:
		consolidated, err = OpenQueryReadyConsolidatedBaseGenerationFileRange(files.Base.Path, files.Base.Offset, files.Base.Length, files.Base.Identity)
	default:
		err = fmt.Errorf("typedcolumn: unsupported query-ready base file kind %d", files.Base.Kind)
	}
	if err != nil {
		return nil, stats, fmt.Errorf("open base %q: %w", files.Base.Path, err)
	}
	closePartial := func(deltas []*QueryReadyDeltaGeneration) {
		for i := len(deltas) - 1; i >= 0; i-- {
			_ = deltas[i].Close()
		}
		if consolidated != nil {
			_ = consolidated.Close()
		}
		if base != nil {
			_ = base.Close()
		}
	}
	deltas := make([]*QueryReadyDeltaGeneration, 0, len(files.Deltas))
	for _, file := range files.Deltas {
		delta, openErr := OpenQueryReadyDeltaGenerationFileRange(file.Path, file.Offset, file.Length, file.Identity)
		if openErr != nil {
			closePartial(deltas)
			return nil, stats, fmt.Errorf("open delta %q: %w", file.Path, openErr)
		}
		deltas = append(deltas, delta)
	}
	baseView := base
	baseTombstones := []Tombstone(nil)
	baseOriginParts, baseAccumulatedDeltaParts := 0, 0
	if consolidated != nil {
		baseView = consolidated.Base
		baseTombstones = consolidated.Tombstones
		baseOriginParts = consolidated.OriginBaseParts
		baseAccumulatedDeltaParts = consolidated.AccumulatedDeltaParts
	}
	selected, err := validateAndSelectQueryReadyOpenDeltas(baseView, deltas, files.SnapshotGeneration)
	if err != nil {
		closePartial(deltas)
		return nil, stats, err
	}
	decision := evaluateQueryReadyBaseDeltaBound(baseView, baseTombstones, baseOriginParts, baseAccumulatedDeltaParts, selected, files.SnapshotGeneration, files.Bound)
	if decision.Triggered {
		closePartial(deltas)
		return nil, stats, &QueryReadyDeltaBoundError{Phase: "open", Decision: decision}
	}
	parts := make([]QueryReadyPreparedPartView, 0, decision.TotalParts)
	for _, part := range baseView.Parts {
		parts = append(parts, QueryReadyPreparedPartView{Role: PartRoleBase, Generation: part.Dependency.SourceGeneration, View: part})
	}
	tombstones := append([]Tombstone(nil), baseTombstones...)
	for _, delta := range selected {
		for _, part := range delta.Base.Parts {
			parts = append(parts, QueryReadyPreparedPartView{Role: PartRoleDelta, Generation: part.Dependency.SourceGeneration, View: part})
		}
		tombstones = append(tombstones, delta.Tombstones...)
	}
	execution, executionStats, err := prepareQueryReadyGenerationExecutionState(parts, tombstones)
	if err != nil {
		closePartial(deltas)
		return nil, stats, fmt.Errorf("prepare query-independent execution state: %w", err)
	}
	prepared := &QueryReadyPreparedGeneration{key: key, base: base, consolidated: consolidated, deltas: deltas, parts: parts, tombstones: tombstones, execution: execution}
	stats.PayloadBytesDecoded = executionStats.PayloadBytesDecoded
	stats.PayloadBytesCopied = executionStats.PayloadBytesCopied
	stats.PartsDecoded = executionStats.PartsDecoded
	stats.DomainsConstructed = executionStats.DomainsConstructed
	stats.RanksConstructed = executionStats.RanksConstructed
	stats.OffsetsConstructed = executionStats.OffsetsConstructed
	stats.WholePartDecodesDuringOpen = executionStats.WholePartDecodesDuringOpen
	stats.ValidationTime = time.Since(validationStarted)
	stats.MappedFiles = 1 + len(deltas)
	// Payload vectors remain encoded and mmap-backed. Open prepares only the
	// query-independent structural, visibility, and dictionary-domain state
	// shared by every M4 operator.
	accumulateQueryReadyOpenBaseStats(&stats, baseView)
	for _, delta := range deltas {
		stats.MappedBytes += delta.Stats.BytesMapped
		stats.LogicalImageBytes += int64(len(delta.Bytes()))
		stats.BytesValidated += delta.Stats.BytesValidated
		stats.StructuresValidated++
		accumulateQueryReadyOpenBaseStats(&stats, delta.Base)
	}
	if consolidated != nil {
		stats.MappedBytes += consolidated.Stats.BytesMapped
		stats.LogicalImageBytes += int64(len(consolidated.Bytes()))
		stats.BytesValidated += consolidated.Stats.BytesValidated
		stats.StructuresValidated++
	} else if base != nil {
		stats.MappedBytes += base.Stats.BytesMapped
		stats.LogicalImageBytes += int64(len(base.Bytes()))
	}
	return prepared, stats, nil
}

func accumulateQueryReadyOpenBaseStats(stats *QueryReadyGenerationOpenStats, base *QueryReadyBaseGeneration) {
	if stats == nil || base == nil {
		return
	}
	stats.StructuresValidated += 1 + len(base.Parts)
	stats.BytesValidated += base.Stats.BytesValidated
}

func validateQueryReadyPreparedGenerationBound(prepared *QueryReadyPreparedGeneration, snapshot uint64, bound QueryReadyDeltaBoundPolicy) error {
	if prepared == nil {
		return errors.New("typedcolumn: nil query-ready prepared generation")
	}
	base := prepared.base
	baseTombstones := []Tombstone(nil)
	baseOriginParts, baseAccumulatedDeltaParts := 0, 0
	if prepared.consolidated != nil {
		base = prepared.consolidated.Base
		baseTombstones = prepared.consolidated.Tombstones
		baseOriginParts = prepared.consolidated.OriginBaseParts
		baseAccumulatedDeltaParts = prepared.consolidated.AccumulatedDeltaParts
	}
	selected, err := validateAndSelectQueryReadyOpenDeltas(base, prepared.deltas, snapshot)
	if err != nil {
		return err
	}
	decision := evaluateQueryReadyBaseDeltaBound(base, baseTombstones, baseOriginParts, baseAccumulatedDeltaParts, selected, snapshot, bound)
	if decision.Triggered {
		return &QueryReadyDeltaBoundError{Phase: "open", Decision: decision}
	}
	return nil
}

func validateAndSelectQueryReadyOpenDeltas(base *QueryReadyBaseGeneration, deltas []*QueryReadyDeltaGeneration, snapshot uint64) ([]*QueryReadyDeltaGeneration, error) {
	if base == nil {
		return nil, errors.New("typedcolumn: nil query-ready open base")
	}
	if snapshot == 0 {
		return nil, errors.New("typedcolumn: query-ready open snapshot generation is zero")
	}
	if base.Identity.Generation > snapshot {
		return nil, fmt.Errorf("typedcolumn: query-ready open base generation=%d exceeds snapshot=%d", base.Identity.Generation, snapshot)
	}
	selected := make([]*QueryReadyDeltaGeneration, 0, len(deltas))
	seen := make(map[uint64]struct{}, len(deltas))
	for i, delta := range deltas {
		if delta == nil || delta.Kind != QueryReadyGenerationDelta || delta.Base == nil {
			return nil, fmt.Errorf("typedcolumn: invalid query-ready open delta at index %d", i)
		}
		if delta.Identity.SchemaHash != base.Identity.SchemaHash {
			return nil, fmt.Errorf("typedcolumn: query-ready open delta generation=%d schema mismatch", delta.Identity.Generation)
		}
		if delta.Identity.Generation <= base.Identity.Generation {
			return nil, fmt.Errorf("typedcolumn: query-ready open delta generation=%d not newer than base=%d", delta.Identity.Generation, base.Identity.Generation)
		}
		if _, ok := seen[delta.Identity.Generation]; ok {
			return nil, fmt.Errorf("typedcolumn: duplicate query-ready open delta generation=%d", delta.Identity.Generation)
		}
		seen[delta.Identity.Generation] = struct{}{}
		if delta.Identity.Generation <= snapshot {
			selected = append(selected, delta)
		}
	}
	sort.Slice(selected, func(i, j int) bool { return selected[i].Identity.Generation < selected[j].Identity.Generation })
	wantGeneration := base.Identity.Generation
	if len(selected) > 0 {
		wantGeneration = selected[len(selected)-1].Identity.Generation
	}
	if snapshot != wantGeneration {
		return nil, fmt.Errorf("typedcolumn: query-ready open snapshot generation=%d overclaims stale selected prefix ending at %d", snapshot, wantGeneration)
	}
	return selected, nil
}

func validateQueryReadyGenerationOpenFiles(key QueryReadyGenerationOpenKey, files QueryReadyGenerationOpenFiles) error {
	if err := validateQueryReadyBaseIdentity(key.Identity); err != nil {
		return err
	}
	if key.ManifestHash == ([sha256.Size]byte{}) {
		return errors.New("typedcolumn: query-ready manifest hash is zero")
	}
	if files.Key != key {
		return errors.New("typedcolumn: query-ready open manifest, schema, or generation is stale")
	}
	if files.SnapshotGeneration != key.Identity.Generation {
		return fmt.Errorf("typedcolumn: query-ready snapshot generation=%d want cache generation=%d", files.SnapshotGeneration, key.Identity.Generation)
	}
	if files.Base.Path == "" {
		return errors.New("typedcolumn: query-ready base path is absent")
	}
	if err := validateQueryReadyGenerationFileRangeDescriptor(files.Base); err != nil {
		return fmt.Errorf("typedcolumn: query-ready base: %w", err)
	}
	if files.Base.Identity.SchemaHash != key.Identity.SchemaHash || files.Base.Identity.Generation > key.Identity.Generation {
		return errors.New("typedcolumn: query-ready base schema or generation is stale")
	}
	type assetRange struct {
		path           string
		offset, length int64
	}
	seenRanges := map[assetRange]struct{}{{files.Base.Path, files.Base.Offset, files.Base.Length}: {}}
	for i, delta := range files.Deltas {
		if delta.Kind != QueryReadyGenerationDelta {
			return fmt.Errorf("typedcolumn: unsupported query-ready delta[%d] kind %d", i, delta.Kind)
		}
		if delta.Path == "" {
			return fmt.Errorf("typedcolumn: query-ready delta[%d] path is absent", i)
		}
		if err := validateQueryReadyGenerationFileRangeDescriptor(delta); err != nil {
			return fmt.Errorf("typedcolumn: query-ready delta[%d]: %w", i, err)
		}
		rangeKey := assetRange{delta.Path, delta.Offset, delta.Length}
		if _, exists := seenRanges[rangeKey]; exists {
			return fmt.Errorf("typedcolumn: query-ready duplicate asset range path=%q offset=%d length=%d", delta.Path, delta.Offset, delta.Length)
		}
		seenRanges[rangeKey] = struct{}{}
		if delta.Identity.SchemaHash != key.Identity.SchemaHash || delta.Identity.Generation > key.Identity.Generation {
			return fmt.Errorf("typedcolumn: query-ready delta[%d] schema or generation is stale", i)
		}
	}
	return nil
}

func validateQueryReadyGenerationFileRangeDescriptor(file QueryReadyGenerationFile) error {
	if file.Offset < 0 || file.Length < 0 || (file.Length == 0 && file.Offset != 0) {
		return fmt.Errorf("invalid asset range offset=%d length=%d", file.Offset, file.Length)
	}
	return nil
}

func classifyQueryReadyGenerationOpenError(err error) QueryReadyOpenState {
	if err == nil {
		return QueryReadyOpenReady
	}
	if errors.Is(err, os.ErrNotExist) || strings.Contains(err.Error(), "path is absent") {
		return QueryReadyOpenAbsentRebuildable
	}
	var boundErr *QueryReadyDeltaBoundError
	if errors.As(err, &boundErr) {
		return QueryReadyOpenUnsupportedOrStale
	}
	message := err.Error()
	if strings.Contains(message, "unsupported") || strings.Contains(message, "stale") || strings.Contains(message, "schema") || strings.Contains(message, "generation=") && strings.Contains(message, " want ") {
		return QueryReadyOpenUnsupportedOrStale
	}
	return QueryReadyOpenCorrupt
}

func mergeQueryReadyGenerationOpenStats(dst *QueryReadyGenerationOpenStats, src QueryReadyGenerationOpenStats) {
	dst.MappedFiles += src.MappedFiles
	dst.MappedBytes += src.MappedBytes
	dst.LogicalImageBytes += src.LogicalImageBytes
	dst.StructuresValidated += src.StructuresValidated
	dst.BytesValidated += src.BytesValidated
	dst.PayloadBytesDecoded += src.PayloadBytesDecoded
	dst.PayloadBytesCopied += src.PayloadBytesCopied
	dst.PartsDecoded += src.PartsDecoded
	dst.DomainsConstructed += src.DomainsConstructed
	dst.RanksConstructed += src.RanksConstructed
	dst.OffsetsConstructed += src.OffsetsConstructed
	dst.WholePartDecodesDuringOpen += src.WholePartDecodesDuringOpen
	dst.WholePartDecodesAfterOpen += src.WholePartDecodesAfterOpen
	dst.ValidationTime += src.ValidationTime
	dst.OpenTime += src.OpenTime
}
