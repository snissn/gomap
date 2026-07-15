package collections

import (
	"context"
	"errors"
	"math"
	"time"
)

const (
	TextIndexMaintenanceSkipReasonNoDebt           = "no_debt"
	TextIndexMaintenanceSkipReasonBelowThresholds  = "below_thresholds"
	TextIndexMaintenanceSkipReasonDryRun           = "dry_run"
	TextIndexMaintenanceSkipReasonMaxIndexes       = "max_indexes"
	TextIndexMaintenanceTriggerReasonForce         = "force"
	TextIndexMaintenanceTriggerReasonDeletedDocs   = "deleted_docs"
	TextIndexMaintenanceTriggerReasonMicroBlocks   = "micro_posting_blocks"
	TextIndexMaintenanceTriggerReasonDeltaBlocks   = "delta_posting_blocks"
	TextIndexMaintenanceTriggerReasonStalePostings = "stale_postings"
	TextIndexMaintenanceTriggerReasonRewriteBlocks = "rewrite_posting_blocks"
)

const textIndexMaintenancePPMScale = uint64(1_000_000)

// TextIndexMaintenancePolicy defines when explicit text-v2 maintenance should
// apply a logical rewrite. Zero-valued policies are normalized to conservative
// production defaults; set one or more fields to opt into a test/operator policy
// with only those thresholds enabled.
type TextIndexMaintenancePolicy struct {
	MinDeletedDocuments        uint64 `json:"min_deleted_documents,omitempty"`
	MinDeletedDocumentRatioPPM uint32 `json:"min_deleted_document_ratio_ppm,omitempty"`
	MinMicroPostingBlocks      uint64 `json:"min_micro_posting_blocks,omitempty"`
	MinDeltaPostingBlocks      uint64 `json:"min_delta_posting_blocks,omitempty"`
	MinStalePostings           uint64 `json:"min_stale_postings,omitempty"`
	MinStalePostingRatioPPM    uint32 `json:"min_stale_posting_ratio_ppm,omitempty"`
	MinRewritePostingBlocks    uint64 `json:"min_rewrite_posting_blocks,omitempty"`
}

// DefaultTextIndexMaintenancePolicy returns conservative text-v2 rewrite
// thresholds. They avoid surprise foreground work for small indexes while still
// giving operators a deterministic automatic policy for explicit maintenance.
func DefaultTextIndexMaintenancePolicy() TextIndexMaintenancePolicy {
	return TextIndexMaintenancePolicy{
		MinDeletedDocuments:        1024,
		MinDeletedDocumentRatioPPM: 100_000,
		MinMicroPostingBlocks:      256,
		MinDeltaPostingBlocks:      256,
		MinStalePostings:           4096,
		MinStalePostingRatioPPM:    100_000,
		MinRewritePostingBlocks:    512,
	}
}

// TextIndexMaintenanceOptions controls bounded explicit text-v2 maintenance.
// It only publishes normal ordered-root mutations. Physical reclamation remains
// in TreeDB's normal maintenance path and is run only when RunStorageCompaction
// is set.
type TextIndexMaintenanceOptions struct {
	Policy TextIndexMaintenancePolicy `json:"policy,omitempty"`

	TargetPostingsPerBlock uint32        `json:"target_postings_per_block,omitempty"`
	Force                  bool          `json:"force,omitempty"`
	DisableTombstonePurge  bool          `json:"disable_tombstone_purge,omitempty"`
	DryRun                 bool          `json:"dry_run,omitempty"`
	MaxTerms               uint64        `json:"max_terms,omitempty"`
	MaxPostingBlocks       uint64        `json:"max_posting_blocks,omitempty"`
	MaxPostings            uint64        `json:"max_postings,omitempty"`
	MaxDuration            time.Duration `json:"max_duration,omitempty"`
	MaxIndexes             int           `json:"max_indexes,omitempty"`

	RunStorageCompaction     bool                  `json:"run_storage_compaction,omitempty"`
	StorageCompactionOptions CompactStorageOptions `json:"storage_compaction_options,omitempty"`
}

// TextIndexMaintenanceDebt reports logical rewrite debt observed while planning
// text-v2 maintenance.
type TextIndexMaintenanceDebt struct {
	Documents               uint64 `json:"documents,omitempty"`
	LiveDocuments           uint64 `json:"live_documents,omitempty"`
	DeletedDocuments        uint64 `json:"deleted_documents,omitempty"`
	DeletedDocumentRatioPPM uint64 `json:"deleted_document_ratio_ppm,omitempty"`
	PostingBlocks           uint64 `json:"posting_blocks,omitempty"`
	SealedPostingBlocks     uint64 `json:"sealed_posting_blocks,omitempty"`
	MicroPostingBlocks      uint64 `json:"micro_posting_blocks,omitempty"`
	DeltaPostingBlocks      uint64 `json:"delta_posting_blocks,omitempty"`
	PostingsRead            uint64 `json:"postings_read,omitempty"`
	StalePostings           uint64 `json:"stale_postings,omitempty"`
	StalePostingRatioPPM    uint64 `json:"stale_posting_ratio_ppm,omitempty"`
	TermsScanned            uint64 `json:"terms_scanned,omitempty"`
	TermsRewritten          uint64 `json:"terms_rewritten,omitempty"`
	PostingBlocksRead       uint64 `json:"posting_blocks_read,omitempty"`
	PostingBlocksBefore     uint64 `json:"posting_blocks_before,omitempty"`
	PostingBlocksAfter      uint64 `json:"posting_blocks_after,omitempty"`
	PostingBlocksWritten    uint64 `json:"posting_blocks_written,omitempty"`
	PostingBlocksDeleted    uint64 `json:"posting_blocks_deleted,omitempty"`
	RewritePostingBlocks    uint64 `json:"rewrite_posting_blocks,omitempty"`
	TombstoneDocIDEntries   uint64 `json:"tombstone_docid_entries,omitempty"`
	TombstoneDocMapEntries  uint64 `json:"tombstone_docmap_entries,omitempty"`
	TombstoneNormEntries    uint64 `json:"tombstone_norm_entries,omitempty"`
	RewriteMergeState       string `json:"rewrite_merge_state,omitempty"`
	PhysicalReclamationPath string `json:"physical_reclamation_path,omitempty"`
}

// TextIndexMaintenanceIndexStats is the per-index operator/test report for one
// maintenance decision.
type TextIndexMaintenanceIndexStats struct {
	IndexName             string                   `json:"index_name"`
	Version               TextIndexVersion         `json:"version"`
	Triggered             bool                     `json:"triggered,omitempty"`
	TriggerReason         string                   `json:"trigger_reason,omitempty"`
	Applied               bool                     `json:"applied,omitempty"`
	Noop                  bool                     `json:"noop,omitempty"`
	DryRun                bool                     `json:"dry_run,omitempty"`
	SkippedReason         string                   `json:"skipped_reason,omitempty"`
	BudgetExhausted       bool                     `json:"budget_exhausted,omitempty"`
	BudgetExhaustedReason string                   `json:"budget_exhausted_reason,omitempty"`
	StorageBefore         TextIndexStorageStats    `json:"storage_before"`
	StorageAfter          TextIndexStorageStats    `json:"storage_after,omitempty"`
	Debt                  TextIndexMaintenanceDebt `json:"debt"`
	Rewrite               TextIndexRewriteStats    `json:"rewrite"`
}

// TextIndexMaintenanceStats reports one explicit maintenance run. The storage
// compaction fields are populated only when RunStorageCompaction is enabled.
type TextIndexMaintenanceStats struct {
	CollectionName          string                           `json:"collection_name,omitempty"`
	PhysicalReclamationPath string                           `json:"physical_reclamation_path,omitempty"`
	IndexesScanned          uint64                           `json:"indexes_scanned,omitempty"`
	IndexesTriggered        uint64                           `json:"indexes_triggered,omitempty"`
	IndexesRewritten        uint64                           `json:"indexes_rewritten,omitempty"`
	IndexesSkipped          uint64                           `json:"indexes_skipped,omitempty"`
	BudgetExhausted         bool                             `json:"budget_exhausted,omitempty"`
	BudgetExhaustedReason   string                           `json:"budget_exhausted_reason,omitempty"`
	StorageCompacted        bool                             `json:"storage_compacted,omitempty"`
	StorageCompaction       *CompactStorageStats             `json:"storage_compaction,omitempty"`
	Indexes                 []TextIndexMaintenanceIndexStats `json:"indexes,omitempty"`
}

// TextIndexMaintenanceManagerStats aggregates explicit policy maintenance
// across all collections known to a CollectionManager.
type TextIndexMaintenanceManagerStats struct {
	PhysicalReclamationPath string                      `json:"physical_reclamation_path,omitempty"`
	CollectionsScanned      uint64                      `json:"collections_scanned,omitempty"`
	IndexesScanned          uint64                      `json:"indexes_scanned,omitempty"`
	IndexesTriggered        uint64                      `json:"indexes_triggered,omitempty"`
	IndexesRewritten        uint64                      `json:"indexes_rewritten,omitempty"`
	IndexesSkipped          uint64                      `json:"indexes_skipped,omitempty"`
	BudgetExhausted         bool                        `json:"budget_exhausted,omitempty"`
	BudgetExhaustedReason   string                      `json:"budget_exhausted_reason,omitempty"`
	StorageCompacted        bool                        `json:"storage_compacted,omitempty"`
	StorageCompaction       *CompactStorageStats        `json:"storage_compaction,omitempty"`
	Collections             []TextIndexMaintenanceStats `json:"collections,omitempty"`
}

func validateTextIndexMaintenanceOptions(opts TextIndexMaintenanceOptions) error {
	if opts.MaxIndexes < 0 {
		return errors.New("collections: text index maintenance MaxIndexes must be non-negative")
	}
	if opts.MaxDuration < 0 {
		return errors.New("collections: text index maintenance MaxDuration must be non-negative")
	}
	return nil
}

// MaintainTextIndexes runs the bounded text-v2 rewrite policy across all
// collections known to the manager. It performs logical rewrites first and, when
// requested, runs one manager-level CompactStorage pass afterward.
func (m *CollectionManager) MaintainTextIndexes(ctx context.Context, opts TextIndexMaintenanceOptions) (TextIndexMaintenanceManagerStats, error) {
	var stats TextIndexMaintenanceManagerStats
	if m == nil {
		return stats, errCollectionManagerNil
	}
	if m.db == nil {
		return stats, errCollectionDBNil
	}
	if err := validateTextIndexMaintenanceOptions(opts); err != nil {
		return stats, err
	}
	stats.PhysicalReclamationPath = TextIndexPhysicalReclamationTreeDB
	metas, err := m.ListCollections()
	if err != nil {
		return stats, err
	}
	logicalOpts := opts
	logicalOpts.RunStorageCompaction = false
	for _, meta := range metas {
		if !collectionMetaHasTextV2MaintenanceIndexes(meta) {
			continue
		}
		if opts.MaxIndexes > 0 {
			remaining := opts.MaxIndexes - int(stats.IndexesScanned)
			if remaining <= 0 {
				stats.BudgetExhausted = true
				stats.BudgetExhaustedReason = TextIndexMaintenanceSkipReasonMaxIndexes
				stats.IndexesSkipped++
				break
			}
			logicalOpts.MaxIndexes = remaining
		}
		col, err := m.OpenCollection(meta.Name)
		if err != nil {
			return stats, err
		}
		collectionStats, err := col.MaintainTextIndexes(ctx, logicalOpts)
		if err != nil {
			return stats, err
		}
		stats.CollectionsScanned++
		stats.Collections = append(stats.Collections, collectionStats)
		stats.IndexesScanned += collectionStats.IndexesScanned
		stats.IndexesTriggered += collectionStats.IndexesTriggered
		stats.IndexesRewritten += collectionStats.IndexesRewritten
		stats.IndexesSkipped += collectionStats.IndexesSkipped
		if collectionStats.BudgetExhausted {
			stats.BudgetExhausted = true
			if stats.BudgetExhaustedReason == "" {
				stats.BudgetExhaustedReason = collectionStats.BudgetExhaustedReason
			}
			break
		}
	}
	if opts.RunStorageCompaction && stats.IndexesRewritten != 0 && !stats.BudgetExhausted && !opts.DryRun {
		compact, err := m.CompactStorage(ctx, opts.StorageCompactionOptions)
		if err != nil {
			return stats, err
		}
		stats.StorageCompacted = true
		stats.StorageCompaction = &compact
	}
	return stats, nil
}

func (c *Collection) MaintainTextIndex(ctx context.Context, indexName string, opts TextIndexMaintenanceOptions) (TextIndexMaintenanceStats, error) {
	var stats TextIndexMaintenanceStats
	if c == nil {
		return stats, errCollectionNil
	}
	if err := validateTextIndexMaintenanceOptions(opts); err != nil {
		return stats, err
	}
	stats.CollectionName = c.collectionName()
	stats.PhysicalReclamationPath = TextIndexPhysicalReclamationTreeDB
	idx, err := c.maintainTextIndexWithRetry(ctx, indexName, opts)
	if err != nil {
		return stats, err
	}
	stats.IndexesScanned = 1
	stats.Indexes = append(stats.Indexes, idx)
	accumulateTextIndexMaintenanceStats(&stats, idx)
	if opts.RunStorageCompaction && stats.IndexesRewritten != 0 && !stats.BudgetExhausted && !opts.DryRun {
		compact, err := c.CompactStorage(ctx, opts.StorageCompactionOptions)
		if err != nil {
			return stats, err
		}
		stats.StorageCompacted = true
		stats.StorageCompaction = &compact
	}
	return stats, nil
}

// MaintainTextIndexes runs the bounded text-v2 rewrite policy across this
// collection's text-v2 indexes. MaxIndexes limits how many indexes are planned.
func (c *Collection) MaintainTextIndexes(ctx context.Context, opts TextIndexMaintenanceOptions) (TextIndexMaintenanceStats, error) {
	var stats TextIndexMaintenanceStats
	if c == nil {
		return stats, errCollectionNil
	}
	if err := validateTextIndexMaintenanceOptions(opts); err != nil {
		return stats, err
	}
	stats.CollectionName = c.collectionName()
	stats.PhysicalReclamationPath = TextIndexPhysicalReclamationTreeDB
	meta := c.Meta()
	for _, def := range meta.TextIndexes {
		if !textIndexDefinitionIsTextV2MaintenanceCandidate(def) {
			continue
		}
		if opts.MaxIndexes > 0 && int(stats.IndexesScanned) >= opts.MaxIndexes {
			stats.BudgetExhausted = true
			stats.BudgetExhaustedReason = TextIndexMaintenanceSkipReasonMaxIndexes
			stats.IndexesSkipped++
			break
		}
		idx, err := c.maintainTextIndexWithRetry(ctx, def.Name, opts)
		if err != nil {
			return stats, err
		}
		stats.IndexesScanned++
		stats.Indexes = append(stats.Indexes, idx)
		accumulateTextIndexMaintenanceStats(&stats, idx)
		if stats.BudgetExhausted {
			break
		}
	}
	if opts.RunStorageCompaction && stats.IndexesRewritten != 0 && !stats.BudgetExhausted && !opts.DryRun {
		compact, err := c.CompactStorage(ctx, opts.StorageCompactionOptions)
		if err != nil {
			return stats, err
		}
		stats.StorageCompacted = true
		stats.StorageCompaction = &compact
	}
	return stats, nil
}

func collectionMetaHasTextV2MaintenanceIndexes(meta CollectionMeta) bool {
	for _, def := range meta.TextIndexes {
		if textIndexDefinitionIsTextV2MaintenanceCandidate(def) {
			return true
		}
	}
	return false
}

func textIndexDefinitionIsTextV2MaintenanceCandidate(def TextIndexDefinition) bool {
	version := def.Version
	if version == TextIndexVersionDefault {
		version = TextIndexVersionV2
	}
	return version == TextIndexVersionV2
}

func (c *Collection) maintainTextIndexWithRetry(ctx context.Context, indexName string, opts TextIndexMaintenanceOptions) (TextIndexMaintenanceIndexStats, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var lastErr error
	for attempt := 0; attempt < maxCollectionMutationRetries; attempt++ {
		stats, err := c.maintainTextIndex(ctx, indexName, opts)
		if !isRetriableCollectionMutationError(err) {
			return stats, err
		}
		lastErr = err
		if err := ctx.Err(); err != nil {
			return TextIndexMaintenanceIndexStats{}, err
		}
		waitBeforeCollectionMutationRetry(attempt)
	}
	return TextIndexMaintenanceIndexStats{}, collectionMutationRetryExhausted(lastErr)
}

func (c *Collection) maintainTextIndex(ctx context.Context, indexName string, opts TextIndexMaintenanceOptions) (TextIndexMaintenanceIndexStats, error) {
	policy := normalizeTextIndexMaintenancePolicy(opts.Policy)
	rewriteOpts := TextIndexRewriteOptions{
		TargetPostingsPerBlock: opts.TargetPostingsPerBlock,
		Force:                  opts.Force,
		DisableTombstonePurge:  opts.DisableTombstonePurge,
		MaxTerms:               opts.MaxTerms,
		MaxPostingBlocks:       opts.MaxPostingBlocks,
		MaxPostings:            opts.MaxPostings,
		MaxDuration:            opts.MaxDuration,
	}
	var plannedDebt TextIndexMaintenanceDebt
	var triggered bool
	var triggerReason string
	decide := func(before TextIndexStorageStats, plan TextIndexRewriteStats) textV2RewriteDecision {
		plannedDebt = textIndexMaintenanceDebtFrom(before, plan)
		triggered, triggerReason = textIndexMaintenanceShouldTrigger(policy, plannedDebt, opts.Force)
		if !triggered {
			return textV2RewriteDecision{SkippedReason: TextIndexMaintenanceSkipReasonBelowThresholds}
		}
		if opts.DryRun {
			return textV2RewriteDecision{SkippedReason: TextIndexMaintenanceSkipReasonDryRun}
		}
		return textV2RewriteDecision{Apply: true}
	}
	rewrite, before, skippedReason, err := c.rewriteTextIndexInternal(ctx, indexName, textV2RewriteRunOptions{
		Rewrite:          rewriteOpts,
		DryRun:           opts.DryRun,
		NeedStorageStats: true,
		Decide:           decide,
	})
	if err != nil {
		return TextIndexMaintenanceIndexStats{}, err
	}
	if plannedDebt.PhysicalReclamationPath == "" {
		plannedDebt = textIndexMaintenanceDebtFrom(before, rewrite)
		if opts.Force && !rewrite.BudgetExhausted && !rewrite.Noop {
			triggered = true
			triggerReason = TextIndexMaintenanceTriggerReasonForce
		}
	}
	idx := TextIndexMaintenanceIndexStats{
		IndexName:             indexName,
		Version:               TextIndexVersionV2,
		Triggered:             triggered,
		TriggerReason:         triggerReason,
		Noop:                  rewrite.Noop,
		DryRun:                opts.DryRun,
		SkippedReason:         skippedReason,
		BudgetExhausted:       rewrite.BudgetExhausted,
		BudgetExhaustedReason: rewrite.BudgetExhaustedReason,
		StorageBefore:         before,
		Debt:                  plannedDebt,
		Rewrite:               rewrite,
	}
	// Budget exhaustion is stronger than a policy skip reason because bounded
	// maintenance stopped before it could publish a complete rewrite.
	if rewrite.BudgetExhausted {
		idx.SkippedReason = rewrite.BudgetExhaustedReason
	}
	idx.Applied = !rewrite.Noop && !opts.DryRun && !rewrite.BudgetExhausted
	if idx.Applied {
		after, err := c.TextIndexStorageStats(indexName)
		if err != nil {
			return idx, err
		}
		idx.StorageAfter = after
	}
	// The no-debt fallback is intentionally last so policy skips and budget
	// exhaustion keep their more specific reasons.
	if idx.SkippedReason == "" && !idx.Applied {
		idx.SkippedReason = TextIndexMaintenanceSkipReasonNoDebt
	}
	return idx, nil
}

func normalizeTextIndexMaintenancePolicy(policy TextIndexMaintenancePolicy) TextIndexMaintenancePolicy {
	if policy == (TextIndexMaintenancePolicy{}) {
		return DefaultTextIndexMaintenancePolicy()
	}
	return policy
}

func textIndexMaintenanceDebtFrom(storage TextIndexStorageStats, rewrite TextIndexRewriteStats) TextIndexMaintenanceDebt {
	deletedRatio := textIndexMaintenanceRatioPPM(storage.V2DeletedDocs, storage.V2LiveDocuments+storage.V2DeletedDocs)
	staleRatio := textIndexMaintenanceRatioPPM(rewrite.StalePostingsPurged, rewrite.PostingsRead)
	return TextIndexMaintenanceDebt{
		Documents:               storage.Documents,
		LiveDocuments:           storage.V2LiveDocuments,
		DeletedDocuments:        storage.V2DeletedDocs,
		DeletedDocumentRatioPPM: deletedRatio,
		PostingBlocks:           storage.V2PostingBlocks,
		SealedPostingBlocks:     storage.V2SealedPostingBlocks,
		MicroPostingBlocks:      storage.V2MicroPostingBlocks,
		DeltaPostingBlocks:      storage.V2DeltaPostingBlocks,
		PostingsRead:            rewrite.PostingsRead,
		StalePostings:           rewrite.StalePostingsPurged,
		StalePostingRatioPPM:    staleRatio,
		TermsScanned:            rewrite.TermsScanned,
		TermsRewritten:          rewrite.TermsRewritten,
		PostingBlocksRead:       rewrite.PostingBlocksRead,
		PostingBlocksBefore:     rewrite.PostingBlocksBefore,
		PostingBlocksAfter:      rewrite.PostingBlocksAfter,
		PostingBlocksWritten:    rewrite.PostingBlocksWritten,
		PostingBlocksDeleted:    rewrite.PostingBlocksDeleted,
		RewritePostingBlocks:    rewrite.PostingBlocksWritten + rewrite.PostingBlocksDeleted,
		TombstoneDocIDEntries:   rewrite.TombstoneDocIDEntriesPurged,
		TombstoneDocMapEntries:  rewrite.TombstoneDocMapEntriesPurged,
		TombstoneNormEntries:    rewrite.TombstoneNormEntriesPurged,
		RewriteMergeState:       storage.V2RewriteMergeState,
		PhysicalReclamationPath: TextIndexPhysicalReclamationTreeDB,
	}
}

func textIndexMaintenanceShouldTrigger(policy TextIndexMaintenancePolicy, debt TextIndexMaintenanceDebt, force bool) (bool, string) {
	if force {
		return true, TextIndexMaintenanceTriggerReasonForce
	}
	if policy.MinDeletedDocuments > 0 && debt.DeletedDocuments >= policy.MinDeletedDocuments {
		return true, TextIndexMaintenanceTriggerReasonDeletedDocs
	}
	if policy.MinDeletedDocumentRatioPPM > 0 && debt.DeletedDocumentRatioPPM >= uint64(policy.MinDeletedDocumentRatioPPM) {
		return true, TextIndexMaintenanceTriggerReasonDeletedDocs
	}
	if policy.MinMicroPostingBlocks > 0 && debt.MicroPostingBlocks >= policy.MinMicroPostingBlocks {
		return true, TextIndexMaintenanceTriggerReasonMicroBlocks
	}
	if policy.MinDeltaPostingBlocks > 0 && debt.DeltaPostingBlocks >= policy.MinDeltaPostingBlocks {
		return true, TextIndexMaintenanceTriggerReasonDeltaBlocks
	}
	if policy.MinStalePostings > 0 && debt.StalePostings >= policy.MinStalePostings {
		return true, TextIndexMaintenanceTriggerReasonStalePostings
	}
	if policy.MinStalePostingRatioPPM > 0 && debt.StalePostingRatioPPM >= uint64(policy.MinStalePostingRatioPPM) {
		return true, TextIndexMaintenanceTriggerReasonStalePostings
	}
	if policy.MinRewritePostingBlocks > 0 && debt.RewritePostingBlocks >= policy.MinRewritePostingBlocks {
		return true, TextIndexMaintenanceTriggerReasonRewriteBlocks
	}
	return false, ""
}

func textIndexMaintenanceRatioPPM(numerator, denominator uint64) uint64 {
	if numerator == 0 || denominator == 0 {
		return 0
	}
	if numerator > math.MaxUint64/textIndexMaintenancePPMScale {
		return math.MaxUint64
	}
	return numerator * textIndexMaintenancePPMScale / denominator
}

func accumulateTextIndexMaintenanceStats(stats *TextIndexMaintenanceStats, idx TextIndexMaintenanceIndexStats) {
	if stats == nil {
		return
	}
	if idx.Triggered {
		stats.IndexesTriggered++
	}
	if idx.Applied {
		stats.IndexesRewritten++
	}
	if !idx.Applied {
		stats.IndexesSkipped++
	}
	if idx.BudgetExhausted {
		stats.BudgetExhausted = true
		if stats.BudgetExhaustedReason == "" {
			stats.BudgetExhaustedReason = idx.BudgetExhaustedReason
		}
	}
}
