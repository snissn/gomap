package collections

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/tree"
)

const (
	TextIndexRewriteMergeStateReady     = "ready"
	TextIndexRewriteMergeStatePending   = "rewrite_merge_pending"
	TextIndexRewriteMergeStateCompacted = "compacted"

	TextIndexRewriteBudgetReasonMaxTerms         = "max_terms"
	TextIndexRewriteBudgetReasonMaxPostingBlocks = "max_posting_blocks"
	TextIndexRewriteBudgetReasonMaxPostings      = "max_postings"
	TextIndexRewriteBudgetReasonMaxDuration      = "max_duration"
)

var errTextV2RewriteBudgetExhausted = errors.New("collections: text-v2 rewrite budget exhausted")

// TextIndexRewriteOptions controls logical text-v2 rewrite/merge maintenance.
// The rewrite is a normal ordered-root mutation: it publishes replacement
// posting-block, term-stat, tombstone, and status records through collection
// roots. Physical reclamation remains owned by TreeDB ValueLogGC,
// value-log rewrite, leaf generation maintenance, index vacuum, and
// CompactStorage.
type TextIndexRewriteOptions struct {
	// TargetPostingsPerBlock controls the sealed block size produced by the
	// rewrite. Zero uses the text-v2 production default.
	TargetPostingsPerBlock uint32
	// Force rewrites every term even if no stale/micro/delta blocks are observed.
	Force bool
	// DisableTombstonePurge keeps deleted docID/docmap/norm tombstones after
	// stale posting blocks are removed. The default purges tombstones.
	DisableTombstonePurge bool
	// MaxTerms bounds how many posting terms may be planned. Zero is unlimited.
	// If the bound is exhausted, no rewrite is published and stats report the
	// exhausted budget reason.
	MaxTerms uint64
	// MaxPostingBlocks bounds posting blocks read while planning. Zero is
	// unlimited. The bound is checked at term boundaries so one term is the
	// accounting unit.
	MaxPostingBlocks uint64
	// MaxPostings bounds posting entries read while planning. Zero is unlimited.
	// The bound is checked at term boundaries so one term is the accounting unit.
	MaxPostings uint64
	// MaxDuration bounds wall-clock planning time. Zero is unlimited. The bound is
	// checked during storage-stat inspection, term-stat planning, posting-block
	// scanning, and term rewrites; exhaustion leaves storage unchanged.
	MaxDuration time.Duration
}

// TextIndexRewriteStats reports logical text-v2 rewrite/merge maintenance work.
type TextIndexRewriteStats struct {
	IndexName string           `json:"index_name"`
	Version   TextIndexVersion `json:"version"`

	RootGenerationBefore uint64 `json:"root_generation_before"`
	RootGenerationAfter  uint64 `json:"root_generation_after"`
	Noop                 bool   `json:"noop"`

	TermsScanned   uint64 `json:"terms_scanned"`
	TermsRewritten uint64 `json:"terms_rewritten"`
	TermsPurged    uint64 `json:"terms_purged"`

	PostingBlocksRead    uint64 `json:"posting_blocks_read"`
	PostingBlocksWritten uint64 `json:"posting_blocks_written"`
	PostingBlocksDeleted uint64 `json:"posting_blocks_deleted"`
	PostingsRead         uint64 `json:"postings_read"`
	LivePostingsRetained uint64 `json:"live_postings_retained"`
	StalePostingsPurged  uint64 `json:"stale_postings_purged"`

	TombstoneDocIDEntriesPurged  uint64 `json:"tombstone_docid_entries_purged"`
	TombstoneDocMapEntriesPurged uint64 `json:"tombstone_docmap_entries_purged"`
	TombstoneNormEntriesPurged   uint64 `json:"tombstone_norm_entries_purged"`

	PostingBlocksBefore uint64 `json:"posting_blocks_before"`
	PostingBlocksAfter  uint64 `json:"posting_blocks_after"`

	BudgetExhausted       bool   `json:"budget_exhausted,omitempty"`
	BudgetExhaustedReason string `json:"budget_exhausted_reason,omitempty"`
}

type textV2PostingRewriteTerm struct {
	term      string
	oldKeys   [][]byte
	entries   []textV2PostingBlockEntry
	liveByOrd map[uint64]textV2PostingBlockEntry
	postings  uint64
	stale     uint64
	oldBlocks uint64
	nonSealed bool
}

type textV2RewritePlan struct {
	stats TextIndexRewriteStats

	postingBlocksTable memtable.Table
	termsTable         memtable.Table
	docIDTable         memtable.Table
	docMapTable        memtable.Table
	normTable          memtable.Table
	generationsTable   memtable.Table

	postingChanged bool
	termsChanged   bool
	tombChanged    bool
	statusChanged  bool

	nextStatus textV2IndexStatusValue
}

type textV2RewriteBudget struct {
	ctx              context.Context
	started          time.Time
	enabled          bool
	maxTerms         uint64
	maxPostingBlocks uint64
	maxPostings      uint64
	maxDuration      time.Duration

	terms         uint64
	postingBlocks uint64
	postings      uint64
	reason        string
}

type textV2RewriteDecision struct {
	Apply         bool
	SkippedReason string
}

type textV2RewriteRunOptions struct {
	Rewrite          TextIndexRewriteOptions
	DryRun           bool
	NeedStorageStats bool
	Decide           func(before TextIndexStorageStats, plan TextIndexRewriteStats) textV2RewriteDecision
}

func newTextV2RewriteBudget(ctx context.Context, opts TextIndexRewriteOptions) *textV2RewriteBudget {
	if ctx == nil {
		ctx = context.Background()
	}
	budget := &textV2RewriteBudget{
		ctx:              ctx,
		enabled:          ctx.Done() != nil || opts.MaxTerms != 0 || opts.MaxPostingBlocks != 0 || opts.MaxPostings != 0 || opts.MaxDuration != 0,
		maxTerms:         opts.MaxTerms,
		maxPostingBlocks: opts.MaxPostingBlocks,
		maxPostings:      opts.MaxPostings,
		maxDuration:      opts.MaxDuration,
	}
	if opts.MaxDuration > 0 {
		budget.started = time.Now()
	}
	return budget
}

func (b *textV2RewriteBudget) check() error {
	if b == nil || !b.enabled {
		return nil
	}
	if b.ctx != nil {
		select {
		case <-b.ctx.Done():
			return b.ctx.Err()
		default:
		}
	}
	if b.maxDuration > 0 && !b.started.IsZero() && time.Since(b.started) >= b.maxDuration {
		b.exhaust(TextIndexRewriteBudgetReasonMaxDuration)
		return errTextV2RewriteBudgetExhausted
	}
	return nil
}

func (b *textV2RewriteBudget) checkTermCount(next uint64) error {
	if err := b.check(); err != nil {
		return err
	}
	if b == nil || !b.enabled {
		return nil
	}
	if b.maxTerms > 0 && next > b.maxTerms {
		b.exhaust(TextIndexRewriteBudgetReasonMaxTerms)
		return errTextV2RewriteBudgetExhausted
	}
	return nil
}

func (b *textV2RewriteBudget) reserveTerm() error {
	if err := b.check(); err != nil {
		return err
	}
	if b == nil || !b.enabled {
		return nil
	}
	if b.maxTerms > 0 && b.terms >= b.maxTerms {
		b.exhaust(TextIndexRewriteBudgetReasonMaxTerms)
		return errTextV2RewriteBudgetExhausted
	}
	b.terms++
	return nil
}

func (b *textV2RewriteBudget) reservePostingBlock() error {
	if err := b.check(); err != nil {
		return err
	}
	if b == nil || !b.enabled {
		return nil
	}
	if b.maxPostingBlocks > 0 && b.postingBlocks >= b.maxPostingBlocks {
		b.exhaust(TextIndexRewriteBudgetReasonMaxPostingBlocks)
		return errTextV2RewriteBudgetExhausted
	}
	b.postingBlocks++
	return nil
}

func (b *textV2RewriteBudget) reservePostings(count uint64) error {
	if err := b.check(); err != nil {
		return err
	}
	if b == nil || !b.enabled {
		return nil
	}
	if b.maxPostings > 0 && (b.postings > b.maxPostings || b.maxPostings-b.postings < count) {
		b.exhaust(TextIndexRewriteBudgetReasonMaxPostings)
		return errTextV2RewriteBudgetExhausted
	}
	b.postings += count
	return nil
}

func (b *textV2RewriteBudget) exhaust(reason string) {
	if b != nil && b.reason == "" {
		b.reason = reason
	}
}

// RewriteTextIndex performs text-v2 logical rewrite/merge maintenance for one
// explicit v2 text index. It coalesces micro/delta blocks into sealed blocks,
// removes stale generations and deleted-document postings, optionally purges
// tombstoned docID/docmap/norm entries, and updates term block counts/status via
// normal TreeDB collection-root publication. It does not run a private physical
// GC; callers can run ValueLogGC, value-log rewrite, or CompactStorage after old
// snapshots release to reclaim obsolete pointer-backed payloads.
func (c *Collection) RewriteTextIndex(indexName string, opts TextIndexRewriteOptions) (TextIndexRewriteStats, error) {
	stats, _, _, err := c.rewriteTextIndexInternal(context.Background(), indexName, textV2RewriteRunOptions{Rewrite: opts})
	return stats, err
}

func validateTextIndexRewriteOptions(opts TextIndexRewriteOptions) error {
	if opts.MaxDuration < 0 {
		return errors.New("collections: text-v2 rewrite MaxDuration must be non-negative")
	}
	return nil
}

func textV2BudgetExhaustedRewriteStats(indexName string, before TextIndexStorageStats, reason string) TextIndexRewriteStats {
	return TextIndexRewriteStats{
		IndexName:             indexName,
		Version:               TextIndexVersionV2,
		RootGenerationBefore:  before.V2RootGeneration,
		RootGenerationAfter:   before.V2RootGeneration,
		Noop:                  true,
		BudgetExhausted:       true,
		BudgetExhaustedReason: reason,
	}
}

func (c *Collection) rewriteTextIndexInternal(ctx context.Context, indexName string, run textV2RewriteRunOptions) (TextIndexRewriteStats, TextIndexStorageStats, string, error) {
	var empty TextIndexRewriteStats
	var emptyStorage TextIndexStorageStats
	if ctx == nil {
		ctx = context.Background()
	}
	opts := run.Rewrite
	if err := validateTextIndexRewriteOptions(opts); err != nil {
		return empty, emptyStorage, "", err
	}
	if err := ValidateIndexName(indexName); err != nil {
		return empty, emptyStorage, "", err
	}
	if c == nil {
		return empty, emptyStorage, "", errCollectionNil
	}
	if c.db == nil {
		return empty, emptyStorage, "", errCollectionDBNil
	}
	if c.db.CommandWALEnabled() {
		return empty, emptyStorage, "", fmt.Errorf("%w: text-v2 rewrite is rejected under command_wal_v2 until collection text maintenance commands are supported", backenddb.ErrCommandWALRejected)
	}
	if err := ctx.Err(); err != nil {
		return empty, emptyStorage, "", err
	}
	if err := c.ensureWriteDomainOpen(); err != nil {
		return empty, emptyStorage, "", err
	}
	runCollectionSchemaMutationBeforeLockHookForTest()
	unlockSchema := c.lockCollectionSchemaWrite()
	defer unlockSchema()
	if err := ctx.Err(); err != nil {
		return empty, emptyStorage, "", err
	}
	if !run.DryRun {
		if err := c.flushCollectionWriteDomainsForSchemaMutation(); err != nil {
			return empty, emptyStorage, "", err
		}
	}

	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return empty, emptyStorage, "", backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalog(snap, c.meta.Name)
	if err != nil {
		return empty, emptyStorage, "", err
	}
	if catalog == nil {
		return empty, emptyStorage, "", errCollectionNotFound
	}
	if err := rejectCatalogRootOverlaysForWrite(catalog); err != nil {
		return empty, emptyStorage, "", err
	}
	baseMeta := catalog.meta
	c.meta = baseMeta
	def, ok := findTextIndex(baseMeta.TextIndexes, indexName)
	if !ok {
		return empty, emptyStorage, "", ErrIndexNotFound
	}
	if def.Version != TextIndexVersionV2 {
		return empty, emptyStorage, "", fmt.Errorf("%w: text index %q is %q; rewrite/merge is only available for explicit text index v2", ErrTextIndexUnavailable, indexName, def.Version)
	}

	budget := newTextV2RewriteBudget(ctx, opts)
	var beforeStats TextIndexStorageStats
	if run.NeedStorageStats || run.Decide != nil {
		beforeStats, err = inspectTextV2IndexStorageWithBudget(snap, catalog, def, budget)
		if err != nil {
			if errors.Is(err, errTextV2RewriteBudgetExhausted) {
				return textV2BudgetExhaustedRewriteStats(indexName, beforeStats, budget.reason), beforeStats, budget.reason, nil
			}
			return empty, emptyStorage, "", err
		}
	}

	plan, err := buildTextV2RewritePlan(ctx, snap, catalog, def, opts, budget)
	if err != nil {
		return empty, emptyStorage, "", err
	}
	defer resetTextV2RewritePlanTables(plan)
	if plan == nil || plan.stats.BudgetExhausted || (!plan.postingChanged && !plan.termsChanged && !plan.tombChanged && !plan.statusChanged) {
		stats := TextIndexRewriteStats{IndexName: indexName, Version: TextIndexVersionV2, Noop: true}
		reason := "no_debt"
		if plan != nil {
			stats = plan.stats
			stats.Noop = true
			if stats.BudgetExhausted {
				reason = stats.BudgetExhaustedReason
			}
		}
		return stats, beforeStats, reason, nil
	}
	if run.Decide != nil {
		decision := run.Decide(beforeStats, plan.stats)
		if !decision.Apply {
			stats := plan.stats
			stats.Noop = true
			return stats, beforeStats, decision.SkippedReason, nil
		}
	}
	if run.DryRun {
		stats := plan.stats
		stats.Noop = true
		return stats, beforeStats, "dry_run", nil
	}
	if err := budget.check(); err != nil {
		if errors.Is(err, errTextV2RewriteBudgetExhausted) {
			return textV2BudgetExhaustedRewriteStats(indexName, beforeStats, budget.reason), beforeStats, budget.reason, nil
		}
		return empty, emptyStorage, "", err
	}

	rootNames := make([]string, 0, 6)
	baseRootIDs := make(map[string]uint64, 6)
	policies := make([]backenddb.OrderedRootStoragePolicy, 0, 6)
	tables := make([]memtable.Table, 0, 6)
	appendRoot := func(rootName string, table memtable.Table) error {
		if table == nil || table.Len() == 0 {
			return nil
		}
		table.Freeze()
		policy, err := collectionRootStoragePolicyForDB(c.db, catalog.meta, rootName)
		if err != nil {
			return err
		}
		rootNames = append(rootNames, rootName)
		baseRootIDs[rootName] = catalog.rootID(rootName)
		policies = append(policies, policy)
		tables = append(tables, table)
		return nil
	}

	if err := appendRoot(collectionTextV2PostingBlocksRootName(catalog.meta.Name, def.Name), plan.postingBlocksTable); err != nil {
		return empty, emptyStorage, "", err
	}
	if err := appendRoot(collectionTextV2TermsRootName(catalog.meta.Name, def.Name), plan.termsTable); err != nil {
		return empty, emptyStorage, "", err
	}
	if err := appendRoot(collectionTextV2DocIDRootName(catalog.meta.Name, def.Name), plan.docIDTable); err != nil {
		return empty, emptyStorage, "", err
	}
	if err := appendRoot(collectionTextV2DocMapRootName(catalog.meta.Name, def.Name), plan.docMapTable); err != nil {
		return empty, emptyStorage, "", err
	}
	if err := appendRoot(collectionTextV2NormBlocksRootName(catalog.meta.Name, def.Name), plan.normTable); err != nil {
		return empty, emptyStorage, "", err
	}
	if err := appendRoot(collectionTextV2GenerationsRootName(catalog.meta.Name, def.Name), plan.generationsTable); err != nil {
		return empty, emptyStorage, "", err
	}
	if len(rootNames) == 0 {
		stats := plan.stats
		stats.Noop = true
		return stats, beforeStats, "no_delta_roots", nil
	}

	ordered := make([]backenddb.OrderedRootDeltaPublishInput, 0, len(rootNames))
	iterators := make([]iterator.UnsafeIterator, 0, len(rootNames))
	defer func() {
		for _, it := range iterators {
			_ = it.Close()
		}
	}()
	for i, rootName := range rootNames {
		iter := tables[i].NewIterator(nil, nil)
		iterators = append(iterators, iter)
		ordered = append(ordered, backenddb.OrderedRootDeltaPublishInput{BaseRoot: baseRootIDs[rootName], Iter: iter, StoragePolicy: policies[i]})
	}
	baseCommitSeq := snapshotCommitSeq(snap)
	baseSystemRoot := snapshotSystemRoot(snap)
	newSystemRoot, rootIDs, err := c.db.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder(ordered, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return c.buildRootDescriptorSystemDeltaIterator(baseCommitSeq, baseSystemRoot, rootNames, baseRootIDs, rootIDs)
	})
	if err != nil {
		return empty, emptyStorage, "", err
	}
	if len(rootIDs) != len(rootNames) {
		return empty, emptyStorage, "", unexpectedOrderedRootCountError(catalog.meta.Name, len(rootNames), len(rootIDs))
	}
	nextCatalog := cloneCatalogWithRootUpdates(catalog, catalog.meta, rootNames, rootIDs)
	c.rememberCatalogAtSystemRoot(newSystemRoot, nextCatalog)
	c.noteWriteDomainCatalog(newSystemRoot, nextCatalog)
	stats := plan.stats
	stats.RootGenerationAfter = plan.nextStatus.RootGeneration
	return stats, beforeStats, "", nil
}

func buildTextV2RewritePlan(ctx context.Context, snap *backenddb.Snapshot, catalog *collectionCatalog, def TextIndexDefinition, opts TextIndexRewriteOptions, budget *textV2RewriteBudget) (*textV2RewritePlan, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if snap == nil {
		return nil, backenddb.ErrClosed
	}
	if catalog == nil {
		return nil, errCollectionNotFound
	}
	generationsRootName := collectionTextV2GenerationsRootName(catalog.meta.Name, def.Name)
	status, ok, err := readTextV2StatusAtRoot(snap, catalog, generationsRootName)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errMalformedTextStorage("missing text-v2 status for collection %q index %q", catalog.meta.Name, def.Name)
	}
	target := opts.TargetPostingsPerBlock
	if target == 0 {
		target = textV2PostingBlockTargetPostings
	}
	if target > textV2PostingBlockMaxPostings {
		target = textV2PostingBlockMaxPostings
	}
	if target == 0 {
		target = textV2PostingBlockTargetPostings
	}

	plan := &textV2RewritePlan{
		postingBlocksTable: newCollectionRunTable(0),
		termsTable:         newCollectionRunTable(0),
		docIDTable:         newCollectionRunTable(0),
		docMapTable:        newCollectionRunTable(0),
		normTable:          newCollectionRunTable(0),
		generationsTable:   newCollectionRunTable(1),
		nextStatus:         status,
	}
	plan.stats.IndexName = def.Name
	plan.stats.Version = TextIndexVersionV2
	plan.stats.RootGenerationBefore = status.RootGeneration
	plan.stats.RootGenerationAfter = status.RootGeneration
	if budget == nil {
		budget = newTextV2RewriteBudget(ctx, opts)
	}
	markBudgetExhausted := func() (*textV2RewritePlan, error) {
		plan.stats.BudgetExhausted = true
		plan.stats.BudgetExhaustedReason = budget.reason
		return plan, nil
	}
	if err := budget.check(); err != nil {
		if errors.Is(err, errTextV2RewriteBudgetExhausted) {
			return markBudgetExhausted()
		}
		resetTextV2RewritePlanTables(plan)
		return nil, err
	}
	termsRootName := collectionTextV2TermsRootName(catalog.meta.Name, def.Name)
	termStats, err := readTextV2RewriteTermStats(snap, catalog, termsRootName, budget)
	if err != nil {
		if errors.Is(err, errTextV2RewriteBudgetExhausted) {
			return markBudgetExhausted()
		}
		resetTextV2RewritePlanTables(plan)
		return nil, err
	}

	processedTerms := make(map[string]struct{}, len(termStats))
	postingRootName := collectionTextV2PostingBlocksRootName(catalog.meta.Name, def.Name)
	searchCtx, err := newTextV2SearchContext(snap, catalog, def, nil)
	if err != nil {
		resetTextV2RewritePlanTables(plan)
		return nil, err
	}
	cache := textV2SearchBlockCache{}
	scanErr := scanTextV2PostingRewriteTerms(snap, catalog, postingRootName, budget, func(term *textV2PostingRewriteTerm) error {
		processedTerms[term.term] = struct{}{}
		if err := plan.processRewriteTerm(snap, catalog, def, searchCtx, status, &cache, term, termStats[term.term], target, opts.Force, budget.check); err != nil {
			return err
		}
		return budget.check()
	})
	if scanErr != nil {
		if errors.Is(scanErr, errTextV2RewriteBudgetExhausted) {
			return markBudgetExhausted()
		}
		resetTextV2RewritePlanTables(plan)
		return nil, scanErr
	}

	terms := make([]string, 0, len(termStats))
	for term := range termStats {
		if _, ok := processedTerms[term]; !ok {
			terms = append(terms, term)
		}
	}
	sort.Strings(terms)
	for _, term := range terms {
		if err := budget.check(); err != nil {
			if errors.Is(err, errTextV2RewriteBudgetExhausted) {
				return markBudgetExhausted()
			}
			resetTextV2RewritePlanTables(plan)
			return nil, err
		}
		stats := termStats[term]
		plan.stats.TermsScanned++
		if stats.DocumentFrequency != 0 || stats.TotalTermFrequency != 0 {
			return nil, errMalformedTextStorage("text-v2 term %q has live stats but no posting blocks", term)
		}
		if stats.PostingBlockCount != 0 {
			plan.termsTable.DeleteSteal(encodeTextV2TermStatsKey(term))
			plan.termsChanged = true
			plan.stats.TermsPurged++
		}
	}

	if !opts.DisableTombstonePurge && status.DeletedDocuments != 0 {
		purged, err := plan.purgeTextV2Tombstones(snap, catalog, def, status, budget.check)
		if err != nil {
			if errors.Is(err, errTextV2RewriteBudgetExhausted) {
				return markBudgetExhausted()
			}
			resetTextV2RewritePlanTables(plan)
			return nil, err
		}
		if purged {
			plan.tombChanged = true
		}
	}

	if plan.postingChanged || plan.termsChanged || plan.tombChanged {
		nextGeneration := status.RootGeneration + 1
		if nextGeneration == 0 {
			resetTextV2RewritePlanTables(plan)
			return nil, errMalformedTextStorage("text-v2 root generation overflow")
		}
		plan.nextStatus.RootGeneration = nextGeneration
		if plan.postingChanged || plan.termsChanged {
			plan.nextStatus.TermGeneration = nextGeneration
		}
		if plan.tombChanged {
			plan.nextStatus.DocMapGeneration = nextGeneration
			plan.nextStatus.NormGeneration = nextGeneration
			plan.nextStatus.DeletedDocuments = 0
		}
		plan.generationsTable.SetSteal(encodeTextV2StatusKey(), encodeTextV2IndexStatusValue(plan.nextStatus))
		plan.statusChanged = true
		plan.stats.RootGenerationAfter = nextGeneration
	}
	return plan, nil
}

func (p *textV2RewritePlan) processRewriteTerm(
	snap *backenddb.Snapshot,
	catalog *collectionCatalog,
	def TextIndexDefinition,
	ctx *textV2SearchContext,
	status textV2IndexStatusValue,
	cache *textV2SearchBlockCache,
	term *textV2PostingRewriteTerm,
	termStats textV2TermStatsValue,
	target uint32,
	force bool,
	checkBudget func() error,
) error {
	if p == nil || term == nil {
		return nil
	}
	if checkBudget != nil {
		if err := checkBudget(); err != nil {
			return err
		}
	}
	p.stats.TermsScanned++
	p.stats.PostingBlocksRead += term.oldBlocks
	p.stats.PostingBlocksBefore += term.oldBlocks
	p.stats.PostingsRead += term.postings

	if err := term.filterCurrentEntries(snap, catalog, ctx, cache, checkBudget); err != nil {
		return err
	}
	p.stats.StalePostingsPurged += term.stale
	live := make([]textV2PostingBlockEntry, 0, len(term.liveByOrd))
	for _, entry := range term.liveByOrd {
		if checkBudget != nil {
			if err := checkBudget(); err != nil {
				return err
			}
		}
		live = append(live, entry)
	}
	sort.Slice(live, func(i, j int) bool { return live[i].Ordinal < live[j].Ordinal })
	p.stats.LivePostingsRetained += uint64(len(live))

	var liveTF uint64
	for _, entry := range live {
		if checkBudget != nil {
			if err := checkBudget(); err != nil {
				return err
			}
		}
		liveTF += uint64(entry.TermFrequency)
	}
	if len(live) == 0 {
		if termStats.DocumentFrequency != 0 || termStats.TotalTermFrequency != 0 {
			return errMalformedTextStorage("text-v2 term %q has live stats df=%d tf=%d but no current postings", term.term, termStats.DocumentFrequency, termStats.TotalTermFrequency)
		}
		for _, oldKey := range term.oldKeys {
			if checkBudget != nil {
				if err := checkBudget(); err != nil {
					return err
				}
			}
			p.postingBlocksTable.DeleteSteal(append([]byte(nil), oldKey...))
		}
		if termStats.PostingBlockCount != 0 {
			p.termsTable.DeleteSteal(encodeTextV2TermStatsKey(term.term))
			p.termsChanged = true
		}
		p.postingChanged = p.postingChanged || len(term.oldKeys) > 0
		p.stats.PostingBlocksDeleted += uint64(len(term.oldKeys))
		p.stats.TermsPurged++
		return nil
	}
	if termStats.StatsGeneration == 0 {
		return errMalformedTextStorage("text-v2 term %q has current postings but missing term stats", term.term)
	}
	if termStats.DocumentFrequency != uint64(len(live)) || termStats.TotalTermFrequency != liveTF {
		return errMalformedTextStorage("text-v2 term %q stats df/tf=%d/%d do not match current postings %d/%d", term.term, termStats.DocumentFrequency, termStats.TotalTermFrequency, len(live), liveTF)
	}

	if checkBudget != nil {
		if err := checkBudget(); err != nil {
			return err
		}
	}
	newBlocks, err := buildTextV2PostingBlockKVs(term.term, live, uint32(len(def.Fields)), textV2PostingBlockBuildOptions{Kind: textV2PostingBlockKindSealed, TargetPostings: target, BlockIDStart: 1})
	if err != nil {
		return err
	}
	needsRewrite := force || term.stale != 0 || term.nonSealed || uint64(len(newBlocks)) != term.oldBlocks || termStats.PostingBlockCount != uint64(len(newBlocks))
	if !needsRewrite {
		p.stats.PostingBlocksAfter += term.oldBlocks
		return nil
	}
	p.stats.PostingBlocksAfter += uint64(len(newBlocks))
	newKeySet := make(map[string]struct{}, len(newBlocks))
	for _, block := range newBlocks {
		if checkBudget != nil {
			if err := checkBudget(); err != nil {
				return err
			}
		}
		newKeySet[string(block.Key)] = struct{}{}
		p.postingBlocksTable.SetSteal(append([]byte(nil), block.Key...), append([]byte(nil), block.Value...))
	}
	for _, oldKey := range term.oldKeys {
		if checkBudget != nil {
			if err := checkBudget(); err != nil {
				return err
			}
		}
		if _, ok := newKeySet[string(oldKey)]; ok {
			continue
		}
		p.postingBlocksTable.DeleteSteal(append([]byte(nil), oldKey...))
		p.stats.PostingBlocksDeleted++
	}
	p.stats.PostingBlocksWritten += uint64(len(newBlocks))
	p.stats.TermsRewritten++
	p.postingChanged = true
	if termStats.PostingBlockCount != uint64(len(newBlocks)) || term.stale != 0 || term.nonSealed || force {
		nextGeneration := status.RootGeneration + 1
		if nextGeneration == 0 {
			return errMalformedTextStorage("text-v2 root generation overflow")
		}
		p.termsTable.SetSteal(encodeTextV2TermStatsKey(term.term), encodeTextV2TermStatsValue(textV2TermStatsValue{
			StatsGeneration:    nextGeneration,
			DocumentFrequency:  termStats.DocumentFrequency,
			TotalTermFrequency: termStats.TotalTermFrequency,
			PostingBlockCount:  uint64(len(newBlocks)),
		}))
		p.termsChanged = true
	}
	return nil
}

func (term *textV2PostingRewriteTerm) filterCurrentEntries(snap *backenddb.Snapshot, catalog *collectionCatalog, ctx *textV2SearchContext, cache *textV2SearchBlockCache, checkBudget func() error) error {
	if term == nil {
		return nil
	}
	if term.liveByOrd == nil {
		term.liveByOrd = make(map[uint64]textV2PostingBlockEntry)
	}
	var lookupStats TextSearchStats
	for _, entry := range term.entries {
		if checkBudget != nil {
			if err := checkBudget(); err != nil {
				return err
			}
		}
		if entry.Ordinal >= ctx.status.NextOrdinal || entry.Generation > ctx.status.RootGeneration {
			return errMalformedTextStorage("text-v2 posting entry outside status snapshot")
		}
		norm, ok, err := cache.normEntry(snap, catalog, ctx, entry.Ordinal, &lookupStats)
		if err != nil {
			return err
		}
		current := ok && !norm.tombstoned() && norm.Generation == entry.Generation
		if current {
			docMap, ok, err := cache.docMapEntry(snap, catalog, ctx, entry.Ordinal)
			if err != nil {
				return err
			}
			current = ok && !docMap.tombstoned() && docMap.Generation == norm.Generation && docMap.Flags == norm.Flags
		}
		if !current {
			term.stale++
			continue
		}
		if prev, exists := term.liveByOrd[entry.Ordinal]; exists && prev.Generation == entry.Generation {
			return errMalformedTextStorage("duplicate current text-v2 posting for term %q ordinal %d generation %d", term.term, entry.Ordinal, entry.Generation)
		}
		term.liveByOrd[entry.Ordinal] = entry
	}
	return nil
}

func scanTextV2PostingRewriteTerms(snap *backenddb.Snapshot, catalog *collectionCatalog, rootName string, budget *textV2RewriteBudget, fn func(*textV2PostingRewriteTerm) error) error {
	it, err := collectionIteratorAtCatalogRoot(snap, catalog, rootName, nil, nil, false)
	if err != nil {
		if err == tree.ErrKeyNotFound {
			return errMalformedTextStorage("missing text-v2 posting blocks root %q", rootName)
		}
		return err
	}
	if it == nil {
		return errMalformedTextStorage("missing text-v2 posting blocks root %q", rootName)
	}
	defer func() { _ = it.Close() }()
	var current *textV2PostingRewriteTerm
	flush := func() error {
		if current == nil {
			return nil
		}
		return fn(current)
	}
	var scratch []uint32
	for it.Valid() {
		if budget != nil {
			if err := budget.check(); err != nil {
				return err
			}
		}
		if it.IsDeleted() {
			it.Next()
			continue
		}
		keyBytes := it.UnsafeKey()
		if bytes.Equal(keyBytes, encodeTextV2FormatKey()) {
			it.Next()
			continue
		}
		key, err := decodeTextV2PostingBlockKey(keyBytes)
		if err != nil {
			return err
		}
		if current == nil || current.term != key.Term {
			if err := flush(); err != nil {
				return err
			}
			if budget != nil {
				if err := budget.reserveTerm(); err != nil {
					return err
				}
			}
			current = &textV2PostingRewriteTerm{term: key.Term, liveByOrd: make(map[uint64]textV2PostingBlockEntry)}
		}
		if budget != nil {
			if err := budget.reservePostingBlock(); err != nil {
				return err
			}
		}
		raw := it.ValueCopy(nil)
		scanner, err := newTextV2PostingBlockEntryScanner(raw, scratch)
		if err != nil {
			return err
		}
		if scanner.block.BlockStart != key.BlockStart || scanner.block.BlockID != key.BlockID {
			return errMalformedTextStorage("text-v2 posting block key/value identity mismatch")
		}
		if budget != nil {
			if err := budget.reservePostings(uint64(scanner.block.Summary.DocCount)); err != nil {
				return err
			}
		}
		current.oldKeys = append(current.oldKeys, append([]byte(nil), keyBytes...))
		current.oldBlocks++
		if scanner.block.Kind != textV2PostingBlockKindSealed {
			current.nonSealed = true
		}
		var entry textV2PostingBlockEntry
		for scanner.Next(&entry) {
			current.postings++
			current.entries = append(current.entries, textV2PostingBlockEntry{
				Ordinal:          entry.Ordinal,
				Generation:       entry.Generation,
				TermFrequency:    entry.TermFrequency,
				FieldFrequencies: append([]uint32(nil), entry.FieldFrequencies...),
				Flags:            entry.Flags,
			})
		}
		if err := scanner.Err(); err != nil {
			return err
		}
		scratch = scanner.scratch
		it.Next()
	}
	if err := it.Error(); err != nil {
		return err
	}
	return flush()
}

func (p *textV2RewritePlan) purgeTextV2Tombstones(snap *backenddb.Snapshot, catalog *collectionCatalog, def TextIndexDefinition, status textV2IndexStatusValue, checkBudget func() error) (bool, error) {
	purgedOrdinals, err := p.purgeTextV2DocIDTombstones(snap, catalog, def, status, checkBudget)
	if err != nil {
		return false, err
	}
	// A delete followed by an insert with the same document ID overwrites the
	// docID tombstone with the new live ordinal. The old ordinal is still
	// tombstoned in docmap/norm blocks and still contributes to DeletedDocuments,
	// so tombstone discovery must include those ordinal-keyed sidecars too.
	if err := collectTextV2DocMapTombstoneOrdinals(snap, catalog, def, status, purgedOrdinals, checkBudget); err != nil {
		return false, err
	}
	if err := collectTextV2NormTombstoneOrdinals(snap, catalog, def, status, purgedOrdinals, checkBudget); err != nil {
		return false, err
	}
	if len(purgedOrdinals) == 0 {
		return false, nil
	}
	if err := p.purgeTextV2DocMapTombstones(snap, catalog, def, purgedOrdinals, checkBudget); err != nil {
		return false, err
	}
	if err := p.purgeTextV2NormTombstones(snap, catalog, def, purgedOrdinals, checkBudget); err != nil {
		return false, err
	}
	return true, nil
}

func (p *textV2RewritePlan) purgeTextV2DocIDTombstones(snap *backenddb.Snapshot, catalog *collectionCatalog, def TextIndexDefinition, status textV2IndexStatusValue, checkBudget func() error) (map[uint64]struct{}, error) {
	rootName := collectionTextV2DocIDRootName(catalog.meta.Name, def.Name)
	it, err := collectionIteratorAtCatalogRoot(snap, catalog, rootName, nil, nil, false)
	if err != nil || it == nil {
		return nil, err
	}
	defer func() { _ = it.Close() }()
	purged := make(map[uint64]struct{})
	for it.Valid() {
		if checkBudget != nil {
			if err := checkBudget(); err != nil {
				return nil, err
			}
		}
		if it.IsDeleted() {
			it.Next()
			continue
		}
		key := it.UnsafeKey()
		if bytes.Equal(key, encodeTextV2FormatKey()) {
			it.Next()
			continue
		}
		if _, err := decodeTextV2DocIDKey(key); err != nil {
			return nil, err
		}
		doc, err := decodeTextV2DocIDValue(it.ValueCopy(nil))
		if err != nil {
			return nil, err
		}
		if doc.Ordinal >= status.NextOrdinal || doc.Generation > status.RootGeneration {
			return nil, errMalformedTextStorage("text-v2 docid tombstone purge entry outside status snapshot")
		}
		if doc.tombstoned() {
			p.docIDTable.DeleteSteal(append([]byte(nil), key...))
			purged[doc.Ordinal] = struct{}{}
			p.stats.TombstoneDocIDEntriesPurged++
		}
		it.Next()
	}
	return purged, it.Error()
}

func collectTextV2DocMapTombstoneOrdinals(snap *backenddb.Snapshot, catalog *collectionCatalog, def TextIndexDefinition, status textV2IndexStatusValue, out map[uint64]struct{}, checkBudget func() error) error {
	rootName := collectionTextV2DocMapRootName(catalog.meta.Name, def.Name)
	it, err := collectionIteratorAtCatalogRoot(snap, catalog, rootName, nil, nil, false)
	if err != nil || it == nil {
		return err
	}
	defer func() { _ = it.Close() }()
	for it.Valid() {
		if checkBudget != nil {
			if err := checkBudget(); err != nil {
				return err
			}
		}
		if it.IsDeleted() {
			it.Next()
			continue
		}
		key := it.UnsafeKey()
		if bytes.Equal(key, encodeTextV2FormatKey()) {
			it.Next()
			continue
		}
		blockStart, err := decodeTextV2BlockKey(key)
		if err != nil {
			return err
		}
		block, err := decodeTextV2DocMapBlockValue(it.ValueCopy(nil))
		if err != nil {
			return err
		}
		if block.BlockStart != blockStart || block.BlockSize != textV2DefaultDocMapBlockSize {
			return errMalformedTextStorage("text-v2 docmap tombstone collection block key/value mismatch")
		}
		for _, entry := range block.Entries {
			if entry.Ordinal >= status.NextOrdinal || entry.Generation > status.DocMapGeneration {
				return errMalformedTextStorage("text-v2 docmap tombstone collection entry outside status snapshot")
			}
			if entry.tombstoned() {
				out[entry.Ordinal] = struct{}{}
			}
		}
		it.Next()
	}
	return it.Error()
}

func collectTextV2NormTombstoneOrdinals(snap *backenddb.Snapshot, catalog *collectionCatalog, def TextIndexDefinition, status textV2IndexStatusValue, out map[uint64]struct{}, checkBudget func() error) error {
	rootName := collectionTextV2NormBlocksRootName(catalog.meta.Name, def.Name)
	it, err := collectionIteratorAtCatalogRoot(snap, catalog, rootName, nil, nil, false)
	if err != nil || it == nil {
		return err
	}
	defer func() { _ = it.Close() }()
	for it.Valid() {
		if checkBudget != nil {
			if err := checkBudget(); err != nil {
				return err
			}
		}
		if it.IsDeleted() {
			it.Next()
			continue
		}
		key := it.UnsafeKey()
		if bytes.Equal(key, encodeTextV2FormatKey()) {
			it.Next()
			continue
		}
		blockStart, err := decodeTextV2BlockKey(key)
		if err != nil {
			return err
		}
		block, err := decodeTextV2NormBlockValue(it.ValueCopy(nil))
		if err != nil {
			return err
		}
		if block.BlockStart != blockStart || block.BlockSize != textV2DefaultNormBlockSize || block.FieldCount != uint32(len(def.Fields)) {
			return errMalformedTextStorage("text-v2 norm tombstone collection block key/value mismatch")
		}
		for _, entry := range block.Entries {
			if entry.Ordinal >= status.NextOrdinal || entry.Generation > status.NormGeneration {
				return errMalformedTextStorage("text-v2 norm tombstone collection entry outside status snapshot")
			}
			if entry.tombstoned() {
				out[entry.Ordinal] = struct{}{}
			}
		}
		it.Next()
	}
	return it.Error()
}

func (p *textV2RewritePlan) purgeTextV2DocMapTombstones(snap *backenddb.Snapshot, catalog *collectionCatalog, def TextIndexDefinition, purged map[uint64]struct{}, checkBudget func() error) error {
	rootName := collectionTextV2DocMapRootName(catalog.meta.Name, def.Name)
	it, err := collectionIteratorAtCatalogRoot(snap, catalog, rootName, nil, nil, false)
	if err != nil || it == nil {
		return err
	}
	defer func() { _ = it.Close() }()
	for it.Valid() {
		if checkBudget != nil {
			if err := checkBudget(); err != nil {
				return err
			}
		}
		if it.IsDeleted() {
			it.Next()
			continue
		}
		key := it.UnsafeKey()
		if bytes.Equal(key, encodeTextV2FormatKey()) {
			it.Next()
			continue
		}
		blockStart, err := decodeTextV2BlockKey(key)
		if err != nil {
			return err
		}
		block, err := decodeTextV2DocMapBlockValue(it.ValueCopy(nil))
		if err != nil {
			return err
		}
		if block.BlockStart != blockStart || block.BlockSize != textV2DefaultDocMapBlockSize {
			return errMalformedTextStorage("text-v2 docmap tombstone purge block key/value mismatch")
		}
		kept := block.Entries[:0]
		changed := false
		for _, entry := range block.Entries {
			if _, ok := purged[entry.Ordinal]; ok && entry.tombstoned() {
				changed = true
				p.stats.TombstoneDocMapEntriesPurged++
				continue
			}
			kept = append(kept, entry)
		}
		if changed {
			block.Entries = append([]textV2DocMapEntry(nil), kept...)
			if len(block.Entries) == 0 {
				p.docMapTable.DeleteSteal(append([]byte(nil), key...))
			} else {
				p.docMapTable.SetSteal(append([]byte(nil), key...), encodeTextV2DocMapBlockValue(block))
			}
		}
		it.Next()
	}
	return it.Error()
}

func (p *textV2RewritePlan) purgeTextV2NormTombstones(snap *backenddb.Snapshot, catalog *collectionCatalog, def TextIndexDefinition, purged map[uint64]struct{}, checkBudget func() error) error {
	rootName := collectionTextV2NormBlocksRootName(catalog.meta.Name, def.Name)
	it, err := collectionIteratorAtCatalogRoot(snap, catalog, rootName, nil, nil, false)
	if err != nil || it == nil {
		return err
	}
	defer func() { _ = it.Close() }()
	for it.Valid() {
		if checkBudget != nil {
			if err := checkBudget(); err != nil {
				return err
			}
		}
		if it.IsDeleted() {
			it.Next()
			continue
		}
		key := it.UnsafeKey()
		if bytes.Equal(key, encodeTextV2FormatKey()) {
			it.Next()
			continue
		}
		blockStart, err := decodeTextV2BlockKey(key)
		if err != nil {
			return err
		}
		block, err := decodeTextV2NormBlockValue(it.ValueCopy(nil))
		if err != nil {
			return err
		}
		if block.BlockStart != blockStart || block.BlockSize != textV2DefaultNormBlockSize || block.FieldCount != uint32(len(def.Fields)) {
			return errMalformedTextStorage("text-v2 norm tombstone purge block key/value mismatch")
		}
		kept := block.Entries[:0]
		changed := false
		for _, entry := range block.Entries {
			if _, ok := purged[entry.Ordinal]; ok && entry.tombstoned() {
				changed = true
				p.stats.TombstoneNormEntriesPurged++
				continue
			}
			kept = append(kept, entry)
		}
		if changed {
			block.Entries = append([]textV2NormBlockEntry(nil), kept...)
			if len(block.Entries) == 0 {
				p.normTable.DeleteSteal(append([]byte(nil), key...))
			} else {
				p.normTable.SetSteal(append([]byte(nil), key...), encodeTextV2NormBlockValue(block))
			}
		}
		it.Next()
	}
	return it.Error()
}

func readTextV2RewriteTermStats(snap *backenddb.Snapshot, catalog *collectionCatalog, rootName string, budget *textV2RewriteBudget) (map[string]textV2TermStatsValue, error) {
	it, err := collectionIteratorAtCatalogRoot(snap, catalog, rootName, nil, nil, false)
	if err != nil || it == nil {
		return nil, err
	}
	defer func() { _ = it.Close() }()
	out := make(map[string]textV2TermStatsValue)
	var termStatsScanned uint64
	for it.Valid() {
		if budget != nil {
			if err := budget.check(); err != nil {
				return nil, err
			}
		}
		if it.IsDeleted() {
			it.Next()
			continue
		}
		key := it.UnsafeKey()
		if bytes.Equal(key, encodeTextV2FormatKey()) {
			it.Next()
			continue
		}
		statsKey, err := decodeTextV2StatsKey(key)
		if err != nil {
			return nil, err
		}
		if statsKey.Kind != textV2KeyKindTermStats {
			it.Next()
			continue
		}
		termStatsScanned++
		if budget != nil {
			if err := budget.checkTermCount(termStatsScanned); err != nil {
				return nil, err
			}
		}
		stats, err := decodeTextV2TermStatsValue(it.ValueCopy(nil))
		if err != nil {
			return nil, err
		}
		out[statsKey.Value] = stats
		it.Next()
	}
	return out, it.Error()
}

func resetTextV2RewritePlanTables(plan *textV2RewritePlan) {
	if plan == nil {
		return
	}
	resetCollectionTables([]memtable.Table{plan.postingBlocksTable, plan.termsTable, plan.docIDTable, plan.docMapTable, plan.normTable, plan.generationsTable})
}

func textV2RewriteMergeStateFromStats(stats TextIndexStorageStats) string {
	if stats.Version != TextIndexVersionV2 {
		return TextIndexRewriteMergeStateNotApplicable
	}
	if stats.V2DeletedDocs != 0 || stats.V2MicroPostingBlocks != 0 || stats.V2DeltaPostingBlocks != 0 {
		return TextIndexRewriteMergeStatePending
	}
	return TextIndexRewriteMergeStateCompacted
}
