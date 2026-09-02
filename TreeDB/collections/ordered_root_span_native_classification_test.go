package collections

import (
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

type collectionOrderedRootRouteAnchor string

const (
	collectionRouteCollectionManagerCreateCollection                 collectionOrderedRootRouteAnchor = "TreeDB/collections.(*CollectionManager).CreateCollection"
	collectionRouteCollectionInsertBatch                             collectionOrderedRootRouteAnchor = "TreeDB/collections.(*Collection).InsertBatch"
	collectionRouteCollectionFlush                                   collectionOrderedRootRouteAnchor = "TreeDB/collections.(*Collection).Flush"
	collectionRouteCollectionCompactRootOverlays                     collectionOrderedRootRouteAnchor = "TreeDB/collections.(*Collection).CompactRootOverlays"
	collectionRouteCollectionDeleteDocument                          collectionOrderedRootRouteAnchor = "TreeDB/collections.(*Collection).DeleteDocument"
	collectionRouteCollectionDeleteBatch                             collectionOrderedRootRouteAnchor = "TreeDB/collections.(*Collection).DeleteBatch"
	collectionRouteCollectionUpdate                                  collectionOrderedRootRouteAnchor = "TreeDB/collections.(*Collection).Update"
	collectionRouteCollectionUpdateBatch                             collectionOrderedRootRouteAnchor = "TreeDB/collections.(*Collection).UpdateBatch"
	collectionRouteCommandWALActive                                  collectionOrderedRootRouteAnchor = "TreeDB/collections.(*Collection).commandWALActive"
	collectionRouteWithCommandWALPublishCoordinator                  collectionOrderedRootRouteAnchor = "TreeDB/collections.(*Collection).withCommandWALPublishCoordinator"
	collectionRouteNewInsertCommandWALIntent                         collectionOrderedRootRouteAnchor = "TreeDB/collections.(*Collection).newCollectionInsertCommandWALIntent"
	collectionRouteNewDeleteCommandWALIntent                         collectionOrderedRootRouteAnchor = "TreeDB/collections.(*Collection).newCollectionDeleteCommandWALIntent"
	collectionRouteNewUpdateCommandWALIntent                         collectionOrderedRootRouteAnchor = "TreeDB/collections.(*Collection).newCollectionUpdateCommandWALIntent"
	collectionRouteFlushBufferedNoIndexLocked                        collectionOrderedRootRouteAnchor = "TreeDB/collections.(*Collection).flushBufferedNoIndexLocked"
	collectionRouteBufferIndexedInsertPlanLocked                     collectionOrderedRootRouteAnchor = "TreeDB/collections.(*Collection).bufferIndexedInsertPlanLocked"
	collectionRoutePublishPreparedIndexedFlush                       collectionOrderedRootRouteAnchor = "TreeDB/collections.(*Collection).publishPreparedIndexedFlush"
	collectionRouteCompactRootOverlaysLocked                         collectionOrderedRootRouteAnchor = "TreeDB/collections.(*Collection).compactRootOverlaysLocked"
	collectionRoutePublishUpdateBatchPlanLocked                      collectionOrderedRootRouteAnchor = "TreeDB/collections.(*Collection).publishUpdateBatchPlanLocked"
	collectionRouteBuildRootDescriptorSystemDeltaIterator            collectionOrderedRootRouteAnchor = "TreeDB/collections.(*Collection).buildRootDescriptorSystemDeltaIterator"
	collectionRouteBuildOverlayDescriptorSystemDelta                 collectionOrderedRootRouteAnchor = "TreeDB/collections.(*Collection).buildRootOverlayDescriptorSystemDeltaIteratorForMeta"
	collectionRouteInsertBatchPlanPublishRootRuns                    collectionOrderedRootRouteAnchor = "TreeDB/collections.(*insertBatchPlan).publishRootRuns"
	collectionRouteInsertBatchPlanRootNamesAndBaseIDs                collectionOrderedRootRouteAnchor = "TreeDB/collections.insertBatchPlanRootNamesAndBaseIDs"
	collectionRouteBuildBufferedOverlayDeltaBatchInputs              collectionOrderedRootRouteAnchor = "TreeDB/collections.buildBufferedRootOverlayDeltaBatchPublishInputs"
	collectionRouteBuildBufferedDeltaBatchInputs                     collectionOrderedRootRouteAnchor = "TreeDB/collections.buildBufferedRootDeltaBatchPublishInputs"
	collectionRouteBuildRootDeltaBatchInputsFromTables               collectionOrderedRootRouteAnchor = "TreeDB/collections.buildRootDeltaBatchPublishInputsFromTables"
	collectionRouteCoalesceCollectionRootDeltaTables                 collectionOrderedRootRouteAnchor = "TreeDB/collections.coalesceCollectionRootDeltaTables"
	collectionRouteBuildCollectionRootOverlayFilters                 collectionOrderedRootRouteAnchor = "TreeDB/collections.buildCollectionRootOverlayFilters"
	collectionRouteOverlayDeltaBaseRoot                              collectionOrderedRootRouteAnchor = "TreeDB/collections.overlayDeltaBaseRoot"
	collectionRouteDBPublishOrderedRootGroup                         collectionOrderedRootRouteAnchor = "TreeDB/db.(*DB).PublishOrderedRootGroup"
	collectionRouteDBPublishOrderedRootDeltaGroup                    collectionOrderedRootRouteAnchor = "TreeDB/db.(*DB).PublishOrderedRootDeltaGroupWithSystemDeltaBuilder"
	collectionRouteDBPublishOrderedRootDeltaGroupCommandWAL          collectionOrderedRootRouteAnchor = "TreeDB/db.(*DB).PublishOrderedRootDeltaGroupWithCommandWALAndSystemDeltaBuilder"
	collectionRouteDBPublishOrderedRootDeltaBatch                    collectionOrderedRootRouteAnchor = "TreeDB/db.(*DB).PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder"
	collectionRouteDBPublishOrderedRootDeltaBatchPreflight           collectionOrderedRootRouteAnchor = "TreeDB/db.(*DB).PublishOrderedRootDeltaBatchGroupWithPreflightAndSystemDeltaBuilder"
	collectionRouteDBPublishOrderedRootDeltaBatchCommandWAL          collectionOrderedRootRouteAnchor = "TreeDB/db.(*DB).PublishOrderedRootDeltaBatchGroupWithCommandWALAndSystemDeltaBuilder"
	collectionRouteDBPublishOrderedRootDeltaBatchPreflightCommandWAL collectionOrderedRootRouteAnchor = "TreeDB/db.(*DB).PublishOrderedRootDeltaBatchGroupWithPreflightCommandWALAndSystemDeltaBuilder"
)

var collectionOrderedRootRouteAnchorEvidence = map[collectionOrderedRootRouteAnchor]any{
	collectionRouteCollectionManagerCreateCollection:                 (*CollectionManager).CreateCollection,
	collectionRouteCollectionInsertBatch:                             (*Collection).InsertBatch,
	collectionRouteCollectionFlush:                                   (*Collection).Flush,
	collectionRouteCollectionCompactRootOverlays:                     (*Collection).CompactRootOverlays,
	collectionRouteCollectionDeleteDocument:                          (*Collection).DeleteDocument,
	collectionRouteCollectionDeleteBatch:                             (*Collection).DeleteBatch,
	collectionRouteCollectionUpdate:                                  (*Collection).Update,
	collectionRouteCollectionUpdateBatch:                             (*Collection).UpdateBatch,
	collectionRouteCommandWALActive:                                  (*Collection).commandWALActive,
	collectionRouteWithCommandWALPublishCoordinator:                  (*Collection).withCommandWALPublishCoordinator,
	collectionRouteNewInsertCommandWALIntent:                         (*Collection).newCollectionInsertCommandWALIntent,
	collectionRouteNewDeleteCommandWALIntent:                         (*Collection).newCollectionDeleteCommandWALIntent,
	collectionRouteNewUpdateCommandWALIntent:                         (*Collection).newCollectionUpdateCommandWALIntent,
	collectionRouteFlushBufferedNoIndexLocked:                        (*Collection).flushBufferedNoIndexLocked,
	collectionRouteBufferIndexedInsertPlanLocked:                     (*Collection).bufferIndexedInsertPlanLocked,
	collectionRoutePublishPreparedIndexedFlush:                       (*Collection).publishPreparedIndexedFlush,
	collectionRouteCompactRootOverlaysLocked:                         (*Collection).compactRootOverlaysLocked,
	collectionRoutePublishUpdateBatchPlanLocked:                      (*Collection).publishUpdateBatchPlanLocked,
	collectionRouteBuildRootDescriptorSystemDeltaIterator:            (*Collection).buildRootDescriptorSystemDeltaIterator,
	collectionRouteBuildOverlayDescriptorSystemDelta:                 (*Collection).buildRootOverlayDescriptorSystemDeltaIteratorForMeta,
	collectionRouteInsertBatchPlanPublishRootRuns:                    (*insertBatchPlan).publishRootRuns,
	collectionRouteInsertBatchPlanRootNamesAndBaseIDs:                insertBatchPlanRootNamesAndBaseIDs,
	collectionRouteBuildBufferedOverlayDeltaBatchInputs:              buildBufferedRootOverlayDeltaBatchPublishInputs,
	collectionRouteBuildBufferedDeltaBatchInputs:                     buildBufferedRootDeltaBatchPublishInputs,
	collectionRouteBuildRootDeltaBatchInputsFromTables:               buildRootDeltaBatchPublishInputsFromTables,
	collectionRouteCoalesceCollectionRootDeltaTables:                 coalesceCollectionRootDeltaTables,
	collectionRouteBuildCollectionRootOverlayFilters:                 buildCollectionRootOverlayFilters,
	collectionRouteOverlayDeltaBaseRoot:                              overlayDeltaBaseRoot,
	collectionRouteDBPublishOrderedRootGroup:                         (*backenddb.DB).PublishOrderedRootGroup,
	collectionRouteDBPublishOrderedRootDeltaGroup:                    (*backenddb.DB).PublishOrderedRootDeltaGroupWithSystemDeltaBuilder,
	collectionRouteDBPublishOrderedRootDeltaGroupCommandWAL:          (*backenddb.DB).PublishOrderedRootDeltaGroupWithCommandWALAndSystemDeltaBuilder,
	collectionRouteDBPublishOrderedRootDeltaBatch:                    (*backenddb.DB).PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder,
	collectionRouteDBPublishOrderedRootDeltaBatchPreflight:           (*backenddb.DB).PublishOrderedRootDeltaBatchGroupWithPreflightAndSystemDeltaBuilder,
	collectionRouteDBPublishOrderedRootDeltaBatchCommandWAL:          (*backenddb.DB).PublishOrderedRootDeltaBatchGroupWithCommandWALAndSystemDeltaBuilder,
	collectionRouteDBPublishOrderedRootDeltaBatchPreflightCommandWAL: (*backenddb.DB).PublishOrderedRootDeltaBatchGroupWithPreflightCommandWALAndSystemDeltaBuilder,
}

type collectionOrderedRootExpectedOutcome string

const (
	collectionOrderedRootOutcomeBufferedNoIndexFlush     collectionOrderedRootExpectedOutcome = "buffered-no-index-flush-delta-group"
	collectionOrderedRootOutcomeBufferedIndexedFlush     collectionOrderedRootExpectedOutcome = "buffered-indexed-flush-delta-batch"
	collectionOrderedRootOutcomeOverlayWarmSingleBase    collectionOrderedRootExpectedOutcome = "overlay-single-base-warm-delta-batch"
	collectionOrderedRootOutcomeOverlayColdBuildZeroBase collectionOrderedRootExpectedOutcome = "overlay-zero-base-cold-build-delta-batch"
	collectionOrderedRootOutcomeOverlayCompaction        collectionOrderedRootExpectedOutcome = "overlay-compaction-delta-group"
	collectionOrderedRootOutcomeNonCommandWALDeltaGroup  collectionOrderedRootExpectedOutcome = "non-command-wal-delta-group"
	collectionOrderedRootOutcomeCommandWALDeltaGroup     collectionOrderedRootExpectedOutcome = "command-wal-delta-group"
	collectionOrderedRootOutcomeNonCommandWALDeltaBatch  collectionOrderedRootExpectedOutcome = "non-command-wal-delta-batch"
	collectionOrderedRootOutcomeCommandWALDeltaBatch     collectionOrderedRootExpectedOutcome = "command-wal-delta-batch"
	collectionOrderedRootOutcomeMultiIndexRootGroups     collectionOrderedRootExpectedOutcome = "multi-index-root-groups"
)

type collectionOrderedRootDownstreamAction string

const (
	collectionOrderedRootActionEligibleFor3022Observability collectionOrderedRootDownstreamAction = "eligible-for-#3022-observability"
	collectionOrderedRootActionRequires3023Correctness      collectionOrderedRootDownstreamAction = "requires-#3023-correctness"
	collectionOrderedRootActionRequires3024OutputOwnership  collectionOrderedRootDownstreamAction = "requires-#3024-output-ownership"
	collectionOrderedRootActionImplementedCurrentSerialOnly collectionOrderedRootDownstreamAction = "implemented-current-serial-only"
)

var collectionOrderedRootExpectedOutcomes = map[collectionOrderedRootExpectedOutcome]struct{}{
	collectionOrderedRootOutcomeBufferedNoIndexFlush:     {},
	collectionOrderedRootOutcomeBufferedIndexedFlush:     {},
	collectionOrderedRootOutcomeOverlayWarmSingleBase:    {},
	collectionOrderedRootOutcomeOverlayColdBuildZeroBase: {},
	collectionOrderedRootOutcomeOverlayCompaction:        {},
	collectionOrderedRootOutcomeNonCommandWALDeltaGroup:  {},
	collectionOrderedRootOutcomeCommandWALDeltaGroup:     {},
	collectionOrderedRootOutcomeNonCommandWALDeltaBatch:  {},
	collectionOrderedRootOutcomeCommandWALDeltaBatch:     {},
	collectionOrderedRootOutcomeMultiIndexRootGroups:     {},
}

var collectionOrderedRootDownstreamActions = map[collectionOrderedRootDownstreamAction]struct{}{
	collectionOrderedRootActionEligibleFor3022Observability: {},
	collectionOrderedRootActionRequires3023Correctness:      {},
	collectionOrderedRootActionRequires3024OutputOwnership:  {},
	collectionOrderedRootActionImplementedCurrentSerialOnly: {},
}

type collectionOrderedRootRouteClassificationRow struct {
	ID                string
	PublicAnchors     []collectionOrderedRootRouteAnchor
	InternalAnchors   []collectionOrderedRootRouteAnchor
	DBPublishAnchors  []collectionOrderedRootRouteAnchor
	ExpectedOutcome   collectionOrderedRootExpectedOutcome
	DownstreamActions []collectionOrderedRootDownstreamAction
	RootGroups        string
	ProductionSupport string
	Covers            []string
}

func collectionOrderedRootRouteClassificationRows() []collectionOrderedRootRouteClassificationRow {
	return []collectionOrderedRootRouteClassificationRow{
		{
			ID:               "buffered-no-index-flush-primary-root",
			PublicAnchors:    []collectionOrderedRootRouteAnchor{collectionRouteCollectionFlush},
			InternalAnchors:  []collectionOrderedRootRouteAnchor{collectionRouteFlushBufferedNoIndexLocked, collectionRouteBuildRootDescriptorSystemDeltaIterator},
			DBPublishAnchors: []collectionOrderedRootRouteAnchor{collectionRouteDBPublishOrderedRootDeltaGroup},
			ExpectedOutcome:  collectionOrderedRootOutcomeBufferedNoIndexFlush,
			DownstreamActions: []collectionOrderedRootDownstreamAction{
				collectionOrderedRootActionEligibleFor3022Observability,
				collectionOrderedRootActionRequires3023Correctness,
			},
			RootGroups:        "primary root only; BaseRoot is the catalog primary root",
			ProductionSupport: "current buffered no-index flush is supported through serial ordered-root delta group publish; #3022/#3023 must classify span-native enablement",
			Covers:            []string{"buffered_flush", "non_command_wal_publish", "primary_root"},
		},
		{
			ID:              "buffered-indexed-flush-root-groups",
			PublicAnchors:   []collectionOrderedRootRouteAnchor{collectionRouteCollectionInsertBatch, collectionRouteCollectionFlush},
			InternalAnchors: []collectionOrderedRootRouteAnchor{collectionRouteBufferIndexedInsertPlanLocked, collectionRoutePublishPreparedIndexedFlush, collectionRouteBuildBufferedDeltaBatchInputs},
			DBPublishAnchors: []collectionOrderedRootRouteAnchor{
				collectionRouteDBPublishOrderedRootDeltaBatch,
			},
			ExpectedOutcome: collectionOrderedRootOutcomeBufferedIndexedFlush,
			DownstreamActions: []collectionOrderedRootDownstreamAction{
				collectionOrderedRootActionEligibleFor3022Observability,
				collectionOrderedRootActionRequires3023Correctness,
				collectionOrderedRootActionRequires3024OutputOwnership,
			},
			RootGroups:        "primary, secondary, index-state, text, and unique-value roots materialized from indexed flush units",
			ProductionSupport: "current indexed flush is supported through ordered-root delta batch publish; #3022 observes route choice and #3023/#3024 gate span-native output",
			Covers:            []string{"buffered_flush", "multi_index_roots", "secondary_index_roots", "non_command_wal_publish"},
		},
		{
			ID:              "indexed-overlay-single-base-warm-flush",
			PublicAnchors:   []collectionOrderedRootRouteAnchor{collectionRouteCollectionInsertBatch, collectionRouteCollectionFlush},
			InternalAnchors: []collectionOrderedRootRouteAnchor{collectionRoutePublishPreparedIndexedFlush, collectionRouteBuildBufferedOverlayDeltaBatchInputs, collectionRouteOverlayDeltaBaseRoot},
			DBPublishAnchors: []collectionOrderedRootRouteAnchor{
				collectionRouteDBPublishOrderedRootDeltaBatch,
			},
			ExpectedOutcome: collectionOrderedRootOutcomeOverlayWarmSingleBase,
			DownstreamActions: []collectionOrderedRootDownstreamAction{
				collectionOrderedRootActionRequires3023Correctness,
				collectionOrderedRootActionRequires3024OutputOwnership,
			},
			RootGroups:        "one existing overlay becomes the nonzero BaseRoot; new overlay delta includes deleted markers",
			ProductionSupport: "single-overlay flush is current warm behavior, distinct from cold-build zero-base overlay publish",
			Covers:            []string{"overlay_flush", "overlay_warm_single_base", "buffered_flush"},
		},
		{
			ID:              "indexed-overlay-zero-base-cold-build-flush",
			PublicAnchors:   []collectionOrderedRootRouteAnchor{collectionRouteCollectionInsertBatch, collectionRouteCollectionFlush},
			InternalAnchors: []collectionOrderedRootRouteAnchor{collectionRoutePublishPreparedIndexedFlush, collectionRouteBuildBufferedOverlayDeltaBatchInputs, collectionRouteOverlayDeltaBaseRoot},
			DBPublishAnchors: []collectionOrderedRootRouteAnchor{
				collectionRouteDBPublishOrderedRootDeltaBatch,
			},
			ExpectedOutcome: collectionOrderedRootOutcomeOverlayColdBuildZeroBase,
			DownstreamActions: []collectionOrderedRootDownstreamAction{
				collectionOrderedRootActionRequires3023Correctness,
				collectionOrderedRootActionRequires3024OutputOwnership,
			},
			RootGroups:        "zero or multiple existing overlays force BaseRoot=0 and include deleted markers",
			ProductionSupport: "zero-base overlay publish remains a deterministic cold-build classification until #3023/#3024 reclassify it",
			Covers:            []string{"overlay_flush", "overlay_cold_build_zero_base", "buffered_flush"},
		},
		{
			ID:               "overlay-compaction-maintenance",
			PublicAnchors:    []collectionOrderedRootRouteAnchor{collectionRouteCollectionCompactRootOverlays},
			InternalAnchors:  []collectionOrderedRootRouteAnchor{collectionRouteCompactRootOverlaysLocked, collectionRouteBuildOverlayDescriptorSystemDelta},
			DBPublishAnchors: []collectionOrderedRootRouteAnchor{collectionRouteDBPublishOrderedRootDeltaGroup},
			ExpectedOutcome:  collectionOrderedRootOutcomeOverlayCompaction,
			DownstreamActions: []collectionOrderedRootDownstreamAction{
				collectionOrderedRootActionImplementedCurrentSerialOnly,
				collectionOrderedRootActionEligibleFor3022Observability,
			},
			RootGroups:        "overlay root names are folded into base roots and overlay descriptors are cleared in one system commit",
			ProductionSupport: "current maintenance compaction is serial ordered-root delta group publish, not span-native output",
			Covers:            []string{"overlay_compaction", "overlay_flush", "non_command_wal_publish"},
		},
		{
			ID:              "insert-non-command-wal-root-groups",
			PublicAnchors:   []collectionOrderedRootRouteAnchor{collectionRouteCollectionInsertBatch},
			InternalAnchors: []collectionOrderedRootRouteAnchor{collectionRouteInsertBatchPlanRootNamesAndBaseIDs, collectionRouteBuildRootDescriptorSystemDeltaIterator},
			DBPublishAnchors: []collectionOrderedRootRouteAnchor{
				collectionRouteDBPublishOrderedRootDeltaGroup,
			},
			ExpectedOutcome: collectionOrderedRootOutcomeNonCommandWALDeltaGroup,
			DownstreamActions: []collectionOrderedRootDownstreamAction{
				collectionOrderedRootActionEligibleFor3022Observability,
				collectionOrderedRootActionRequires3023Correctness,
			},
			RootGroups:        "primary plus any secondary, text, vector, or index-state roots in the insert plan",
			ProductionSupport: "foreground non-command-WAL insert publish is current serial behavior; #3022/#3023 must classify span-native route changes",
			Covers:            []string{"non_command_wal_publish", "insert_root_groups", "multi_index_roots"},
		},
		{
			ID:              "insert-command-wal-root-groups",
			PublicAnchors:   []collectionOrderedRootRouteAnchor{collectionRouteCollectionInsertBatch},
			InternalAnchors: []collectionOrderedRootRouteAnchor{collectionRouteCommandWALActive, collectionRouteWithCommandWALPublishCoordinator, collectionRouteNewInsertCommandWALIntent},
			DBPublishAnchors: []collectionOrderedRootRouteAnchor{
				collectionRouteDBPublishOrderedRootDeltaGroupCommandWAL,
			},
			ExpectedOutcome: collectionOrderedRootOutcomeCommandWALDeltaGroup,
			DownstreamActions: []collectionOrderedRootDownstreamAction{
				collectionOrderedRootActionEligibleFor3022Observability,
				collectionOrderedRootActionRequires3023Correctness,
			},
			RootGroups:        "same insert root group as non-command-WAL, with command intent appended before ordered-root publish",
			ProductionSupport: "command-WAL insert publish is supported; span-native eligibility must preserve command-WAL ordering",
			Covers:            []string{"command_wal_publish", "insert_root_groups", "multi_index_roots"},
		},
		{
			ID:              "delete-non-command-wal-root-groups",
			PublicAnchors:   []collectionOrderedRootRouteAnchor{collectionRouteCollectionDeleteDocument, collectionRouteCollectionDeleteBatch},
			InternalAnchors: []collectionOrderedRootRouteAnchor{collectionRouteBuildRootDeltaBatchInputsFromTables, collectionRouteBuildRootDescriptorSystemDeltaIterator},
			DBPublishAnchors: []collectionOrderedRootRouteAnchor{
				collectionRouteDBPublishOrderedRootDeltaBatch,
			},
			ExpectedOutcome: collectionOrderedRootOutcomeNonCommandWALDeltaBatch,
			DownstreamActions: []collectionOrderedRootDownstreamAction{
				collectionOrderedRootActionRequires3023Correctness,
				collectionOrderedRootActionRequires3024OutputOwnership,
			},
			RootGroups:        "primary tombstones plus secondary, text, index-state, and retained-column reclaim roots when present",
			ProductionSupport: "delete root groups are supported through ordered-root delta batch publish; #3023/#3024 gate span-native enablement",
			Covers:            []string{"delete_root_groups", "non_command_wal_publish", "multi_index_roots"},
		},
		{
			ID:              "delete-command-wal-root-groups",
			PublicAnchors:   []collectionOrderedRootRouteAnchor{collectionRouteCollectionDeleteDocument, collectionRouteCollectionDeleteBatch},
			InternalAnchors: []collectionOrderedRootRouteAnchor{collectionRouteCommandWALActive, collectionRouteWithCommandWALPublishCoordinator, collectionRouteNewDeleteCommandWALIntent},
			DBPublishAnchors: []collectionOrderedRootRouteAnchor{
				collectionRouteDBPublishOrderedRootDeltaBatchCommandWAL,
			},
			ExpectedOutcome: collectionOrderedRootOutcomeCommandWALDeltaBatch,
			DownstreamActions: []collectionOrderedRootDownstreamAction{
				collectionOrderedRootActionRequires3023Correctness,
				collectionOrderedRootActionRequires3024OutputOwnership,
			},
			RootGroups:        "same delete root group as non-command-WAL, covered by a command-WAL delete intent",
			ProductionSupport: "command-WAL delete publish is supported; span-native eligibility must preserve command-WAL ordering and batch atomicity",
			Covers:            []string{"delete_root_groups", "command_wal_publish", "multi_index_roots"},
		},
		{
			ID:              "update-non-command-wal-root-groups",
			PublicAnchors:   []collectionOrderedRootRouteAnchor{collectionRouteCollectionUpdate, collectionRouteCollectionUpdateBatch},
			InternalAnchors: []collectionOrderedRootRouteAnchor{collectionRoutePublishUpdateBatchPlanLocked, collectionRouteCoalesceCollectionRootDeltaTables, collectionRouteBuildRootDeltaBatchInputsFromTables},
			DBPublishAnchors: []collectionOrderedRootRouteAnchor{
				collectionRouteDBPublishOrderedRootDeltaBatchPreflight,
			},
			ExpectedOutcome: collectionOrderedRootOutcomeNonCommandWALDeltaBatch,
			DownstreamActions: []collectionOrderedRootDownstreamAction{
				collectionOrderedRootActionRequires3023Correctness,
				collectionOrderedRootActionRequires3024OutputOwnership,
			},
			RootGroups:        "coalesced primary, secondary, unique, text, vector, and index-state update deltas",
			ProductionSupport: "non-command-WAL update publish is supported with preflight validation; #3023/#3024 gate span-native enablement",
			Covers:            []string{"update_root_groups", "non_command_wal_publish", "multi_index_roots"},
		},
		{
			ID:              "update-command-wal-root-groups",
			PublicAnchors:   []collectionOrderedRootRouteAnchor{collectionRouteCollectionUpdate, collectionRouteCollectionUpdateBatch},
			InternalAnchors: []collectionOrderedRootRouteAnchor{collectionRouteCommandWALActive, collectionRouteWithCommandWALPublishCoordinator, collectionRouteNewUpdateCommandWALIntent},
			DBPublishAnchors: []collectionOrderedRootRouteAnchor{
				collectionRouteDBPublishOrderedRootDeltaBatchPreflightCommandWAL,
			},
			ExpectedOutcome: collectionOrderedRootOutcomeCommandWALDeltaBatch,
			DownstreamActions: []collectionOrderedRootDownstreamAction{
				collectionOrderedRootActionRequires3023Correctness,
				collectionOrderedRootActionRequires3024OutputOwnership,
			},
			RootGroups:        "same update root group as non-command-WAL, covered by a command-WAL update intent",
			ProductionSupport: "command-WAL update publish is supported; span-native eligibility must preserve command-WAL ordering and preflight semantics",
			Covers:            []string{"update_root_groups", "command_wal_publish", "multi_index_roots"},
		},
		{
			ID:              "multi-index-root-group-inventory",
			PublicAnchors:   []collectionOrderedRootRouteAnchor{collectionRouteCollectionManagerCreateCollection, collectionRouteCollectionInsertBatch, collectionRouteCollectionUpdateBatch, collectionRouteCollectionDeleteBatch},
			InternalAnchors: []collectionOrderedRootRouteAnchor{collectionRouteInsertBatchPlanPublishRootRuns, collectionRouteInsertBatchPlanRootNamesAndBaseIDs, collectionRouteBuildCollectionRootOverlayFilters},
			DBPublishAnchors: []collectionOrderedRootRouteAnchor{
				collectionRouteDBPublishOrderedRootGroup,
				collectionRouteDBPublishOrderedRootDeltaGroup,
				collectionRouteDBPublishOrderedRootDeltaBatch,
			},
			ExpectedOutcome: collectionOrderedRootOutcomeMultiIndexRootGroups,
			DownstreamActions: []collectionOrderedRootDownstreamAction{
				collectionOrderedRootActionEligibleFor3022Observability,
				collectionOrderedRootActionRequires3023Correctness,
			},
			RootGroups:        "collection primary, secondary, index-state, text, vector, unique-value, and overlay root descriptors",
			ProductionSupport: "multi-index root group inventory is supported by current serial/group publish and must remain explicit for #3022/#3023",
			Covers:            []string{"multi_index_roots", "secondary_index_roots", "insert_root_groups", "update_root_groups", "delete_root_groups"},
		},
	}
}

func TestCollectionOrderedRootRouteClassificationCoversPublicSurfaces(t *testing.T) {
	required := map[string]bool{
		"buffered_flush":               false,
		"overlay_flush":                false,
		"overlay_compaction":           false,
		"command_wal_publish":          false,
		"non_command_wal_publish":      false,
		"insert_root_groups":           false,
		"delete_root_groups":           false,
		"update_root_groups":           false,
		"multi_index_roots":            false,
		"secondary_index_roots":        false,
		"primary_root":                 false,
		"overlay_warm_single_base":     false,
		"overlay_cold_build_zero_base": false,
	}

	rows := collectionOrderedRootRouteClassificationRows()
	if len(rows) == 0 {
		t.Fatal("collection ordered-root route classification matrix is empty")
	}
	seenIDs := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		validateCollectionOrderedRootRouteClassificationRow(t, row, seenIDs)
		for _, token := range row.Covers {
			if _, ok := required[token]; !ok {
				t.Fatalf("collection ordered-root row %q has unregistered coverage token %q", row.ID, token)
			}
			required[token] = true
		}
	}
	for token, seen := range required {
		if !seen {
			t.Fatalf("collection ordered-root route classification missing coverage token %q", token)
		}
	}
}

func TestCollectionOrderedRootOverlayDeltaBaseClassification(t *testing.T) {
	warm := collectionOrderedRootRouteClassificationRowByID(t, "indexed-overlay-single-base-warm-flush")
	if warm.ExpectedOutcome != collectionOrderedRootOutcomeOverlayWarmSingleBase {
		t.Fatalf("warm overlay row outcome=%q want %q", warm.ExpectedOutcome, collectionOrderedRootOutcomeOverlayWarmSingleBase)
	}
	cold := collectionOrderedRootRouteClassificationRowByID(t, "indexed-overlay-zero-base-cold-build-flush")
	if cold.ExpectedOutcome != collectionOrderedRootOutcomeOverlayColdBuildZeroBase {
		t.Fatalf("cold overlay row outcome=%q want %q", cold.ExpectedOutcome, collectionOrderedRootOutcomeOverlayColdBuildZeroBase)
	}

	if got := overlayDeltaBaseRoot(nil); got != 0 {
		t.Fatalf("overlayDeltaBaseRoot(nil)=%d want 0 cold-build base", got)
	}
	if got := overlayDeltaBaseRoot([]uint64{17}); got != 17 {
		t.Fatalf("overlayDeltaBaseRoot(single)=%d want warm base 17", got)
	}
	if got := overlayDeltaBaseRoot([]uint64{17, 23}); got != 0 {
		t.Fatalf("overlayDeltaBaseRoot(multiple)=%d want 0 cold-build base", got)
	}
}

func validateCollectionOrderedRootRouteClassificationRow(t *testing.T, row collectionOrderedRootRouteClassificationRow, seenIDs map[string]struct{}) {
	t.Helper()
	if row.ID == "" {
		t.Fatalf("collection ordered-root row has empty ID: %+v", row)
	}
	if _, exists := seenIDs[row.ID]; exists {
		t.Fatalf("duplicate collection ordered-root row ID %q", row.ID)
	}
	seenIDs[row.ID] = struct{}{}
	for field, value := range map[string]string{
		"expected_outcome":   string(row.ExpectedOutcome),
		"root_groups":        row.RootGroups,
		"production_support": row.ProductionSupport,
	} {
		if strings.TrimSpace(value) == "" {
			t.Fatalf("collection ordered-root row %q missing %s", row.ID, field)
		}
		lower := strings.ToLower(value)
		if strings.Contains(lower, "unknown") || strings.Contains(lower, "implicit disabled") {
			t.Fatalf("collection ordered-root row %q has unclassified %s=%q", row.ID, field, value)
		}
	}
	if _, ok := collectionOrderedRootExpectedOutcomes[row.ExpectedOutcome]; !ok {
		t.Fatalf("collection ordered-root row %q has unsupported expected outcome %q", row.ID, row.ExpectedOutcome)
	}
	validateCollectionOrderedRootAnchors(t, row.ID, "public", row.PublicAnchors)
	validateCollectionOrderedRootAnchors(t, row.ID, "internal", row.InternalAnchors)
	validateCollectionOrderedRootAnchors(t, row.ID, "db_publish", row.DBPublishAnchors)
	if len(row.DownstreamActions) == 0 {
		t.Fatalf("collection ordered-root row %q has no downstream action", row.ID)
	}
	seenActions := make(map[collectionOrderedRootDownstreamAction]struct{}, len(row.DownstreamActions))
	for _, action := range row.DownstreamActions {
		if _, ok := collectionOrderedRootDownstreamActions[action]; !ok {
			t.Fatalf("collection ordered-root row %q has unsupported downstream action %q", row.ID, action)
		}
		if _, exists := seenActions[action]; exists {
			t.Fatalf("collection ordered-root row %q repeats downstream action %q", row.ID, action)
		}
		seenActions[action] = struct{}{}
	}
	if len(row.Covers) == 0 {
		t.Fatalf("collection ordered-root row %q has no coverage tokens", row.ID)
	}
}

func validateCollectionOrderedRootAnchors(t *testing.T, rowID, field string, anchors []collectionOrderedRootRouteAnchor) {
	t.Helper()
	if len(anchors) == 0 {
		t.Fatalf("collection ordered-root row %q has no %s anchors", rowID, field)
	}
	seen := make(map[collectionOrderedRootRouteAnchor]struct{}, len(anchors))
	for _, anchor := range anchors {
		if _, ok := collectionOrderedRootRouteAnchorEvidence[anchor]; !ok {
			t.Fatalf("collection ordered-root row %q has unsupported %s anchor %q", rowID, field, anchor)
		}
		if _, exists := seen[anchor]; exists {
			t.Fatalf("collection ordered-root row %q repeats %s anchor %q", rowID, field, anchor)
		}
		seen[anchor] = struct{}{}
	}
}

func collectionOrderedRootRouteClassificationRowByID(t *testing.T, id string) collectionOrderedRootRouteClassificationRow {
	t.Helper()
	for _, row := range collectionOrderedRootRouteClassificationRows() {
		if row.ID == id {
			return row
		}
	}
	t.Fatalf("missing collection ordered-root route classification row %q", id)
	return collectionOrderedRootRouteClassificationRow{}
}
