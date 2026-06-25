package db

import (
	"strconv"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

const (
	orderedRootPublishStatusImplemented                 = "implemented"
	orderedRootPublishStatusDeterministicFallbackPrefix = "deterministic_fallback:"
	orderedRootPublishStatusBlockedByPrefix             = "blocked_by:"
)

type orderedRootPublishRouteAnchor string

const (
	orderedRootRouteDBPublishOrderedRootIterator                                               orderedRootPublishRouteAnchor = "TreeDB/db.(*DB).PublishOrderedRootIterator"
	orderedRootRouteDBPublishOrderedRootGroup                                                  orderedRootPublishRouteAnchor = "TreeDB/db.(*DB).PublishOrderedRootGroup"
	orderedRootRouteDBPublishOrderedRootGroupWithSystemBuilder                                 orderedRootPublishRouteAnchor = "TreeDB/db.(*DB).PublishOrderedRootGroupWithSystemBuilder"
	orderedRootRouteDBPublishOrderedRootDeltaGroupWithSystemBuilder                            orderedRootPublishRouteAnchor = "TreeDB/db.(*DB).PublishOrderedRootDeltaGroupWithSystemBuilder"
	orderedRootRouteDBPublishOrderedRootDeltaGroupWithSystemDeltaBuilder                       orderedRootPublishRouteAnchor = "TreeDB/db.(*DB).PublishOrderedRootDeltaGroupWithSystemDeltaBuilder"
	orderedRootRouteDBPublishOrderedRootDeltaGroupWithCommandWALContextAndSystemDeltaBuilder   orderedRootPublishRouteAnchor = "TreeDB/db.(*DB).PublishOrderedRootDeltaGroupWithCommandWALContextAndSystemDeltaBuilder"
	orderedRootRouteDBPublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder                  orderedRootPublishRouteAnchor = "TreeDB/db.(*DB).PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder"
	orderedRootRouteDBPublishOrderedRootDeltaBatchGroupWithCommandWALAndSystemDeltaBuilder     orderedRootPublishRouteAnchor = "TreeDB/db.(*DB).PublishOrderedRootDeltaBatchGroupWithCommandWALAndSystemDeltaBuilder"
	orderedRootRouteDBPublishOrderedRootDeltaBatchGroupWithPreflightAndSystemDeltaBuilder      orderedRootPublishRouteAnchor = "TreeDB/db.(*DB).PublishOrderedRootDeltaBatchGroupWithPreflightAndSystemDeltaBuilder"
	orderedRootRouteDBPublishOrderedRootDeltaBatchGroupWithPreflightCommandWALAndSystemBuilder orderedRootPublishRouteAnchor = "TreeDB/db.(*DB).PublishOrderedRootDeltaBatchGroupWithPreflightCommandWALAndSystemDeltaBuilder"
	orderedRootRouteDBOrderedRootDeltaBatchApplyOptions                                        orderedRootPublishRouteAnchor = "TreeDB/db.(*DB).orderedRootDeltaBatchApplyOptions"
	orderedRootRouteDBPublishOrderedRootDeltaBatchWithAllocator                                orderedRootPublishRouteAnchor = "TreeDB/db.(*DB).publishOrderedRootDeltaBatchWithAllocator"
	orderedRootRouteDBRunOrderedRootDeltaBatchReadOnlyPrepare                                  orderedRootPublishRouteAnchor = "TreeDB/db.(*DB).runOrderedRootDeltaBatchReadOnlyPrepare"
	orderedRootRouteDBTryPublishOrderedRootDeltaBatchGroupOptimistic                           orderedRootPublishRouteAnchor = "TreeDB/db.(*DB).tryPublishOrderedRootDeltaBatchGroupOptimistic"
	orderedRootRouteDBRejectUnloggedCommandWALRootPublish                                      orderedRootPublishRouteAnchor = "TreeDB/db.(*DB).rejectUnloggedCommandWALRootPublish"
	orderedRootRouteOrderedRootDeltaBatchPublishInput                                          orderedRootPublishRouteAnchor = "TreeDB/db.OrderedRootDeltaBatchPublishInput"
	orderedRootRouteOrderedRootStoragePagerLeaves                                              orderedRootPublishRouteAnchor = "TreeDB/db.OrderedRootStoragePagerLeaves"
	orderedRootRouteOrderedRootStorageValueLogLeaves                                           orderedRootPublishRouteAnchor = "TreeDB/db.OrderedRootStorageValueLogLeaves"
	orderedRootRouteFlushAdmissionPolicyAuto                                                   orderedRootPublishRouteAnchor = "TreeDB/db.FlushAdmissionPolicyAuto"
)

var orderedRootPublishRouteAnchorEvidence = map[orderedRootPublishRouteAnchor]any{
	orderedRootRouteDBPublishOrderedRootIterator:                                               (*DB).PublishOrderedRootIterator,
	orderedRootRouteDBPublishOrderedRootGroup:                                                  (*DB).PublishOrderedRootGroup,
	orderedRootRouteDBPublishOrderedRootGroupWithSystemBuilder:                                 (*DB).PublishOrderedRootGroupWithSystemBuilder,
	orderedRootRouteDBPublishOrderedRootDeltaGroupWithSystemBuilder:                            (*DB).PublishOrderedRootDeltaGroupWithSystemBuilder,
	orderedRootRouteDBPublishOrderedRootDeltaGroupWithSystemDeltaBuilder:                       (*DB).PublishOrderedRootDeltaGroupWithSystemDeltaBuilder,
	orderedRootRouteDBPublishOrderedRootDeltaGroupWithCommandWALContextAndSystemDeltaBuilder:   (*DB).PublishOrderedRootDeltaGroupWithCommandWALContextAndSystemDeltaBuilder,
	orderedRootRouteDBPublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder:                  (*DB).PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder,
	orderedRootRouteDBPublishOrderedRootDeltaBatchGroupWithCommandWALAndSystemDeltaBuilder:     (*DB).PublishOrderedRootDeltaBatchGroupWithCommandWALAndSystemDeltaBuilder,
	orderedRootRouteDBPublishOrderedRootDeltaBatchGroupWithPreflightAndSystemDeltaBuilder:      (*DB).PublishOrderedRootDeltaBatchGroupWithPreflightAndSystemDeltaBuilder,
	orderedRootRouteDBPublishOrderedRootDeltaBatchGroupWithPreflightCommandWALAndSystemBuilder: (*DB).PublishOrderedRootDeltaBatchGroupWithPreflightCommandWALAndSystemDeltaBuilder,
	orderedRootRouteDBOrderedRootDeltaBatchApplyOptions:                                        (*DB).orderedRootDeltaBatchApplyOptions,
	orderedRootRouteDBPublishOrderedRootDeltaBatchWithAllocator:                                (*DB).publishOrderedRootDeltaBatchWithAllocator,
	orderedRootRouteDBRunOrderedRootDeltaBatchReadOnlyPrepare:                                  (*DB).runOrderedRootDeltaBatchReadOnlyPrepare,
	orderedRootRouteDBTryPublishOrderedRootDeltaBatchGroupOptimistic:                           (*DB).tryPublishOrderedRootDeltaBatchGroupOptimistic,
	orderedRootRouteDBRejectUnloggedCommandWALRootPublish:                                      (*DB).rejectUnloggedCommandWALRootPublish,
	orderedRootRouteOrderedRootDeltaBatchPublishInput:                                          OrderedRootDeltaBatchPublishInput{},
	orderedRootRouteOrderedRootStoragePagerLeaves:                                              OrderedRootStoragePagerLeaves,
	orderedRootRouteOrderedRootStorageValueLogLeaves:                                           OrderedRootStorageValueLogLeaves,
	orderedRootRouteFlushAdmissionPolicyAuto:                                                   FlushAdmissionPolicyAuto,
}

type orderedRootPublishExpectedOutcome string

const (
	orderedRootOutcomeCurrentSerialPublish            orderedRootPublishExpectedOutcome = "current-serial-publish"
	orderedRootOutcomeCurrentSerializedGroupPublish   orderedRootPublishExpectedOutcome = "current-serialized-group-publish"
	orderedRootOutcomeCurrentOptimisticGroupPublish   orderedRootPublishExpectedOutcome = "current-optimistic-group-publish"
	orderedRootOutcomeCurrentCommandWALCoveredPublish orderedRootPublishExpectedOutcome = "current-command-wal-covered-publish"
	orderedRootOutcomeCurrentFailClosed               orderedRootPublishExpectedOutcome = "current-fail-closed"
	orderedRootOutcomePolicyBlocked                   orderedRootPublishExpectedOutcome = "policy-blocked"
	orderedRootOutcomeCurrentReadOnlyPrepareOnly      orderedRootPublishExpectedOutcome = "current-read-only-prepare-only"
	orderedRootOutcomeBlanketNonSpanNativeFallback    orderedRootPublishExpectedOutcome = "current-blanket-non-span-native-fallback"
	orderedRootOutcomeCurrentEmptyNoop                orderedRootPublishExpectedOutcome = "current-empty-noop"
	orderedRootOutcomeCurrentStoragePolicy            orderedRootPublishExpectedOutcome = "current-storage-policy"
	orderedRootOutcomeCurrentCollectionRoute          orderedRootPublishExpectedOutcome = "current-collection-route"
	orderedRootOutcomeCurrentWarmOverlaySingleBase    orderedRootPublishExpectedOutcome = "current-warm-overlay-single-base"
	orderedRootOutcomeCurrentColdBuildZeroBase        orderedRootPublishExpectedOutcome = "current-cold-build-zero-base"
	orderedRootOutcomeCurrentSnapshotVisibility       orderedRootPublishExpectedOutcome = "current-snapshot-visibility"
	orderedRootOutcomeRawRouteBlocked                 orderedRootPublishExpectedOutcome = "raw-route-blocked"
)

type orderedRootPublishDownstreamAction string

const (
	orderedRootActionImplementedCurrentSerialOnly   orderedRootPublishDownstreamAction = "implemented-current-serial-only"
	orderedRootActionEligibleFor3022Observability   orderedRootPublishDownstreamAction = "eligible-for-#3022-observability"
	orderedRootActionRequires3023Correctness        orderedRootPublishDownstreamAction = "requires-#3023-correctness"
	orderedRootActionRequires3024OutputOwnership    orderedRootPublishDownstreamAction = "requires-#3024-output-ownership"
	orderedRootActionBlockedBy3032                  orderedRootPublishDownstreamAction = "blocked-by-#3032"
	orderedRootActionBlockedBy3033                  orderedRootPublishDownstreamAction = "blocked-by-#3033"
	orderedRootActionImplementedCurrentFailClosed   orderedRootPublishDownstreamAction = "implemented-current-fail-closed"
	orderedRootActionImplementedCurrentReadOnlyOnly orderedRootPublishDownstreamAction = "implemented-current-read-only-only"
)

var orderedRootExpectedOutcomes = map[orderedRootPublishExpectedOutcome]struct{}{
	orderedRootOutcomeCurrentSerialPublish:            {},
	orderedRootOutcomeCurrentSerializedGroupPublish:   {},
	orderedRootOutcomeCurrentOptimisticGroupPublish:   {},
	orderedRootOutcomeCurrentCommandWALCoveredPublish: {},
	orderedRootOutcomeCurrentFailClosed:               {},
	orderedRootOutcomePolicyBlocked:                   {},
	orderedRootOutcomeCurrentReadOnlyPrepareOnly:      {},
	orderedRootOutcomeBlanketNonSpanNativeFallback:    {},
	orderedRootOutcomeCurrentEmptyNoop:                {},
	orderedRootOutcomeCurrentStoragePolicy:            {},
	orderedRootOutcomeCurrentCollectionRoute:          {},
	orderedRootOutcomeCurrentWarmOverlaySingleBase:    {},
	orderedRootOutcomeCurrentColdBuildZeroBase:        {},
	orderedRootOutcomeCurrentSnapshotVisibility:       {},
	orderedRootOutcomeRawRouteBlocked:                 {},
}

var orderedRootDownstreamActions = map[orderedRootPublishDownstreamAction]struct{}{
	orderedRootActionImplementedCurrentSerialOnly:   {},
	orderedRootActionEligibleFor3022Observability:   {},
	orderedRootActionRequires3023Correctness:        {},
	orderedRootActionRequires3024OutputOwnership:    {},
	orderedRootActionBlockedBy3032:                  {},
	orderedRootActionBlockedBy3033:                  {},
	orderedRootActionImplementedCurrentFailClosed:   {},
	orderedRootActionImplementedCurrentReadOnlyOnly: {},
}

type orderedRootSpanNativeClassificationRow struct {
	ID                string
	Category          string
	Route             string
	RouteAnchors      []orderedRootPublishRouteAnchor
	ExpectedOutcome   orderedRootPublishExpectedOutcome
	DownstreamActions []orderedRootPublishDownstreamAction
	Mode              string
	Semantics         string
	Storage           string
	Durability        string
	Status            string
	ProductionSupport string
	Covers            []string
}

func orderedRootSpanNativeClassificationRows() []orderedRootSpanNativeClassificationRow {
	return []orderedRootSpanNativeClassificationRow{
		{
			ID:                "direct-iterator-publish-cold-warm",
			Category:          "direct publisher routes",
			Route:             "PublishOrderedRootIterator",
			RouteAnchors:      []orderedRootPublishRouteAnchor{orderedRootRouteDBPublishOrderedRootIterator},
			ExpectedOutcome:   orderedRootOutcomeCurrentSerialPublish,
			DownstreamActions: []orderedRootPublishDownstreamAction{orderedRootActionImplementedCurrentSerialOnly, orderedRootActionEligibleFor3022Observability},
			Mode:              "serial apply",
			Semantics:         "point inserts and overwrites through ordered iterators",
			Storage:           "root-local inline or DB default leaf policy",
			Durability:        "checkpoint, reopen, snapshot read visibility",
			Status:            orderedRootPublishStatusImplemented,
			ProductionSupport: "current serial publisher is supported; it remains serial-only unless #3022 observability identifies a later span-native candidate",
			Covers: []string{
				"direct_batch_publish", "serial_apply", "point_insert", "overwrite",
				"inline_leaf_output", "root_local_policy", "checkpoint", "reopen", "snapshot_read_visibility",
			},
		},
		{
			ID:       "grouped-full-root-publish",
			Category: "direct publisher routes",
			Route:    "PublishOrderedRootGroup and PublishOrderedRootGroupWithSystemBuilder",
			RouteAnchors: []orderedRootPublishRouteAnchor{
				orderedRootRouteDBPublishOrderedRootGroup,
				orderedRootRouteDBPublishOrderedRootGroupWithSystemBuilder,
			},
			ExpectedOutcome:   orderedRootOutcomeCurrentSerializedGroupPublish,
			DownstreamActions: []orderedRootPublishDownstreamAction{orderedRootActionImplementedCurrentSerialOnly, orderedRootActionEligibleFor3022Observability},
			Mode:              "serialized group publish",
			Semantics:         "full-root grouped publication with system descriptor publish",
			Storage:           "per-root storage policy",
			Durability:        "one backend commit preserves system-root identity",
			Status:            orderedRootPublishStatusImplemented,
			ProductionSupport: "current grouped full-root publish is supported for ordered roots and descriptors; span-native enablement is not implied by this serial route",
			Covers: []string{
				"grouped_publish", "serialized_group_publish", "system_delta_builder_publish",
				"non_command_wal_system_publish", "multi_index_root_groups", "system_root_identity",
			},
		},
		{
			ID:                "serialized-delta-group-publish",
			Category:          "direct publisher routes",
			Route:             "PublishOrderedRootDeltaGroupWithSystemDeltaBuilder",
			RouteAnchors:      []orderedRootPublishRouteAnchor{orderedRootRouteDBPublishOrderedRootDeltaGroupWithSystemDeltaBuilder},
			ExpectedOutcome:   orderedRootOutcomeCurrentSerialPublish,
			DownstreamActions: []orderedRootPublishDownstreamAction{orderedRootActionRequires3023Correctness},
			Mode:              "serial apply",
			Semantics:         "warm root-local iterator deltas preserve omitted base entries",
			Storage:           "per-root storage policy",
			Durability:        "system root and ordered roots commit together",
			Status:            orderedRootPublishStatusImplemented,
			ProductionSupport: "current iterator-delta publish is supported as serial ordered-root behavior; span-native enablement requires #3023 correctness coverage",
			Covers: []string{
				"serialized_group_publish", "point_insert", "overwrite", "delete_tombstone",
				"mixed_update_delete_batch", "system_root_identity",
			},
		},
		{
			ID:       "optimistic-delta-batch-group-publish",
			Category: "direct publisher routes",
			Route:    "PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder",
			RouteAnchors: []orderedRootPublishRouteAnchor{
				orderedRootRouteDBPublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder,
				orderedRootRouteDBTryPublishOrderedRootDeltaBatchGroupOptimistic,
			},
			ExpectedOutcome:   orderedRootOutcomeCurrentOptimisticGroupPublish,
			DownstreamActions: []orderedRootPublishDownstreamAction{orderedRootActionRequires3023Correctness, orderedRootActionRequires3024OutputOwnership},
			Mode:              "optimistic group publish with optional root-level parallel apply",
			Semantics:         "materialized batch deltas publish roots before guarded final commit",
			Storage:           "per-root storage policy",
			Durability:        "root mismatch retries through serialized fallback",
			Status:            orderedRootPublishStatusImplemented,
			ProductionSupport: "current optimistic root-group publish is supported; span-native leaf apply requires #3023 correctness and #3024 output ownership",
			Covers: []string{
				"optimistic_group_publish", "parallel_root_apply", "duplicate_same_key_overwrite",
				"output_ownership", "snapshot_read_visibility",
			},
		},
		{
			ID:                "command-wal-context-publish",
			Category:          "system-root routes",
			Route:             "PublishOrderedRootDeltaGroupWithCommandWALContextAndSystemDeltaBuilder",
			RouteAnchors:      []orderedRootPublishRouteAnchor{orderedRootRouteDBPublishOrderedRootDeltaGroupWithCommandWALContextAndSystemDeltaBuilder},
			ExpectedOutcome:   orderedRootOutcomeCurrentCommandWALCoveredPublish,
			DownstreamActions: []orderedRootPublishDownstreamAction{orderedRootActionRequires3023Correctness, orderedRootActionEligibleFor3022Observability},
			Mode:              "WAL-enabled command publish",
			Semantics:         "command-WAL LSN is assigned before system delta builder",
			Storage:           "per-root storage policy",
			Durability:        "command-WAL ordering and system-root identity",
			Status:            orderedRootPublishStatusImplemented,
			ProductionSupport: "current command-WAL covered ordered-root publish is supported with fail-closed poisoning; span-native enablement requires #3023 and #3022 observability",
			Covers: []string{
				"command_wal_system_publish", "wal_enabled", "command_wal_ordering", "system_root_identity",
			},
		},
		{
			ID:                "unlogged-publish-in-command-wal-mode",
			Category:          "system-root routes",
			Route:             "ordinary non-command-WAL ordered-root publish on command-WAL DB",
			RouteAnchors:      []orderedRootPublishRouteAnchor{orderedRootRouteDBRejectUnloggedCommandWALRootPublish},
			ExpectedOutcome:   orderedRootOutcomeCurrentFailClosed,
			DownstreamActions: []orderedRootPublishDownstreamAction{orderedRootActionImplementedCurrentFailClosed},
			Mode:              "WAL-enabled rejection",
			Semantics:         "uncovered logical system changes cannot bypass command-WAL",
			Storage:           "per-root storage policy",
			Durability:        "command-WAL ordering barrier",
			Status:            orderedRootPublishStatusDeterministicFallbackPrefix + "command_wal_barrier",
			ProductionSupport: "supported fail-closed rejection; caller must use command-WAL covered publish APIs",
			Covers: []string{
				"non_command_wal_system_publish", "wal_enabled", "command_wal_ordering",
			},
		},
		{
			ID:                "wal-disabled-cached-mode-admission",
			Category:          "execution modes",
			Route:             "cached ordered-root collection publish with WAL disabled",
			RouteAnchors:      []orderedRootPublishRouteAnchor{orderedRootRouteFlushAdmissionPolicyAuto},
			ExpectedOutcome:   orderedRootOutcomePolicyBlocked,
			DownstreamActions: []orderedRootPublishDownstreamAction{orderedRootActionBlockedBy3032},
			Mode:              "WAL-disabled cached mode",
			Semantics:         "ordered-root behavior depends on resolved default admission decision",
			Storage:           "persistent value-log remains durable storage",
			Durability:        "WAL-off durability posture is owned by admission policy",
			Status:            orderedRootPublishStatusBlockedByPrefix + "#3032",
			ProductionSupport: "production support waits for #3032 resolved admission and rollback contract",
			Covers: []string{
				"wal_disabled_cached_mode", "admission_policy_dependency_3032", "depends_3032",
			},
		},
		{
			ID:       "read-only-prepare-only",
			Category: "execution modes",
			Route:    "OrderedRootDeltaBatchPublishInput.PrepareReadOnly",
			RouteAnchors: []orderedRootPublishRouteAnchor{
				orderedRootRouteOrderedRootDeltaBatchPublishInput,
				orderedRootRouteDBRunOrderedRootDeltaBatchReadOnlyPrepare,
			},
			ExpectedOutcome:   orderedRootOutcomeCurrentReadOnlyPrepareOnly,
			DownstreamActions: []orderedRootPublishDownstreamAction{orderedRootActionImplementedCurrentReadOnlyOnly, orderedRootActionEligibleFor3022Observability},
			Mode:              "read-only prepare",
			Semantics:         "side-effect-free leaf-span planning before warm apply",
			Storage:           "no durable output owned by prepare",
			Durability:        "planning must not publish roots or value-log pointers",
			Status:            orderedRootPublishStatusImplemented,
			ProductionSupport: "supported as diagnostics and planning only; it does not enable ordered-root span-native output",
			Covers: []string{
				"read_only_prepare", "output_ownership",
			},
		},
		{
			ID:       "ordered-root-blanket-non-span-native-fallback",
			Category: "execution modes",
			Route:    "orderedRootDeltaBatchApplyOptions",
			RouteAnchors: []orderedRootPublishRouteAnchor{
				orderedRootRouteDBOrderedRootDeltaBatchApplyOptions,
				orderedRootRouteDBPublishOrderedRootDeltaBatchWithAllocator,
			},
			ExpectedOutcome:   orderedRootOutcomeBlanketNonSpanNativeFallback,
			DownstreamActions: []orderedRootPublishDownstreamAction{orderedRootActionRequires3023Correctness, orderedRootActionRequires3024OutputOwnership},
			Mode:              "span-native prepare/apply",
			Semantics:         "ordered-root delta batches currently force non-span-native apply before reducer execution",
			Storage:           "prepared span-native output is not produced for ordered-root delta batches",
			Durability:        "no span-native reducer publish occurs; prior no-replacement-changed-root reducer failure is classified but not reproduced here",
			Status:            orderedRootPublishStatusDeterministicFallbackPrefix + FlushSpanRunFallbackSpanNativeNotImplemented.String(),
			ProductionSupport: "current support is blanket non-span-native fallback before reducer execution; #3023/#3024 must update this row before enablement",
			Covers: []string{
				"span_native_prepare", "prior_no_replacement_changed_root_guardrail", "output_ownership",
			},
		},
		{
			ID:                "empty-and-noop-deltas",
			Category:          "delta semantics",
			Route:             "publishOrderedRootDeltaBatchWithAllocator",
			RouteAnchors:      []orderedRootPublishRouteAnchor{orderedRootRouteDBPublishOrderedRootDeltaBatchWithAllocator},
			ExpectedOutcome:   orderedRootOutcomeCurrentEmptyNoop,
			DownstreamActions: []orderedRootPublishDownstreamAction{orderedRootActionRequires3023Correctness},
			Mode:              "serial apply",
			Semantics:         "empty deltas and unchanged roots preserve base root identity",
			Storage:           "no new leaf output",
			Durability:        "checkpoint/reopen observe existing root",
			Status:            orderedRootPublishStatusImplemented,
			ProductionSupport: "supported current behavior; no-op rows must remain explicit in #3023 span-native correctness enablement",
			Covers: []string{
				"empty_delta", "unchanged_noop_root", "checkpoint", "reopen",
			},
		},
		{
			ID:       "delete-tombstone-and-mixed-deltas",
			Category: "delta semantics",
			Route:    "PublishOrderedRootDeltaGroupWithSystemBuilder and batch variant",
			RouteAnchors: []orderedRootPublishRouteAnchor{
				orderedRootRouteDBPublishOrderedRootDeltaGroupWithSystemBuilder,
				orderedRootRouteDBPublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder,
			},
			ExpectedOutcome:   orderedRootOutcomeCurrentSerialPublish,
			DownstreamActions: []orderedRootPublishDownstreamAction{orderedRootActionRequires3023Correctness, orderedRootActionRequires3024OutputOwnership},
			Mode:              "serial apply",
			Semantics:         "deletes, tombstones, and mixed update/delete batches",
			Storage:           "per-root storage policy",
			Durability:        "value-log refs removed from replaced/deleted entries become GC candidates only after commit reachability changes",
			Status:            orderedRootPublishStatusImplemented,
			ProductionSupport: "supported serial behavior; span-native correctness remains gated by #3023 and output ownership by #3024",
			Covers: []string{
				"delete_tombstone", "mixed_update_delete_batch", "value_log_gc_safety",
			},
		},
		{
			ID:                "pager-inline-leaf-output",
			Category:          "storage/output policy",
			Route:             "OrderedRootStoragePagerLeaves",
			RouteAnchors:      []orderedRootPublishRouteAnchor{orderedRootRouteOrderedRootStoragePagerLeaves},
			ExpectedOutcome:   orderedRootOutcomeCurrentStoragePolicy,
			DownstreamActions: []orderedRootPublishDownstreamAction{orderedRootActionRequires3024OutputOwnership},
			Mode:              "serial apply",
			Semantics:         "inline/pager leaf output for ordered roots",
			Storage:           "pager leaves with root-local policy",
			Durability:        "checkpoint/reopen through index pages",
			Status:            orderedRootPublishStatusImplemented,
			ProductionSupport: "supported current output policy; span-native output ownership proof is #3024 work",
			Covers: []string{
				"inline_leaf_output", "root_local_policy", "checkpoint", "reopen",
			},
		},
		{
			ID:                "value-log-leaf-output",
			Category:          "storage/output policy",
			Route:             "OrderedRootStorageValueLogLeaves",
			RouteAnchors:      []orderedRootPublishRouteAnchor{orderedRootRouteOrderedRootStorageValueLogLeaves},
			ExpectedOutcome:   orderedRootOutcomeCurrentStoragePolicy,
			DownstreamActions: []orderedRootPublishDownstreamAction{orderedRootActionRequires3024OutputOwnership},
			Mode:              "serial apply",
			Semantics:         "ordered-root leaves stored as persistent leaf-log records",
			Storage:           "value-log leaf output",
			Durability:        "persistent pointer reachability and value-log GC safety",
			Status:            orderedRootPublishStatusImplemented,
			ProductionSupport: "supported as persistent value-log storage; #3024 span-native output ownership must preserve the same reachability rules",
			Covers: []string{
				"value_log_leaf_output", "persistent_vlog_pointer_reachability", "value_log_gc_safety", "checkpoint", "reopen",
			},
		},
		{
			ID:       "collection-buffered-roots",
			Category: "collection routes",
			Route:    "Collection buffered root publish",
			RouteAnchors: []orderedRootPublishRouteAnchor{
				orderedRootRouteDBPublishOrderedRootDeltaGroupWithSystemDeltaBuilder,
				orderedRootRouteDBPublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder,
			},
			ExpectedOutcome:   orderedRootOutcomeCurrentCollectionRoute,
			DownstreamActions: []orderedRootPublishDownstreamAction{orderedRootActionEligibleFor3022Observability, orderedRootActionRequires3023Correctness},
			Mode:              "ordered-root delta batch group",
			Semantics:         "primary buffered roots publish through ordered-root batch APIs",
			Storage:           "collection-selected root policy",
			Durability:        "collection metadata and roots commit together",
			Status:            orderedRootPublishStatusImplemented,
			ProductionSupport: "current buffered collection publish is supported; #3022 must observe route selection and #3023 gates span-native enablement",
			Covers: []string{
				"buffered_collection_roots", "collection_routes",
			},
		},
		{
			ID:       "collection-secondary-index-roots",
			Category: "collection routes",
			Route:    "secondary index root groups",
			RouteAnchors: []orderedRootPublishRouteAnchor{
				orderedRootRouteDBPublishOrderedRootGroup,
				orderedRootRouteDBPublishOrderedRootDeltaGroupWithSystemDeltaBuilder,
				orderedRootRouteDBPublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder,
			},
			ExpectedOutcome:   orderedRootOutcomeCurrentCollectionRoute,
			DownstreamActions: []orderedRootPublishDownstreamAction{orderedRootActionEligibleFor3022Observability, orderedRootActionRequires3023Correctness},
			Mode:              "multi-index ordered-root group",
			Semantics:         "secondary indexes publish alongside collection roots",
			Storage:           "collection-selected root policy",
			Durability:        "multi-index root group identity is persisted through system descriptors",
			Status:            orderedRootPublishStatusImplemented,
			ProductionSupport: "current secondary-index root publishing is supported via ordered-root groups; #3022 observes route choice and #3023 gates span-native enablement",
			Covers: []string{
				"secondary_index_roots", "multi_index_root_groups", "system_root_identity", "collection_routes",
			},
		},
		{
			ID:       "collection-overlay-single-base-warm-route",
			Category: "collection routes",
			Route:    "single-overlay ordered-root delta batch publish",
			RouteAnchors: []orderedRootPublishRouteAnchor{
				orderedRootRouteDBPublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder,
				orderedRootRouteDBPublishOrderedRootDeltaBatchWithAllocator,
			},
			ExpectedOutcome:   orderedRootOutcomeCurrentWarmOverlaySingleBase,
			DownstreamActions: []orderedRootPublishDownstreamAction{orderedRootActionRequires3023Correctness, orderedRootActionRequires3024OutputOwnership},
			Mode:              "warm overlay publish",
			Semantics:         "exactly one overlay can be used as the nonzero base root before publishing a newer overlay delta",
			Storage:           "collection-selected root policy",
			Durability:        "checkpoint/reopen preserve overlay identity",
			Status:            orderedRootPublishStatusImplemented,
			ProductionSupport: "supported current warm overlay behavior; #3023/#3024 must classify correctness and output ownership before span-native enablement",
			Covers: []string{
				"overlay_warm_single_base_route", "tombstone_semantics", "checkpoint", "reopen",
			},
		},
		{
			ID:       "collection-overlay-cold-build-zero-base-route",
			Category: "collection routes",
			Route:    "zero-base overlay/cold-build ordered-root routes",
			RouteAnchors: []orderedRootPublishRouteAnchor{
				orderedRootRouteDBPublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder,
				orderedRootRouteDBPublishOrderedRootDeltaBatchWithAllocator,
			},
			ExpectedOutcome:   orderedRootOutcomeCurrentColdBuildZeroBase,
			DownstreamActions: []orderedRootPublishDownstreamAction{orderedRootActionRequires3023Correctness, orderedRootActionRequires3024OutputOwnership},
			Mode:              "cold build publish",
			Semantics:         "zero overlays or multiple existing overlays use BaseRoot=0 and include deleted entries as cold-build input",
			Storage:           "collection-selected root policy",
			Durability:        "checkpoint/reopen preserve overlay identity",
			Status:            orderedRootPublishStatusDeterministicFallbackPrefix + FlushSpanRunFallbackColdBuild.String(),
			ProductionSupport: "supported current cold-build route; #3023/#3024 must update this row before span-native output is enabled",
			Covers: []string{
				"overlay_cold_build_routes", "tombstone_semantics", "checkpoint", "reopen",
			},
		},
		{
			ID:       "snapshot-and-system-root-visibility",
			Category: "durability/read correctness",
			Route:    "group final commit",
			RouteAnchors: []orderedRootPublishRouteAnchor{
				orderedRootRouteDBPublishOrderedRootGroupWithSystemBuilder,
				orderedRootRouteDBPublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder,
			},
			ExpectedOutcome:   orderedRootOutcomeCurrentSnapshotVisibility,
			DownstreamActions: []orderedRootPublishDownstreamAction{orderedRootActionRequires3023Correctness},
			Mode:              "serial or optimistic publish",
			Semantics:         "read visibility and system-root identity after publish",
			Storage:           "per-root storage policy",
			Durability:        "snapshot/read visibility and system-root identity",
			Status:            orderedRootPublishStatusImplemented,
			ProductionSupport: "supported current invariant; #3023 span-native publish must keep the same root visibility boundary",
			Covers: []string{
				"snapshot_read_visibility", "system_root_identity",
			},
		},
		{
			ID:                "default-admission-concurrency-dependency",
			Category:          "execution modes",
			Route:             "resolved admission decision from #3032",
			RouteAnchors:      []orderedRootPublishRouteAnchor{orderedRootRouteFlushAdmissionPolicyAuto},
			ExpectedOutcome:   orderedRootOutcomePolicyBlocked,
			DownstreamActions: []orderedRootPublishDownstreamAction{orderedRootActionBlockedBy3032},
			Mode:              "auto/explicit/off admission",
			Semantics:         "ordered-root default eligibility must consume one policy contract",
			Storage:           "no storage change in this issue",
			Durability:        "unsafe WAL-off/default rows must fail closed by policy",
			Status:            orderedRootPublishStatusBlockedByPrefix + "#3032",
			ProductionSupport: "blocked until #3032 defines deterministic auto/explicit/off admission and rollback",
			Covers: []string{
				"admission_policy_dependency_3032", "depends_3032",
			},
		},
		{
			ID:                "raw-route-shared-contract-dependency",
			Category:          "route ownership",
			Route:             "raw TreeDB span-native route inventory",
			RouteAnchors:      []orderedRootPublishRouteAnchor{orderedRootRouteDBPublishOrderedRootDeltaBatchWithAllocator},
			ExpectedOutcome:   orderedRootOutcomeRawRouteBlocked,
			DownstreamActions: []orderedRootPublishDownstreamAction{orderedRootActionBlockedBy3033},
			Mode:              "raw/default route parity",
			Semantics:         "ordered-root coverage must not substitute for raw route support",
			Storage:           "no storage change in this issue",
			Durability:        "raw route checkpoint/reopen/GC proof remains separate",
			Status:            orderedRootPublishStatusBlockedByPrefix + "#3033",
			ProductionSupport: "blocked until #3033 classifies raw public/cached/command-WAL routes",
			Covers: []string{
				"depends_3033",
			},
		},
	}
}

func TestOrderedRootSpanNativeClassificationMatrixCoversIssue3021(t *testing.T) {
	required := map[string]bool{
		"direct_batch_publish":                        false,
		"grouped_publish":                             false,
		"serialized_group_publish":                    false,
		"optimistic_group_publish":                    false,
		"system_delta_builder_publish":                false,
		"command_wal_system_publish":                  false,
		"non_command_wal_system_publish":              false,
		"collection_routes":                           false,
		"buffered_collection_roots":                   false,
		"secondary_index_roots":                       false,
		"multi_index_root_groups":                     false,
		"overlay_warm_single_base_route":              false,
		"overlay_cold_build_routes":                   false,
		"serial_apply":                                false,
		"parallel_root_apply":                         false,
		"read_only_prepare":                           false,
		"span_native_prepare":                         false,
		"wal_enabled":                                 false,
		"wal_disabled_cached_mode":                    false,
		"admission_policy_dependency_3032":            false,
		"point_insert":                                false,
		"overwrite":                                   false,
		"unchanged_noop_root":                         false,
		"delete_tombstone":                            false,
		"tombstone_semantics":                         false,
		"mixed_update_delete_batch":                   false,
		"empty_delta":                                 false,
		"duplicate_same_key_overwrite":                false,
		"inline_leaf_output":                          false,
		"root_local_policy":                           false,
		"value_log_leaf_output":                       false,
		"output_ownership":                            false,
		"persistent_vlog_pointer_reachability":        false,
		"checkpoint":                                  false,
		"reopen":                                      false,
		"snapshot_read_visibility":                    false,
		"system_root_identity":                        false,
		"command_wal_ordering":                        false,
		"value_log_gc_safety":                         false,
		"depends_3032":                                false,
		"depends_3033":                                false,
		"prior_no_replacement_changed_root_guardrail": false,
	}

	rows := orderedRootSpanNativeClassificationRows()
	if len(rows) == 0 {
		t.Fatal("ordered-root classification matrix is empty")
	}
	seenIDs := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		validateOrderedRootSpanNativeClassificationRow(t, row, seenIDs)
		for _, token := range row.Covers {
			if _, ok := required[token]; !ok {
				t.Fatalf("ordered-root classification row %q has unregistered coverage token %q", row.ID, token)
			}
			required[token] = true
		}
	}
	for token, seen := range required {
		if !seen {
			t.Fatalf("ordered-root classification matrix missing issue #3021 coverage token %q", token)
		}
	}
}

func validateOrderedRootSpanNativeClassificationRow(t *testing.T, row orderedRootSpanNativeClassificationRow, seenIDs map[string]struct{}) {
	t.Helper()
	if row.ID == "" {
		t.Fatalf("classification row has empty ID: %+v", row)
	}
	if _, exists := seenIDs[row.ID]; exists {
		t.Fatalf("duplicate classification row ID %q", row.ID)
	}
	seenIDs[row.ID] = struct{}{}
	for field, value := range map[string]string{
		"category":           row.Category,
		"route":              row.Route,
		"expected_outcome":   string(row.ExpectedOutcome),
		"mode":               row.Mode,
		"semantics":          row.Semantics,
		"storage":            row.Storage,
		"durability":         row.Durability,
		"status":             row.Status,
		"production_support": row.ProductionSupport,
	} {
		if strings.TrimSpace(value) == "" {
			t.Fatalf("classification row %q missing %s", row.ID, field)
		}
		lower := strings.ToLower(value)
		if strings.Contains(lower, "unknown") || strings.Contains(lower, "implicit disabled") {
			t.Fatalf("classification row %q has unclassified %s=%q", row.ID, field, value)
		}
	}
	switch {
	case row.Status == orderedRootPublishStatusImplemented:
	case strings.HasPrefix(row.Status, orderedRootPublishStatusDeterministicFallbackPrefix):
		if strings.TrimPrefix(row.Status, orderedRootPublishStatusDeterministicFallbackPrefix) == "" {
			t.Fatalf("classification row %q has empty deterministic fallback reason", row.ID)
		}
	case strings.HasPrefix(row.Status, orderedRootPublishStatusBlockedByPrefix):
		issue := strings.TrimPrefix(row.Status, orderedRootPublishStatusBlockedByPrefix)
		if !strings.HasPrefix(issue, "#") || len(issue) == 1 {
			t.Fatalf("classification row %q has invalid blocker status %q", row.ID, row.Status)
		}
	default:
		t.Fatalf("classification row %q has unsupported status %q", row.ID, row.Status)
	}
	if len(row.RouteAnchors) == 0 {
		t.Fatalf("classification row %q has no concrete route anchors", row.ID)
	}
	for _, anchor := range row.RouteAnchors {
		if _, ok := orderedRootPublishRouteAnchorEvidence[anchor]; !ok {
			t.Fatalf("classification row %q has unsupported route anchor %q", row.ID, anchor)
		}
	}
	if _, ok := orderedRootExpectedOutcomes[row.ExpectedOutcome]; !ok {
		t.Fatalf("classification row %q has unsupported expected outcome %q", row.ID, row.ExpectedOutcome)
	}
	if len(row.DownstreamActions) == 0 {
		t.Fatalf("classification row %q has no downstream action", row.ID)
	}
	seenActions := make(map[orderedRootPublishDownstreamAction]struct{}, len(row.DownstreamActions))
	for _, action := range row.DownstreamActions {
		if _, ok := orderedRootDownstreamActions[action]; !ok {
			t.Fatalf("classification row %q has unsupported downstream action %q", row.ID, action)
		}
		if _, exists := seenActions[action]; exists {
			t.Fatalf("classification row %q repeats downstream action %q", row.ID, action)
		}
		seenActions[action] = struct{}{}
	}
	switch {
	case strings.HasPrefix(row.Status, orderedRootPublishStatusBlockedByPrefix+"#3032"):
		if _, ok := seenActions[orderedRootActionBlockedBy3032]; !ok {
			t.Fatalf("classification row %q blocked by #3032 without matching downstream action", row.ID)
		}
	case strings.HasPrefix(row.Status, orderedRootPublishStatusBlockedByPrefix+"#3033"):
		if _, ok := seenActions[orderedRootActionBlockedBy3033]; !ok {
			t.Fatalf("classification row %q blocked by #3033 without matching downstream action", row.ID)
		}
	default:
		if _, ok := seenActions[orderedRootActionBlockedBy3032]; ok {
			t.Fatalf("classification row %q has #3032 action without blocked status", row.ID)
		}
		if _, ok := seenActions[orderedRootActionBlockedBy3033]; ok {
			t.Fatalf("classification row %q has #3033 action without blocked status", row.ID)
		}
	}
	if len(row.Covers) == 0 {
		t.Fatalf("classification row %q has no coverage tokens", row.ID)
	}
}

func TestOrderedRootSpanNativePublishLocksBlanketNonSpanNativeFallback(t *testing.T) {
	row := orderedRootSpanNativeClassificationRowByID(t, "ordered-root-blanket-non-span-native-fallback")
	wantStatus := orderedRootPublishStatusDeterministicFallbackPrefix + FlushSpanRunFallbackSpanNativeNotImplemented.String()
	if row.Status != wantStatus {
		t.Fatalf("blanket non-span-native fallback row status=%q want %q", row.Status, wantStatus)
	}

	db, err := Open(Options{
		Dir:                    t.TempDir(),
		FlushAdmissionPolicy:   FlushAdmissionPolicyExplicit,
		FlushApplyConcurrency:  2,
		FlushApplyMinEntries:   1,
		FlushApplyMinSpans:     1,
		FlushApplyMinBytes:     1,
		FlushApplySpanNative:   true,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	rawApplyOpts := db.flushApplyOptions()
	if !rawApplyOpts.SpanNativeApply {
		t.Fatal("fixture did not request raw span-native apply")
	}
	orderedApplyOpts := db.orderedRootDeltaBatchApplyOptions()
	if orderedApplyOpts.SpanNativeApply {
		t.Fatalf("ordered-root delta batch span-native apply enabled; matrix row %q must be updated before behavior changes", row.ID)
	}
	if orderedApplyOpts.SpanNativeForceFallbackReason != "" {
		t.Fatalf("ordered-root fallback reason hook=%q, want empty because behavior is current forced non-span-native apply", orderedApplyOpts.SpanNativeForceFallbackReason)
	}
	if !orderedApplyOpts.PrepareReadOnly || orderedApplyOpts.ReadOnlyPrepareWorkers != 2 {
		t.Fatalf("ordered-root read-only prepare options=%+v, want prepare-only planning with two workers", orderedApplyOpts)
	}

	baseRoot, err := db.PublishOrderedRootIterator(0, mustFrozenSystemMemtable(t, "doc/1", "base").NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("PublishOrderedRootIterator: %v", err)
	}
	updateIter := mustFrozenSystemMemtable(t, "doc/2", "update").NewIterator(nil, nil)
	update, err := OrderedRootDeltaBatchFromIterator(updateIter)
	_ = updateIter.Close()
	if err != nil {
		t.Fatalf("OrderedRootDeltaBatchFromIterator: %v", err)
	}
	defer func() { _ = update.Close() }()

	_, rootIDs, err := db.PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder(
		[]OrderedRootDeltaBatchPublishInput{{
			BaseRoot:               baseRoot,
			Delta:                  update,
			PrepareReadOnly:        true,
			ReadOnlyPrepareWorkers: 2,
		}},
		func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			return mustFrozenSystemMemtable(t, "sys/collections/users/root", strconv.FormatUint(rootIDs[0], 10)).NewIterator(nil, nil), nil
		},
	)
	if err != nil {
		t.Fatalf("PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder: %v", err)
	}
	if len(rootIDs) != 1 || rootIDs[0] == 0 {
		t.Fatalf("rootIDs=%v, want one non-zero root", rootIDs)
	}

	stats := db.Stats()
	requireOrderedRootStatCounterPositive(t, stats, "treedb.flush_apply.read_only_prepare.calls_total")
	requireOrderedRootStatCounterZero(t, stats, "treedb.flush_apply.span_native.used_ops_total")
	requireOrderedRootStatCounterPositive(t, stats, "treedb.flush_apply.span_native.fallback.reason."+FlushSpanRunFallbackSpanNativeNotImplemented.String()+".ops_total")
	requireOrderedRootStatCounterZero(t, stats, "treedb.flush_apply.span_native.fallback.reason."+FlushSpanRunFallbackUnknown.String()+".ops_total")
}

func orderedRootSpanNativeClassificationRowByID(t *testing.T, id string) orderedRootSpanNativeClassificationRow {
	t.Helper()
	for _, row := range orderedRootSpanNativeClassificationRows() {
		if row.ID == id {
			return row
		}
	}
	t.Fatalf("missing ordered-root classification row %q", id)
	return orderedRootSpanNativeClassificationRow{}
}

func requireOrderedRootStatCounterPositive(t *testing.T, stats map[string]string, key string) {
	t.Helper()
	if got := orderedRootStatCounterValue(t, stats, key); got == 0 {
		t.Fatalf("stat %q=0, want positive counter", key)
	}
}

func requireOrderedRootStatCounterZero(t *testing.T, stats map[string]string, key string) {
	t.Helper()
	if got := orderedRootStatCounterValue(t, stats, key); got != 0 {
		t.Fatalf("stat %q=%d, want zero counter", key, got)
	}
}

func orderedRootStatCounterValue(t *testing.T, stats map[string]string, key string) uint64 {
	t.Helper()
	raw, ok := stats[key]
	if !ok {
		t.Fatalf("missing stat %q", key)
	}
	got, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		t.Fatalf("stat %q=%q is not an unsigned counter: %v", key, raw, err)
	}
	return got
}
