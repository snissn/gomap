package db

import (
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/zipper"
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
	orderedRootOutcomeCurrentSpanNativeApply          orderedRootPublishExpectedOutcome = "current-span-native-apply"
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
	orderedRootActionImplemented3022Observability   orderedRootPublishDownstreamAction = "implemented-#3022-observability-proof"
	orderedRootActionImplemented3023Correctness     orderedRootPublishDownstreamAction = "implemented-#3023-correctness-proof"
	orderedRootActionImplemented3023StoragePolicy   orderedRootPublishDownstreamAction = "implemented-#3023-storage-policy-proof"
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
	orderedRootOutcomeCurrentSpanNativeApply:          {},
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
	orderedRootActionImplemented3022Observability:   {},
	orderedRootActionImplemented3023Correctness:     {},
	orderedRootActionImplemented3023StoragePolicy:   {},
	orderedRootActionRequires3023Correctness:        {},
	orderedRootActionRequires3024OutputOwnership:    {},
	orderedRootActionBlockedBy3032:                  {},
	orderedRootActionBlockedBy3033:                  {},
	orderedRootActionImplementedCurrentFailClosed:   {},
	orderedRootActionImplementedCurrentReadOnlyOnly: {},
}

var orderedRootSupportedBlockerActions = map[string]orderedRootPublishDownstreamAction{
	"#3032": orderedRootActionBlockedBy3032,
	"#3033": orderedRootActionBlockedBy3033,
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
			DownstreamActions: []orderedRootPublishDownstreamAction{orderedRootActionImplementedCurrentSerialOnly, orderedRootActionImplemented3022Observability},
			Mode:              "serial apply",
			Semantics:         "point inserts and overwrites through ordered iterators",
			Storage:           "root-local inline or DB default leaf policy",
			Durability:        "checkpoint, reopen, snapshot read visibility",
			Status:            orderedRootPublishStatusImplemented,
			ProductionSupport: "current serial publisher is supported; #3022 triage reports direct_publish as route_ineligible so it cannot be mistaken for ordered-root span-native proof",
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
			DownstreamActions: []orderedRootPublishDownstreamAction{orderedRootActionImplementedCurrentSerialOnly, orderedRootActionImplemented3022Observability},
			Mode:              "serialized group publish",
			Semantics:         "full-root grouped publication with system descriptor publish",
			Storage:           "per-root storage policy",
			Durability:        "one backend commit preserves system-root identity",
			Status:            orderedRootPublishStatusImplemented,
			ProductionSupport: "current grouped full-root publish is supported; #3022 triage reports grouped_publish as route_ineligible while warm system delta routes are eligible when admission allows",
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
			DownstreamActions: []orderedRootPublishDownstreamAction{orderedRootActionImplemented3023Correctness},
			Mode:              "serial apply",
			Semantics:         "warm root-local iterator deltas preserve omitted base entries",
			Storage:           "per-root storage policy",
			Durability:        "system root and ordered roots commit together",
			Status:            orderedRootPublishStatusImplemented,
			ProductionSupport: "current iterator-delta publish is supported and warm exact deltas inherit the #3023 ordered-root span-native correctness contract through materialized batch apply",
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
			DownstreamActions: []orderedRootPublishDownstreamAction{orderedRootActionImplemented3023Correctness, orderedRootActionImplemented3023StoragePolicy},
			Mode:              "optimistic group publish with optional root-level parallel apply",
			Semantics:         "materialized batch deltas publish roots before guarded final commit",
			Storage:           "per-root storage policy",
			Durability:        "root mismatch retries through serialized fallback",
			Status:            orderedRootPublishStatusImplemented,
			ProductionSupport: "current optimistic root-group publish is supported; warm exact root applies can use span-native leaf apply and retain root-mismatch retry/final-commit ownership checks",
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
			DownstreamActions: []orderedRootPublishDownstreamAction{orderedRootActionImplemented3023Correctness, orderedRootActionImplemented3022Observability},
			Mode:              "WAL-enabled command publish",
			Semantics:         "command-WAL LSN is assigned before system delta builder",
			Storage:           "per-root storage policy",
			Durability:        "command-WAL ordering and system-root identity",
			Status:            orderedRootPublishStatusImplemented,
			ProductionSupport: "current command-WAL covered ordered-root publish is supported with fail-closed poisoning; warm command-WAL deltas can use span-native apply after LSN assignment",
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
			DownstreamActions: []orderedRootPublishDownstreamAction{orderedRootActionImplementedCurrentFailClosed},
			Mode:              "WAL-disabled cached mode",
			Semantics:         "ordered-root behavior depends on resolved default admission decision",
			Storage:           "persistent value-log remains durable storage",
			Durability:        "WAL-off durability posture is owned by admission policy",
			Status:            orderedRootPublishStatusDeterministicFallbackPrefix + FlushSpanRunFallbackAdmissionPolicyDecline.String(),
			ProductionSupport: "supported fail-closed policy row: cached/WAL-off ordered-root candidates must be explicitly admitted, otherwise counters and triage expose admission_policy_decline",
			Covers: []string{
				"wal_disabled_cached_mode", "admission_policy_fail_closed",
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
			DownstreamActions: []orderedRootPublishDownstreamAction{orderedRootActionImplementedCurrentReadOnlyOnly, orderedRootActionImplemented3022Observability},
			Mode:              "read-only prepare",
			Semantics:         "side-effect-free leaf-span planning before warm apply",
			Storage:           "no durable output owned by prepare",
			Durability:        "planning must not publish roots or value-log pointers",
			Status:            orderedRootPublishStatusImplemented,
			ProductionSupport: "supported as diagnostics and planning; read-only prepare is also the planning contract for admitted warm ordered-root span-native apply",
			Covers: []string{
				"read_only_prepare", "output_ownership",
			},
		},
		{
			ID:       "ordered-root-span-native-apply-enabled",
			Category: "execution modes",
			Route:    "orderedRootDeltaBatchApplyOptions",
			RouteAnchors: []orderedRootPublishRouteAnchor{
				orderedRootRouteDBOrderedRootDeltaBatchApplyOptions,
				orderedRootRouteDBPublishOrderedRootDeltaBatchWithAllocator,
			},
			ExpectedOutcome:   orderedRootOutcomeCurrentSpanNativeApply,
			DownstreamActions: []orderedRootPublishDownstreamAction{orderedRootActionImplemented3023Correctness, orderedRootActionImplemented3023StoragePolicy},
			Mode:              "span-native prepare/apply",
			Semantics:         "ordered-root delta batches allow admitted span-native apply for exact warm point spans before reducer execution",
			Storage:           "prepared span-native output uses the selected ordered-root storage policy, including value-log leaf output when requested",
			Durability:        "span-native reducer publish preserves checkpoint, reopen, snapshot visibility, and root-mismatch/output ownership guardrails",
			Status:            orderedRootPublishStatusImplemented,
			ProductionSupport: "current support records ordered-root candidate, eligible, and used ops for warm exact rows; deterministic fallback counters remain for no-op, cold, maintenance, policy, and validation rows",
			Covers: []string{
				"span_native_prepare", "prior_no_replacement_changed_root_guardrail", "output_ownership",
				"ordered_root_span_native_candidate_ops_total", "ordered_root_span_native_fallback_reason", "ordered_root_span_native_used_ops_total",
			},
		},
		{
			ID:                "empty-and-noop-deltas",
			Category:          "delta semantics",
			Route:             "publishOrderedRootDeltaBatchWithAllocator",
			RouteAnchors:      []orderedRootPublishRouteAnchor{orderedRootRouteDBPublishOrderedRootDeltaBatchWithAllocator},
			ExpectedOutcome:   orderedRootOutcomeCurrentEmptyNoop,
			DownstreamActions: []orderedRootPublishDownstreamAction{orderedRootActionImplemented3023Correctness},
			Mode:              "serial apply",
			Semantics:         "empty deltas and unchanged roots preserve base root identity",
			Storage:           "no new leaf output",
			Durability:        "checkpoint/reopen observe existing root",
			Status:            orderedRootPublishStatusImplemented,
			ProductionSupport: "supported current behavior; no-op rows preserve base root identity and emit below_threshold ordered-root fallback counters",
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
			DownstreamActions: []orderedRootPublishDownstreamAction{orderedRootActionImplemented3023Correctness, orderedRootActionImplemented3023StoragePolicy},
			Mode:              "serial apply",
			Semantics:         "deletes, tombstones, and mixed update/delete batches",
			Storage:           "per-root storage policy",
			Durability:        "value-log refs removed from replaced/deleted entries become GC candidates only after commit reachability changes",
			Status:            orderedRootPublishStatusImplemented,
			ProductionSupport: "supported behavior; exact point deletes, tombstones, and mixed update/delete batches are covered by #3023 span-native parity tests and runtime counters",
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
			DownstreamActions: []orderedRootPublishDownstreamAction{orderedRootActionImplemented3023StoragePolicy},
			Mode:              "serial apply",
			Semantics:         "inline/pager leaf output for ordered roots",
			Storage:           "pager leaves with root-local policy",
			Durability:        "checkpoint/reopen through index pages",
			Status:            orderedRootPublishStatusImplemented,
			ProductionSupport: "supported output policy; #3023 span-native apply uses the same root-local pager leaf output ownership path",
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
			DownstreamActions: []orderedRootPublishDownstreamAction{orderedRootActionImplemented3023StoragePolicy},
			Mode:              "serial apply",
			Semantics:         "ordered-root leaves stored as persistent leaf-log records",
			Storage:           "value-log leaf output",
			Durability:        "persistent pointer reachability and value-log GC safety",
			Status:            orderedRootPublishStatusImplemented,
			ProductionSupport: "supported as persistent value-log storage; #3023 span-native apply preserves leaf-log pointer reachability through GC, checkpoint, and reopen",
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
			DownstreamActions: []orderedRootPublishDownstreamAction{orderedRootActionImplemented3022Observability, orderedRootActionImplemented3023Correctness},
			Mode:              "ordered-root delta batch group",
			Semantics:         "primary buffered roots publish through ordered-root batch APIs",
			Storage:           "collection-selected root policy",
			Durability:        "collection metadata and roots commit together",
			Status:            orderedRootPublishStatusImplemented,
			ProductionSupport: "current buffered collection publish is supported; warm collection buffered roots are eligible for ordered-root span-native apply when admitted",
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
			DownstreamActions: []orderedRootPublishDownstreamAction{orderedRootActionImplemented3022Observability, orderedRootActionImplemented3023Correctness},
			Mode:              "multi-index ordered-root group",
			Semantics:         "secondary indexes publish alongside collection roots",
			Storage:           "collection-selected root policy",
			Durability:        "multi-index root group identity is persisted through system descriptors",
			Status:            orderedRootPublishStatusImplemented,
			ProductionSupport: "current secondary-index root publishing is supported via ordered-root groups; warm multi-index root groups are eligible for ordered-root span-native apply when admitted",
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
			DownstreamActions: []orderedRootPublishDownstreamAction{orderedRootActionImplemented3023Correctness, orderedRootActionImplemented3023StoragePolicy},
			Mode:              "warm overlay publish",
			Semantics:         "exactly one overlay can be used as the nonzero base root before publishing a newer overlay delta",
			Storage:           "collection-selected root policy",
			Durability:        "checkpoint/reopen preserve overlay identity",
			Status:            orderedRootPublishStatusImplemented,
			ProductionSupport: "supported warm overlay behavior; one nonzero base overlay is an admitted exact warm ordered-root span-native candidate",
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
			DownstreamActions: []orderedRootPublishDownstreamAction{orderedRootActionImplementedCurrentFailClosed},
			Mode:              "cold build publish",
			Semantics:         "zero overlays or multiple existing overlays use BaseRoot=0 and include deleted entries as cold-build input",
			Storage:           "collection-selected root policy",
			Durability:        "checkpoint/reopen preserve overlay identity",
			Status:            orderedRootPublishStatusDeterministicFallbackPrefix + FlushSpanRunFallbackColdBuild.String(),
			ProductionSupport: "supported fail-closed cold-build route; zero-base overlay/cold-build publishes emit cold_build fallback because there are no existing leaf spans to replace",
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
			DownstreamActions: []orderedRootPublishDownstreamAction{orderedRootActionImplemented3023Correctness},
			Mode:              "serial or optimistic publish",
			Semantics:         "read visibility and system-root identity after publish",
			Storage:           "per-root storage policy",
			Durability:        "snapshot/read visibility and system-root identity",
			Status:            orderedRootPublishStatusImplemented,
			ProductionSupport: "supported invariant; #3023 span-native publish keeps the same root visibility boundary",
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
			DownstreamActions: []orderedRootPublishDownstreamAction{orderedRootActionImplementedCurrentFailClosed},
			Mode:              "auto/explicit/off admission",
			Semantics:         "ordered-root default eligibility must consume one policy contract",
			Storage:           "no storage change in this issue",
			Durability:        "unsafe WAL-off/default rows must fail closed by policy",
			Status:            orderedRootPublishStatusDeterministicFallbackPrefix + FlushSpanRunFallbackAdmissionPolicyDecline.String(),
			ProductionSupport: "supported fail-closed admission row: auto/explicit/off decisions are surfaced as eligible when admitted or as disabled/admission_policy_decline fallbacks when not admitted",
			Covers: []string{
				"admission_policy_fail_closed",
			},
		},
		{
			ID:                "raw-route-shared-contract-dependency",
			Category:          "route ownership",
			Route:             "raw TreeDB span-native route inventory",
			RouteAnchors:      []orderedRootPublishRouteAnchor{orderedRootRouteDBPublishOrderedRootDeltaBatchWithAllocator},
			ExpectedOutcome:   orderedRootOutcomeRawRouteBlocked,
			DownstreamActions: []orderedRootPublishDownstreamAction{orderedRootActionImplementedCurrentFailClosed},
			Mode:              "raw/default route parity",
			Semantics:         "ordered-root coverage must not substitute for raw route support",
			Storage:           "no storage change in this issue",
			Durability:        "raw route checkpoint/reopen/GC proof remains separate",
			Status:            orderedRootPublishStatusDeterministicFallbackPrefix + FlushSpanRunFallbackRouteIneligible.String(),
			ProductionSupport: "supported fail-closed route-ownership row: ordered-root coverage does not substitute for raw route support, which remains outside this matrix",
			Covers: []string{
				"raw_route_fail_closed",
			},
		},
	}
}

func TestOrderedRootSpanNativeClassificationMatrixCoversIssue3021(t *testing.T) {
	required := map[string]bool{
		"direct_batch_publish":                         false,
		"grouped_publish":                              false,
		"serialized_group_publish":                     false,
		"optimistic_group_publish":                     false,
		"system_delta_builder_publish":                 false,
		"command_wal_system_publish":                   false,
		"non_command_wal_system_publish":               false,
		"collection_routes":                            false,
		"buffered_collection_roots":                    false,
		"secondary_index_roots":                        false,
		"multi_index_root_groups":                      false,
		"overlay_warm_single_base_route":               false,
		"overlay_cold_build_routes":                    false,
		"serial_apply":                                 false,
		"parallel_root_apply":                          false,
		"read_only_prepare":                            false,
		"span_native_prepare":                          false,
		"wal_enabled":                                  false,
		"wal_disabled_cached_mode":                     false,
		"admission_policy_fail_closed":                 false,
		"point_insert":                                 false,
		"overwrite":                                    false,
		"unchanged_noop_root":                          false,
		"delete_tombstone":                             false,
		"tombstone_semantics":                          false,
		"mixed_update_delete_batch":                    false,
		"empty_delta":                                  false,
		"duplicate_same_key_overwrite":                 false,
		"inline_leaf_output":                           false,
		"root_local_policy":                            false,
		"value_log_leaf_output":                        false,
		"output_ownership":                             false,
		"persistent_vlog_pointer_reachability":         false,
		"checkpoint":                                   false,
		"reopen":                                       false,
		"snapshot_read_visibility":                     false,
		"system_root_identity":                         false,
		"command_wal_ordering":                         false,
		"value_log_gc_safety":                          false,
		"raw_route_fail_closed":                        false,
		"prior_no_replacement_changed_root_guardrail":  false,
		"ordered_root_span_native_candidate_ops_total": false,
		"ordered_root_span_native_fallback_reason":     false,
		"ordered_root_span_native_used_ops_total":      false,
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

func TestOrderedRootSpanNativeClassificationHasNoOpenBlockers(t *testing.T) {
	for _, row := range orderedRootSpanNativeClassificationRows() {
		if strings.HasPrefix(row.Status, orderedRootPublishStatusBlockedByPrefix) {
			t.Fatalf("classification row %q still has blocked status %q", row.ID, row.Status)
		}
		for _, action := range row.DownstreamActions {
			if action == orderedRootActionRequires3023Correctness || action == orderedRootActionRequires3024OutputOwnership {
				t.Fatalf("classification row %q still has open downstream action %q", row.ID, action)
			}
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
		if _, ok := orderedRootSupportedBlockerActions[issue]; !ok {
			t.Fatalf("classification row %q has unsupported blocker status %q", row.ID, row.Status)
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
	if strings.HasPrefix(row.Status, orderedRootPublishStatusBlockedByPrefix) {
		issue := strings.TrimPrefix(row.Status, orderedRootPublishStatusBlockedByPrefix)
		action := orderedRootSupportedBlockerActions[issue]
		if _, ok := seenActions[action]; !ok {
			t.Fatalf("classification row %q blocked by %s without matching downstream action", row.ID, issue)
		}
	} else {
		for issue, action := range orderedRootSupportedBlockerActions {
			if _, ok := seenActions[action]; ok {
				t.Fatalf("classification row %q has %s action without blocked status", row.ID, issue)
			}
		}
	}
	if len(row.Covers) == 0 {
		t.Fatalf("classification row %q has no coverage tokens", row.ID)
	}
}

func TestOrderedRootSpanNativePublishUsesSpanNativeWhenAdmitted(t *testing.T) {
	row := orderedRootSpanNativeClassificationRowByID(t, "ordered-root-span-native-apply-enabled")
	if row.Status != orderedRootPublishStatusImplemented {
		t.Fatalf("span-native apply row status=%q want implemented", row.Status)
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
	if rawApplyOpts.SpanNativeAllowMaintenancePointOps {
		t.Fatalf("raw apply options enabled maintenance point span-native opt-in: %+v", rawApplyOpts)
	}
	orderedApplyOpts := db.orderedRootDeltaBatchApplyOptions(systemRootOrderedPublishOptions(db))
	if !orderedApplyOpts.SpanNativeApply {
		t.Fatalf("ordered-root delta batch span-native apply disabled; matrix row %q expects admitted warm apply", row.ID)
	}
	if !orderedApplyOpts.SpanNativeAllowMaintenancePointOps {
		t.Fatalf("ordered-root apply options did not enable maintenance point span-native opt-in: %+v", orderedApplyOpts)
	}
	if got := orderedApplyOpts.SpanNativeForceFallbackReason; got != "" {
		t.Fatalf("ordered-root fallback reason hook=%q, want empty", got)
	}
	if !orderedApplyOpts.PrepareReadOnly || orderedApplyOpts.ReadOnlyPrepareWorkers != 2 {
		t.Fatalf("ordered-root read-only prepare options=%+v, want span-native planning with two workers", orderedApplyOpts)
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
	requireOrderedRootStatCounterPositive(t, stats, "treedb.flush_apply.span_native.used_ops_total")
	requireOrderedRootStatCounterPositive(t, stats, "treedb.publish.ordered_root_delta_group.span_native.candidate_ops_total")
	requireOrderedRootStatCounterPositive(t, stats, "treedb.publish.ordered_root_delta_group.span_native.eligible_ops_total")
	requireOrderedRootStatCounterPositive(t, stats, "treedb.publish.ordered_root_delta_group.span_native.used_ops_total")
	requireOrderedRootStatCounterZero(t, stats, "treedb.publish.ordered_root_delta_group.span_native.fallback.reason."+FlushSpanRunFallbackSpanNativeNotImplemented.String()+".count_total")
	requireOrderedRootStatCounterZero(t, stats, "treedb.publish.ordered_root_delta_group.span_native.fallback.reason."+FlushSpanRunFallbackSpanNativeNotImplemented.String()+".ops_total")
	requireOrderedRootStatCounterZero(t, stats, "treedb.publish.ordered_root_delta_group.span_native.fallback.reason."+FlushSpanRunFallbackUnknown.String()+".count_total")
	requireOrderedRootStatCounterZero(t, stats, "treedb.publish.ordered_root_delta_group.span_native.fallback.reason."+FlushSpanRunFallbackUnknown.String()+".ops_total")
	multiIndexPrefix := "treedb.publish.ordered_root_delta_group.span_native.route.multi_index_group_publish."
	requireOrderedRootStatCounterPositive(t, stats, multiIndexPrefix+"observations_total")
	requireOrderedRootStatCounterPositive(t, stats, multiIndexPrefix+"candidate_ops_total")
	requireOrderedRootStatCounterPositive(t, stats, multiIndexPrefix+"eligible_ops_total")
	requireOrderedRootStatCounterPositive(t, stats, multiIndexPrefix+"used_ops_total")
	requireOrderedRootStatCounterZero(t, stats, multiIndexPrefix+"fallback.reason."+FlushSpanRunFallbackSpanNativeNotImplemented.String()+".count_total")
	requireOrderedRootStatCounterZero(t, stats, multiIndexPrefix+"fallback.reason."+FlushSpanRunFallbackSpanNativeNotImplemented.String()+".ops_total")
	readOnlyPreparePrefix := "treedb.publish.ordered_root_delta_group.span_native.route.read_only_prepare."
	requireOrderedRootStatCounterPositive(t, stats, readOnlyPreparePrefix+"observations_total")
	requireOrderedRootStatCounterPositive(t, stats, readOnlyPreparePrefix+"candidate_ops_total")
	requireOrderedRootStatCounterZero(t, stats, readOnlyPreparePrefix+"used_ops_total")
	requireOrderedRootStatCounterZero(t, stats, "treedb.publish.ordered_root_delta_group.span_native.route.delta_batch_publish.candidate_ops_total")
	if got := stats["treedb.publish.ordered_root_delta_group.span_native.triage.route.multi_index_group_publish.status"]; got != string(OrderedRootSpanNativeStatusEligible) {
		t.Fatalf("ordered-root multi-index triage status=%q want eligible", got)
	}
	if got := stats["treedb.publish.ordered_root_delta_group.span_native.triage.route.multi_index_group_publish.fallback_reason"]; got != "" {
		t.Fatalf("ordered-root multi-index triage fallback=%q want empty", got)
	}
}

func TestOrderedRootSpanNativeDefaultAutoAdmissionEnablesApplyOptions(t *testing.T) {
	opts := Options{}
	decision := computeFlushAdmissionDecisionForHardware(&opts, 16, 6)
	if !decision.Admitted || !opts.FlushApplySpanNative || opts.FlushApplyConcurrency <= 1 {
		t.Fatalf("fixture default auto admission=%+v opts=%+v, want admitted span-native concurrency", decision, opts)
	}
	db := &DB{
		flushAdmission:        decision,
		flushApplyConcurrency: normalizeFlushApplyConcurrency(opts.FlushApplyConcurrency),
		flushApplyMinEntries:  opts.FlushApplyMinEntries,
		flushApplyMinSpans:    opts.FlushApplyMinSpans,
		flushApplyMinBytes:    opts.FlushApplyMinBytes,
		flushApplySpanNative:  opts.FlushApplySpanNative,
	}

	rawApplyOpts := db.flushApplyOptions()
	if !rawApplyOpts.SpanNativeApply || rawApplyOpts.ParallelApplyConcurrency <= 1 {
		t.Fatalf("default raw apply options=%+v, want admitted span-native workers", rawApplyOpts)
	}
	if rawApplyOpts.SpanNativeAllowMaintenancePointOps {
		t.Fatalf("default raw apply options enabled ordered-root maintenance opt-in: %+v", rawApplyOpts)
	}

	orderedApplyOpts := db.orderedRootDeltaBatchApplyOptions(
		systemRootOrderedPublishOptions(db).withSpanNativeRoute(OrderedRootSpanNativeRouteMultiIndexGroupPublish, "default auto ordered-root apply"),
	)
	if !orderedApplyOpts.SpanNativeApply || orderedApplyOpts.ParallelApplyConcurrency <= 1 {
		t.Fatalf("default ordered-root apply options=%+v, want admitted span-native workers", orderedApplyOpts)
	}
	if !orderedApplyOpts.SpanNativeAllowMaintenancePointOps {
		t.Fatalf("default ordered-root apply options did not enable maintenance point opt-in: %+v", orderedApplyOpts)
	}
	if got := orderedApplyOpts.SpanNativeForceFallbackReason; got != "" {
		t.Fatalf("default ordered-root fallback reason=%q want empty", got)
	}
}

func TestOrderedRootSpanNativeAdmissionPolicyDisablesApplyOptions(t *testing.T) {
	cases := []struct {
		name   string
		opts   Options
		gomax  int
		cores  int
		reason FlushSpanRunFallbackReason
	}{
		{
			name:   "off",
			opts:   Options{FlushAdmissionPolicy: FlushAdmissionPolicyOff, FlushApplySpanNative: true, FlushApplyConcurrency: 4},
			gomax:  16,
			cores:  6,
			reason: FlushSpanRunFallbackDisabled,
		},
		{
			name:   "auto-decline",
			opts:   Options{FlushAdmissionPolicy: FlushAdmissionPolicyAuto, FlushApplySpanNative: true, FlushApplyConcurrency: 1},
			gomax:  4,
			cores:  4,
			reason: FlushSpanRunFallbackAdmissionPolicyDecline,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := tc.opts
			decision := computeFlushAdmissionDecisionForHardware(&opts, tc.gomax, tc.cores)
			if decision.Admitted || opts.FlushApplySpanNative || opts.FlushApplyConcurrency != 0 {
				t.Fatalf("admission decision=%+v opts=%+v, want disabled ordered-root apply inputs", decision, opts)
			}
			db := &DB{
				flushAdmission:        decision,
				flushApplyConcurrency: normalizeFlushApplyConcurrency(opts.FlushApplyConcurrency),
				flushApplyMinEntries:  opts.FlushApplyMinEntries,
				flushApplyMinSpans:    opts.FlushApplyMinSpans,
				flushApplyMinBytes:    opts.FlushApplyMinBytes,
				flushApplySpanNative:  opts.FlushApplySpanNative,
			}
			orderedApplyOpts := db.orderedRootDeltaBatchApplyOptions(
				systemRootOrderedPublishOptions(db).withSpanNativeRoute(OrderedRootSpanNativeRouteMultiIndexGroupPublish, "policy disabled ordered-root apply"),
			)
			if orderedApplyOpts.SpanNativeApply || orderedApplyOpts.PrepareReadOnly || orderedApplyOpts.ParallelApplyConcurrency != 0 {
				t.Fatalf("ordered-root apply options=%+v, want policy-disabled serial apply", orderedApplyOpts)
			}
			row := orderedRootSpanNativeTriageRowsByRoute(db.OrderedRootSpanNativeTriageSnapshot())[OrderedRootSpanNativeRouteMultiIndexGroupPublish]
			if row.FallbackReason != tc.reason.String() || row.FallbackClass != OrderedRootSpanNativeFallbackClassPolicy {
				t.Fatalf("triage row=%+v want %s policy fallback", row, tc.reason)
			}
		})
	}
}

func TestOrderedRootSpanNativeUnsupportedRouteLabelFallsBack(t *testing.T) {
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
			BaseRoot:          baseRoot,
			Delta:             update,
			SpanNativeRoute:   OrderedRootSpanNativeRouteDirectPublish,
			SpanNativeContext: "unsupported direct route label",
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
	directPrefix := "treedb.publish.ordered_root_delta_group.span_native.route.direct_publish."
	requireOrderedRootStatCounterPositive(t, stats, directPrefix+"observations_total")
	requireOrderedRootStatCounterPositive(t, stats, directPrefix+"fallback.reason."+FlushSpanRunFallbackRouteIneligible.String()+".count_total")
	requireOrderedRootStatCounterPositive(t, stats, directPrefix+"fallback.reason."+FlushSpanRunFallbackRouteIneligible.String()+".ops_total")
	requireOrderedRootStatCounterZero(t, stats, directPrefix+"used_ops_total")
	requireOrderedRootStatCounterZero(t, stats, directPrefix+"fallback.reason."+FlushSpanRunFallbackSpanNativeNotImplemented.String()+".ops_total")
	requireOrderedRootStatCounterZero(t, stats, "treedb.publish.ordered_root_delta_group.span_native.route.multi_index_group_publish.used_ops_total")
}

func TestOrderedRootSpanNativeReadOnlyPrepareUsesPointDeleteOptions(t *testing.T) {
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

	baseRoot, err := db.PublishOrderedRootIterator(0, mustFrozenSystemMemtable(t,
		"doc/delete-me", "base",
		"doc/keep", "base",
	).NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("PublishOrderedRootIterator: %v", err)
	}
	delta := batch.New(nil, orderedRootDeltaBatchInlineThreshold)
	if err := delta.Delete([]byte("doc/delete-me")); err != nil {
		t.Fatalf("Delete delta: %v", err)
	}
	defer func() { _ = delta.Close() }()

	_, rootIDs, err := db.PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder(
		[]OrderedRootDeltaBatchPublishInput{{
			BaseRoot:               baseRoot,
			Delta:                  delta,
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
	readOnlyPreparePrefix := "treedb.publish.ordered_root_delta_group.span_native.route.read_only_prepare."
	requireOrderedRootStatCounterPositive(t, stats, readOnlyPreparePrefix+"candidate_ops_total")
	requireOrderedRootStatCounterPositive(t, stats, readOnlyPreparePrefix+"eligible_ops_total")
	requireOrderedRootStatCounterZero(t, stats, readOnlyPreparePrefix+"fallback.reason."+FlushSpanRunFallbackInexactLeafSpans.String()+".count_total")
	requireOrderedRootStatCounterZero(t, stats, readOnlyPreparePrefix+"fallback.reason."+FlushSpanRunFallbackInexactLeafSpans.String()+".ops_total")
	requireOrderedRootStatCounterZero(t, stats, readOnlyPreparePrefix+"fallback.reason."+FlushSpanRunFallbackUnknown.String()+".count_total")
}

func TestOrderedRootSpanNativePublishObservesExplicitSpanNativeFallback(t *testing.T) {
	db, err := Open(Options{
		Dir:                    t.TempDir(),
		FlushAdmissionPolicy:   FlushAdmissionPolicyExplicit,
		FlushApplyConcurrency:  1,
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
	if !rawApplyOpts.SpanNativeApply || !rawApplyOpts.PrepareReadOnly || rawApplyOpts.ParallelApplyConcurrency > 1 {
		t.Fatalf("fixture raw apply options=%+v, want span-native prepare-only without parallel apply", rawApplyOpts)
	}
	orderedOpts := systemRootOrderedPublishOptions(db).withSpanNativeFallback(FlushSpanRunFallbackSpanNativeNotImplemented)
	orderedApplyOpts := db.orderedRootDeltaBatchApplyOptions(orderedOpts)
	if !orderedApplyOpts.SpanNativeApply {
		t.Fatalf("ordered-root delta batch span-native apply disabled; want explicit fallback with prepare retained")
	}
	if got, want := orderedApplyOpts.SpanNativeForceFallbackReason, FlushSpanRunFallbackSpanNativeNotImplemented.String(); got != want {
		t.Fatalf("ordered-root fallback reason hook=%q, want %q", got, want)
	}
	if !orderedApplyOpts.PrepareReadOnly || orderedApplyOpts.ParallelApplyConcurrency > 1 {
		t.Fatalf("ordered-root read-only prepare options=%+v, want span-native fallback prepare retained", orderedApplyOpts)
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

	newRoot, _, _, err := db.publishOrderedRootDeltaBatch(baseRoot, update, orderedOpts)
	if err != nil {
		t.Fatalf("publishOrderedRootDeltaBatch: %v", err)
	}
	if newRoot == 0 || newRoot == baseRoot {
		t.Fatalf("newRoot=%d baseRoot=%d, want changed non-zero root", newRoot, baseRoot)
	}

	stats := db.Stats()
	requireOrderedRootStatCounterPositive(t, stats, "treedb.flush_apply.read_only_prepare.calls_total")
	requireOrderedRootStatCounterPositive(t, stats, "treedb.publish.ordered_root_delta_group.span_native.candidate_ops_total")
	requireOrderedRootStatCounterZero(t, stats, "treedb.publish.ordered_root_delta_group.span_native.eligible_ops_total")
	requireOrderedRootStatCounterZero(t, stats, "treedb.publish.ordered_root_delta_group.span_native.used_ops_total")
	requireOrderedRootStatCounterPositive(t, stats, "treedb.publish.ordered_root_delta_group.span_native.fallback.reason."+FlushSpanRunFallbackSpanNativeNotImplemented.String()+".count_total")
	requireOrderedRootStatCounterPositive(t, stats, "treedb.publish.ordered_root_delta_group.span_native.fallback.reason."+FlushSpanRunFallbackSpanNativeNotImplemented.String()+".ops_total")
	routePrefix := "treedb.publish.ordered_root_delta_group.span_native.route.delta_batch_publish."
	requireOrderedRootStatCounterPositive(t, stats, routePrefix+"observations_total")
	requireOrderedRootStatCounterPositive(t, stats, routePrefix+"candidate_ops_total")
	requireOrderedRootStatCounterPositive(t, stats, routePrefix+"fallback.reason."+FlushSpanRunFallbackSpanNativeNotImplemented.String()+".count_total")
}

func TestOrderedRootSpanNativeEligibilityCountsCandidatesAndEligibleRows(t *testing.T) {
	opts := Options{}
	decision := computeFlushAdmissionDecisionForHardware(&opts, 16, 6)
	db := &DB{flushAdmission: decision}
	row := db.orderedRootSpanNativeEligibility(orderedRootSpanNativeEligibilityRequest{
		Route:              OrderedRootSpanNativeRouteDeltaBatchPublish,
		Context:            "test warm delta batch",
		Summary:            orderedRootSafeSpanSummary(6, 3),
		SpanNativeEligible: true,
	})
	if !row.Candidate {
		t.Fatalf("candidate=false want true: %+v", row)
	}
	if !row.Eligible || row.Used {
		t.Fatalf("eligible/used=%t/%t want eligible-but-not-used", row.Eligible, row.Used)
	}
	if row.Status != OrderedRootSpanNativeStatusEligible {
		t.Fatalf("status=%q want eligible", row.Status)
	}
	if row.FallbackReason != "" || row.FallbackClass != OrderedRootSpanNativeFallbackClassNone {
		t.Fatalf("fallback=%q class=%q want none", row.FallbackReason, row.FallbackClass)
	}
	db.observeOrderedRootSpanNativeEligibility(row)
	if got := db.orderedRootSpanNativeCandidateOps.Load(); got != 6 {
		t.Fatalf("candidate ops=%d want 6", got)
	}
	if got := db.orderedRootSpanNativeCandidateSpans.Load(); got != 3 {
		t.Fatalf("candidate spans=%d want 3", got)
	}
	if got := db.orderedRootSpanNativeEligibleOps.Load(); got != 6 {
		t.Fatalf("eligible ops=%d want 6", got)
	}
	if got := db.orderedRootSpanNativeUsedOps.Load(); got != 0 {
		t.Fatalf("used ops=%d want 0", got)
	}
	if got := db.orderedRootSpanNativeFallbackOps[FlushSpanRunFallbackSpanNativeNotImplemented].Load(); got != 0 {
		t.Fatalf("not implemented fallback ops=%d want 0", got)
	}
	if got := db.orderedRootSpanNativeFallbackReasonCounts[FlushSpanRunFallbackSpanNativeNotImplemented].Load(); got != 0 {
		t.Fatalf("not implemented fallback count=%d want 0", got)
	}
	if got := db.orderedRootSpanNativeFallbackOps[FlushSpanRunFallbackUnknown].Load(); got != 0 {
		t.Fatalf("unknown fallback ops=%d want 0", got)
	}
	if got := db.orderedRootSpanNativeFallbackReasonCounts[FlushSpanRunFallbackUnknown].Load(); got != 0 {
		t.Fatalf("unknown fallback count=%d want 0", got)
	}
	stats := map[string]string{}
	db.appendOrderedRootSpanNativeStats(stats)
	routePrefix := "treedb.publish.ordered_root_delta_group.span_native.route.delta_batch_publish."
	if got := stats[routePrefix+"observations_total"]; got != "1" {
		t.Fatalf("delta batch route observations=%q want 1", got)
	}
	if got := stats[routePrefix+"candidate_ops_total"]; got != "6" {
		t.Fatalf("delta batch route candidate ops=%q want 6", got)
	}
	if got := stats[routePrefix+"eligible_ops_total"]; got != "6" {
		t.Fatalf("delta batch route eligible ops=%q want 6", got)
	}
	if got := stats[routePrefix+"fallback.reason."+FlushSpanRunFallbackSpanNativeNotImplemented.String()+".ops_total"]; got != "0" {
		t.Fatalf("delta batch route fallback ops=%q want 0", got)
	}
	if got := stats["treedb.publish.ordered_root_delta_group.span_native.route.command_wal_publish.candidate_ops_total"]; got != "0" {
		t.Fatalf("command WAL route candidate ops=%q want 0", got)
	}
}

func TestOrderedRootSpanNativeEligibilityDoesNotCountEligibleAsFallback(t *testing.T) {
	db := &DB{}
	db.observeOrderedRootSpanNativeEligibility(OrderedRootSpanNativeTriageRow{
		Route:         OrderedRootSpanNativeRouteDeltaBatchPublish,
		Context:       "eligible observation",
		Status:        OrderedRootSpanNativeStatusEligible,
		Candidate:     true,
		Eligible:      true,
		Ops:           8,
		Spans:         2,
		FallbackClass: OrderedRootSpanNativeFallbackClassNone,
	})
	if got := db.orderedRootSpanNativeCandidateOps.Load(); got != 8 {
		t.Fatalf("candidate ops=%d want 8", got)
	}
	if got := db.orderedRootSpanNativeEligibleOps.Load(); got != 8 {
		t.Fatalf("eligible ops=%d want 8", got)
	}
	if got := db.orderedRootSpanNativeFallbacks.Load(); got != 0 {
		t.Fatalf("fallbacks=%d want 0", got)
	}
	if got := db.orderedRootSpanNativeFallbackReasonCounts[FlushSpanRunFallbackUnknown].Load(); got != 0 {
		t.Fatalf("unknown fallback count=%d want 0", got)
	}
	stats := map[string]string{}
	db.appendOrderedRootSpanNativeStats(stats)
	prefix := "treedb.publish.ordered_root_delta_group.span_native.route.delta_batch_publish."
	if got := stats[prefix+"eligible_ops_total"]; got != "8" {
		t.Fatalf("route eligible ops=%q want 8", got)
	}
	if got := stats[prefix+"fallbacks_total"]; got != "0" {
		t.Fatalf("route fallbacks=%q want 0", got)
	}
	if got := stats[prefix+"fallback.reason."+FlushSpanRunFallbackUnknown.String()+".count_total"]; got != "0" {
		t.Fatalf("route unknown fallback count=%q want 0", got)
	}
}

func TestOrderedRootSpanNativeApplyResultPreservesPrepareFailureReason(t *testing.T) {
	opts := Options{}
	db := &DB{flushAdmission: computeFlushAdmissionDecisionForHardware(&opts, 16, 6)}
	prepared := zipper.ReadOnlyPrepareResult{
		Ops:            4,
		PointOps:       4,
		ExactLeafSpans: true,
		LeafSpans: []zipper.ReadOnlyLeafSpan{{
			OpCount:      4,
			PointOpCount: 4,
			ByteCount:    128,
			PointOpStart: 0,
			PointOpEnd:   4,
		}},
	}
	result := zipper.ApplyResult{
		ReadOnlyPrepareRequested:        true,
		ReadOnlyPrepareValidationFailed: true,
		ReadOnlyPrepare:                 prepared,
	}
	db.observeOrderedRootSpanNativeApplyResult(
		OrderedRootSpanNativeRouteDeltaBatchPublish,
		"prepare validation failure",
		result,
		nil,
		FlushSpanRunFallbackSpanNativeNotImplemented.String(),
	)

	stats := map[string]string{}
	db.appendOrderedRootSpanNativeStats(stats)
	requireOrderedRootStatCounterPositive(t, stats, "treedb.publish.ordered_root_delta_group.span_native.fallback.reason."+FlushSpanRunFallbackValidationFailed.String()+".ops_total")
	requireOrderedRootStatCounterZero(t, stats, "treedb.publish.ordered_root_delta_group.span_native.fallback.reason."+FlushSpanRunFallbackSpanNativeNotImplemented.String()+".ops_total")
	routePrefix := "treedb.publish.ordered_root_delta_group.span_native.route.delta_batch_publish."
	requireOrderedRootStatCounterPositive(t, stats, routePrefix+"fallback.reason."+FlushSpanRunFallbackValidationFailed.String()+".ops_total")
	requireOrderedRootStatCounterZero(t, stats, routePrefix+"fallback.reason."+FlushSpanRunFallbackSpanNativeNotImplemented.String()+".ops_total")

	errorDB := &DB{flushAdmission: computeFlushAdmissionDecisionForHardware(&opts, 16, 6)}
	errorDB.observeOrderedRootSpanNativeApplyResult(
		OrderedRootSpanNativeRouteDeltaBatchPublish,
		"prepare read failure",
		zipper.ApplyResult{
			ReadOnlyPrepareRequested: true,
			ReadOnlyPrepareFailed:    true,
			ReadOnlyPrepare:          prepared,
		},
		errors.New("prepare failed"),
		FlushSpanRunFallbackSpanNativeNotImplemented.String(),
	)
	errorStats := map[string]string{}
	errorDB.appendOrderedRootSpanNativeStats(errorStats)
	requireOrderedRootStatCounterPositive(t, errorStats, "treedb.publish.ordered_root_delta_group.span_native.fallback.reason."+FlushSpanRunFallbackPrepareError.String()+".ops_total")
	requireOrderedRootStatCounterZero(t, errorStats, "treedb.publish.ordered_root_delta_group.span_native.fallback.reason."+FlushSpanRunFallbackSpanNativeNotImplemented.String()+".ops_total")
}

func TestOrderedRootSpanNativeApplyResultDoesNotClassifyPostPrepareApplyErrorAsPrepareError(t *testing.T) {
	opts := Options{
		FlushAdmissionPolicy:  FlushAdmissionPolicyExplicit,
		FlushApplyConcurrency: 2,
		FlushApplyMinEntries:  1,
		FlushApplyMinSpans:    1,
		FlushApplyMinBytes:    1,
		FlushApplySpanNative:  true,
	}
	db := &DB{flushAdmission: computeFlushAdmissionDecisionForHardware(&opts, 16, 6)}
	prepared := zipper.ReadOnlyPrepareResult{
		Ops:            4,
		PointOps:       4,
		ExactLeafSpans: true,
		LeafSpans: []zipper.ReadOnlyLeafSpan{{
			OpCount:      4,
			PointOpCount: 4,
			ByteCount:    128,
		}},
	}
	db.observeOrderedRootSpanNativeApplyResult(
		OrderedRootSpanNativeRouteDeltaBatchPublish,
		"post-prepare apply failure",
		zipper.ApplyResult{
			ReadOnlyPrepareRequested: true,
			ReadOnlyPrepare:          prepared,
		},
		errors.New("apply failed after prepare"),
		FlushSpanRunFallbackSpanNativeNotImplemented.String(),
	)
	stats := map[string]string{}
	db.appendOrderedRootSpanNativeStats(stats)
	requireOrderedRootStatCounterZero(t, stats, "treedb.publish.ordered_root_delta_group.span_native.fallback.reason."+FlushSpanRunFallbackPrepareError.String()+".ops_total")
	requireOrderedRootStatCounterZero(t, stats, "treedb.publish.ordered_root_delta_group.span_native.fallback.reason."+FlushSpanRunFallbackSpanNativeNotImplemented.String()+".ops_total")
	requireOrderedRootStatCounterPositive(t, stats, "treedb.publish.ordered_root_delta_group.span_native.fallback.reason."+FlushSpanRunFallbackUnknown.String()+".ops_total")
	routePrefix := "treedb.publish.ordered_root_delta_group.span_native.route.delta_batch_publish."
	requireOrderedRootStatCounterZero(t, stats, routePrefix+"fallback.reason."+FlushSpanRunFallbackPrepareError.String()+".ops_total")
	requireOrderedRootStatCounterZero(t, stats, routePrefix+"fallback.reason."+FlushSpanRunFallbackSpanNativeNotImplemented.String()+".ops_total")
	requireOrderedRootStatCounterPositive(t, stats, routePrefix+"fallback.reason."+FlushSpanRunFallbackUnknown.String()+".ops_total")
}

func TestOrderedRootSpanNativeApplyResultPreservesBarrierFallbackReason(t *testing.T) {
	opts := Options{}
	db := &DB{flushAdmission: computeFlushAdmissionDecisionForHardware(&opts, 16, 6)}
	prepared := zipper.ReadOnlyPrepareResult{
		Ops:            1,
		DeleteRanges:   1,
		ExactLeafSpans: true,
		LeafSpans: []zipper.ReadOnlyLeafSpan{{
			OpCount:          1,
			DeleteRangeCount: 1,
			DeleteRangeStart: 0,
			DeleteRangeEnd:   1,
			ByteCount:        128,
		}},
	}
	db.observeOrderedRootSpanNativeApplyResult(
		OrderedRootSpanNativeRouteDeltaBatchPublish,
		"range delete barrier",
		zipper.ApplyResult{
			ReadOnlyPrepareRequested: true,
			ReadOnlyPrepare:          prepared,
		},
		nil,
		FlushSpanRunFallbackSpanNativeNotImplemented.String(),
	)

	stats := map[string]string{}
	db.appendOrderedRootSpanNativeStats(stats)
	routePrefix := "treedb.publish.ordered_root_delta_group.span_native.route.delta_batch_publish."
	requireOrderedRootStatCounterPositive(t, stats, "treedb.publish.ordered_root_delta_group.span_native.fallback.reason."+FlushSpanRunFallbackRangeDeleteBarrier.String()+".ops_total")
	requireOrderedRootStatCounterZero(t, stats, "treedb.publish.ordered_root_delta_group.span_native.fallback.reason."+FlushSpanRunFallbackSpanNativeNotImplemented.String()+".ops_total")
	requireOrderedRootStatCounterPositive(t, stats, routePrefix+"fallback.reason."+FlushSpanRunFallbackRangeDeleteBarrier.String()+".ops_total")
	requireOrderedRootStatCounterZero(t, stats, routePrefix+"fallback.reason."+FlushSpanRunFallbackSpanNativeNotImplemented.String()+".ops_total")
}

func TestOrderedRootSpanNativeApplyResultPreservesInexactLeafFallbackReason(t *testing.T) {
	opts := Options{}
	db := &DB{flushAdmission: computeFlushAdmissionDecisionForHardware(&opts, 16, 6)}
	prepared := zipper.ReadOnlyPrepareResult{
		Ops:      2,
		PointOps: 2,
		LeafSpans: []zipper.ReadOnlyLeafSpan{{
			OpCount:      2,
			PointOpCount: 2,
			PointOpStart: 0,
			PointOpEnd:   2,
			ByteCount:    128,
		}},
	}
	db.observeOrderedRootSpanNativeApplyResult(
		OrderedRootSpanNativeRouteDeltaBatchPublish,
		"inexact leaf spans",
		zipper.ApplyResult{
			ReadOnlyPrepareRequested: true,
			ReadOnlyPrepare:          prepared,
		},
		nil,
		FlushSpanRunFallbackSpanNativeNotImplemented.String(),
	)

	stats := map[string]string{}
	db.appendOrderedRootSpanNativeStats(stats)
	routePrefix := "treedb.publish.ordered_root_delta_group.span_native.route.delta_batch_publish."
	requireOrderedRootStatCounterPositive(t, stats, "treedb.publish.ordered_root_delta_group.span_native.fallback.reason."+FlushSpanRunFallbackInexactLeafSpans.String()+".ops_total")
	requireOrderedRootStatCounterZero(t, stats, "treedb.publish.ordered_root_delta_group.span_native.fallback.reason."+FlushSpanRunFallbackSpanNativeNotImplemented.String()+".ops_total")
	requireOrderedRootStatCounterPositive(t, stats, routePrefix+"fallback.reason."+FlushSpanRunFallbackInexactLeafSpans.String()+".ops_total")
	requireOrderedRootStatCounterZero(t, stats, routePrefix+"fallback.reason."+FlushSpanRunFallbackSpanNativeNotImplemented.String()+".ops_total")
}

func TestOrderedRootSpanNativeExplicitBatchRouteCounters(t *testing.T) {
	dir := t.TempDir()
	db := openOrderedRootSpanNativeRouteCounterDB(t, dir)
	defer func() { _ = db.Close() }()

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
			BaseRoot:          baseRoot,
			Delta:             update,
			SpanNativeRoute:   OrderedRootSpanNativeRouteCollectionBufferedRoots,
			SpanNativeContext: "test collection buffered root",
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

	secondUpdateIter := mustFrozenSystemMemtable(t, "doc/3", "second-update").NewIterator(nil, nil)
	secondUpdate, err := OrderedRootDeltaBatchFromIterator(secondUpdateIter)
	_ = secondUpdateIter.Close()
	if err != nil {
		t.Fatalf("second OrderedRootDeltaBatchFromIterator: %v", err)
	}
	defer func() { _ = secondUpdate.Close() }()

	_, secondRootIDs, err := db.PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder(
		[]OrderedRootDeltaBatchPublishInput{{
			BaseRoot:          rootIDs[0],
			Delta:             secondUpdate,
			SpanNativeRoute:   OrderedRootSpanNativeRouteCollectionBufferedRoots,
			SpanNativeContext: "test collection buffered root second publish",
		}},
		func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			return mustFrozenSystemMemtable(t, "sys/collections/users/root", strconv.FormatUint(rootIDs[0], 10)).NewIterator(nil, nil), nil
		},
	)
	if err != nil {
		t.Fatalf("second PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder: %v", err)
	}
	if len(secondRootIDs) != 1 || secondRootIDs[0] == 0 {
		t.Fatalf("secondRootIDs=%v, want one non-zero root", secondRootIDs)
	}

	stats := db.Stats()
	collectionPrefix := "treedb.publish.ordered_root_delta_group.span_native.route.collection_buffered_roots."
	requireOrderedRootStatCounterPositive(t, stats, collectionPrefix+"observations_total")
	requireOrderedRootStatCounterPositive(t, stats, collectionPrefix+"candidate_ops_total")
	requireOrderedRootStatCounterPositive(t, stats, collectionPrefix+"eligible_ops_total")
	requireOrderedRootStatCounterPositive(t, stats, collectionPrefix+"used_ops_total")
	requireOrderedRootStatCounterZero(t, stats, collectionPrefix+"fallback.reason."+FlushSpanRunFallbackSpanNativeNotImplemented.String()+".ops_total")
	systemPrefix := "treedb.publish.ordered_root_delta_group.span_native.route.system_delta_builder_publish."
	requireOrderedRootStatCounterPositive(t, stats, systemPrefix+"observations_total")
	requireOrderedRootStatCounterPositive(t, stats, systemPrefix+"candidate_ops_total")
	requireOrderedRootStatCounterPositive(t, stats, systemPrefix+"used_ops_total")
	requireOrderedRootStatCounterZero(t, stats, "treedb.publish.ordered_root_delta_group.span_native.route.multi_index_group_publish.candidate_ops_total")
	requireOrderedRootStatCounterZero(t, stats, "treedb.publish.ordered_root_delta_group.span_native.route.delta_batch_publish.candidate_ops_total")
}

func TestOrderedRootSpanNativeIteratorDeltaGroupRouteCounters(t *testing.T) {
	dir := t.TempDir()
	db := openOrderedRootSpanNativeRouteCounterDB(t, dir)
	defer func() { _ = db.Close() }()

	baseRoot, err := db.PublishOrderedRootIterator(0, mustFrozenSystemMemtable(t, "doc/1", "base").NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("PublishOrderedRootIterator: %v", err)
	}
	_, rootIDs, err := db.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder(
		[]OrderedRootDeltaPublishInput{{
			BaseRoot: baseRoot,
			Iter:     mustFrozenSystemMemtable(t, "doc/2", "update").NewIterator(nil, nil),
		}},
		func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			return mustFrozenSystemMemtable(t, "sys/collections/users/root", strconv.FormatUint(rootIDs[0], 10)).NewIterator(nil, nil), nil
		},
	)
	if err != nil {
		t.Fatalf("PublishOrderedRootDeltaGroupWithSystemDeltaBuilder: %v", err)
	}
	if len(rootIDs) != 1 || rootIDs[0] == 0 {
		t.Fatalf("rootIDs=%v, want one non-zero root", rootIDs)
	}

	_, secondRootIDs, err := db.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder(
		[]OrderedRootDeltaPublishInput{{
			BaseRoot: rootIDs[0],
			Iter:     mustFrozenSystemMemtable(t, "doc/3", "second-update").NewIterator(nil, nil),
		}},
		func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			return mustFrozenSystemMemtable(t, "sys/collections/users/root", strconv.FormatUint(rootIDs[0], 10)).NewIterator(nil, nil), nil
		},
	)
	if err != nil {
		t.Fatalf("second PublishOrderedRootDeltaGroupWithSystemDeltaBuilder: %v", err)
	}
	if len(secondRootIDs) != 1 || secondRootIDs[0] == 0 {
		t.Fatalf("secondRootIDs=%v, want one non-zero root", secondRootIDs)
	}

	stats := db.Stats()
	multiIndexPrefix := "treedb.publish.ordered_root_delta_group.span_native.route.multi_index_group_publish."
	requireOrderedRootStatCounterPositive(t, stats, multiIndexPrefix+"observations_total")
	requireOrderedRootStatCounterPositive(t, stats, multiIndexPrefix+"candidate_ops_total")
	requireOrderedRootStatCounterPositive(t, stats, multiIndexPrefix+"eligible_ops_total")
	requireOrderedRootStatCounterPositive(t, stats, multiIndexPrefix+"used_ops_total")
	requireOrderedRootStatCounterZero(t, stats, multiIndexPrefix+"fallback.reason."+FlushSpanRunFallbackSpanNativeNotImplemented.String()+".ops_total")
	systemPrefix := "treedb.publish.ordered_root_delta_group.span_native.route.system_delta_builder_publish."
	requireOrderedRootStatCounterPositive(t, stats, systemPrefix+"observations_total")
	requireOrderedRootStatCounterPositive(t, stats, systemPrefix+"candidate_ops_total")
	requireOrderedRootStatCounterPositive(t, stats, systemPrefix+"used_ops_total")
	requireOrderedRootStatCounterZero(t, stats, systemPrefix+"fallback.reason."+FlushSpanRunFallbackSpanNativeNotImplemented.String()+".ops_total")
	requireOrderedRootStatCounterZero(t, stats, "treedb.publish.ordered_root_delta_group.span_native.route.collection_buffered_roots.candidate_ops_total")
	requireOrderedRootStatCounterZero(t, stats, "treedb.publish.ordered_root_delta_group.span_native.route.command_wal_publish.candidate_ops_total")
	requireOrderedRootStatCounterZero(t, stats, "treedb.publish.ordered_root_delta_group.span_native.route.delta_batch_publish.candidate_ops_total")
}

func TestOrderedRootSpanNativeIteratorDeltaGroupColdBuildRouteCounters(t *testing.T) {
	dir := t.TempDir()
	db := openOrderedRootSpanNativeRouteCounterDB(t, dir)
	defer func() { _ = db.Close() }()

	systemRoot, rootIDs, err := db.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder(
		[]OrderedRootDeltaPublishInput{{
			BaseRoot: 0,
			Iter: &stableRootDeltaIterator{entries: []stableRootDeltaEntry{{
				key:   []byte("doc/1"),
				value: []byte("base"),
			}}},
		}},
		func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			return mustFrozenSystemMemtable(t, "sys/collections/users/root", strconv.FormatUint(rootIDs[0], 10)).NewIterator(nil, nil), nil
		},
	)
	if err != nil {
		t.Fatalf("PublishOrderedRootDeltaGroupWithSystemDeltaBuilder: %v", err)
	}
	if systemRoot == 0 || len(rootIDs) != 1 || rootIDs[0] == 0 {
		t.Fatalf("systemRoot=%d rootIDs=%v, want non-zero roots", systemRoot, rootIDs)
	}

	stats := db.Stats()
	overlayPrefix := "treedb.publish.ordered_root_delta_group.span_native.route.overlay_cold_build."
	requireOrderedRootStatCounterPositive(t, stats, overlayPrefix+"observations_total")
	requireOrderedRootStatCounterPositive(t, stats, overlayPrefix+"fallback.reason."+FlushSpanRunFallbackColdBuild.String()+".count_total")
	requireOrderedRootStatCounterPositive(t, stats, overlayPrefix+"fallback.reason."+FlushSpanRunFallbackColdBuild.String()+".ops_total")
	requireOrderedRootStatCounterZero(t, stats, "treedb.publish.ordered_root_delta_group.span_native.route.multi_index_group_publish.observations_total")
	requireOrderedRootStatCounterZero(t, stats, "treedb.publish.ordered_root_delta_group.span_native.route.delta_batch_publish.observations_total")
}

func TestOrderedRootSpanNativeIteratorCommandWALRouteCounters(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openOrderedRootSpanNativeRouteCounterDB(t, dir)
	defer func() { _ = db.Close() }()

	_, rootIDs, err := db.PublishOrderedRootDeltaGroupWithCommandWALContextAndSystemDeltaBuilder(
		[]OrderedRootDeltaPublishInput{{
			BaseRoot: 0,
			Iter:     mustFrozenSystemMemtable(t, "doc/1", "base").NewIterator(nil, nil),
		}},
		mustRawKVCommandWALIntent(t, db, "cmd/1", "1"),
		func(ctx CommandWALPublishContext, rootIDs []uint64) (iterator.UnsafeIterator, error) {
			return mustFrozenSystemMemtable(t, "sys/collections/users/root", strconv.FormatUint(rootIDs[0], 10)).NewIterator(nil, nil), nil
		},
	)
	if err != nil {
		t.Fatalf("PublishOrderedRootDeltaGroupWithCommandWALContextAndSystemDeltaBuilder: %v", err)
	}
	if len(rootIDs) != 1 || rootIDs[0] == 0 {
		t.Fatalf("rootIDs=%v, want one non-zero root", rootIDs)
	}

	_, secondRootIDs, err := db.PublishOrderedRootDeltaGroupWithCommandWALContextAndSystemDeltaBuilder(
		[]OrderedRootDeltaPublishInput{{
			BaseRoot: rootIDs[0],
			Iter:     mustFrozenSystemMemtable(t, "doc/2", "update").NewIterator(nil, nil),
		}},
		mustRawKVCommandWALIntent(t, db, "cmd/2", "1"),
		func(ctx CommandWALPublishContext, rootIDs []uint64) (iterator.UnsafeIterator, error) {
			return mustFrozenSystemMemtable(t, "sys/collections/users/root", strconv.FormatUint(rootIDs[0], 10)).NewIterator(nil, nil), nil
		},
	)
	if err != nil {
		t.Fatalf("second PublishOrderedRootDeltaGroupWithCommandWALContextAndSystemDeltaBuilder: %v", err)
	}
	if len(secondRootIDs) != 1 || secondRootIDs[0] == 0 {
		t.Fatalf("secondRootIDs=%v, want one non-zero root", secondRootIDs)
	}

	stats := db.Stats()
	commandWALPrefix := "treedb.publish.ordered_root_delta_group.span_native.route.command_wal_publish."
	requireOrderedRootStatCounterPositive(t, stats, commandWALPrefix+"observations_total")
	requireOrderedRootStatCounterPositive(t, stats, commandWALPrefix+"candidate_ops_total")
	requireOrderedRootStatCounterPositive(t, stats, commandWALPrefix+"eligible_ops_total")
	requireOrderedRootStatCounterPositive(t, stats, commandWALPrefix+"used_ops_total")
	requireOrderedRootStatCounterZero(t, stats, commandWALPrefix+"fallback.reason."+FlushSpanRunFallbackSpanNativeNotImplemented.String()+".ops_total")
	requireOrderedRootStatCounterZero(t, stats, "treedb.publish.ordered_root_delta_group.span_native.route.collection_buffered_roots.candidate_ops_total")
	requireOrderedRootStatCounterZero(t, stats, "treedb.publish.ordered_root_delta_group.span_native.route.multi_index_group_publish.candidate_ops_total")
	requireOrderedRootStatCounterZero(t, stats, "treedb.publish.ordered_root_delta_group.span_native.route.system_delta_builder_publish.candidate_ops_total")
	requireOrderedRootStatCounterZero(t, stats, "treedb.publish.ordered_root_delta_group.span_native.route.delta_batch_publish.candidate_ops_total")
}

func openOrderedRootSpanNativeRouteCounterDB(t *testing.T, dir string) *DB {
	t.Helper()
	db, err := Open(Options{
		Dir:                    dir,
		FlushAdmissionPolicy:   FlushAdmissionPolicyExplicit,
		FlushApplyConcurrency:  2,
		FlushApplyMinEntries:   1,
		FlushApplyMinSpans:     1,
		FlushApplyMinBytes:     1,
		FlushApplySpanNative:   true,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	return db
}

func TestOrderedRootSpanNativeEligibilityDistinguishesAdmissionFallbacks(t *testing.T) {
	summary := orderedRootSafeSpanSummary(4, 2)
	offOpts := Options{FlushAdmissionPolicy: FlushAdmissionPolicyOff, FlushApplySpanNative: true, FlushApplyConcurrency: 4}
	off := &DB{flushAdmission: computeFlushAdmissionDecisionForHardware(&offOpts, 16, 6)}
	offRow := off.orderedRootSpanNativeEligibility(orderedRootSpanNativeEligibilityRequest{
		Route:   OrderedRootSpanNativeRouteDeltaBatchPublish,
		Context: "off policy",
		Summary: summary,
	})
	if offRow.FallbackReason != FlushSpanRunFallbackDisabled.String() || offRow.FallbackClass != OrderedRootSpanNativeFallbackClassPolicy {
		t.Fatalf("off fallback row=%+v want disabled policy fallback", offRow)
	}
	offExplicitRow := off.orderedRootSpanNativeEligibility(orderedRootSpanNativeEligibilityRequest{
		Route:                  OrderedRootSpanNativeRouteDeltaBatchPublish,
		Context:                "off policy explicit fallback",
		Summary:                summary,
		ExplicitFallbackReason: FlushSpanRunFallbackSpanNativeNotImplemented.String(),
	})
	if offExplicitRow.FallbackReason != FlushSpanRunFallbackDisabled.String() || offExplicitRow.FallbackClass != OrderedRootSpanNativeFallbackClassPolicy {
		t.Fatalf("off explicit fallback row=%+v want disabled policy fallback", offExplicitRow)
	}

	declineOpts := Options{FlushAdmissionPolicy: FlushAdmissionPolicyAuto, FlushApplyConcurrency: 1, FlushApplySpanNative: true}
	declined := &DB{flushAdmission: computeFlushAdmissionDecisionForHardware(&declineOpts, 4, 4)}
	declineRow := declined.orderedRootSpanNativeEligibility(orderedRootSpanNativeEligibilityRequest{
		Route:   OrderedRootSpanNativeRouteDeltaBatchPublish,
		Context: "auto decline",
		Summary: summary,
	})
	if declineRow.FallbackReason != FlushSpanRunFallbackAdmissionPolicyDecline.String() || declineRow.FallbackClass != OrderedRootSpanNativeFallbackClassPolicy {
		t.Fatalf("auto decline row=%+v want admission-policy decline", declineRow)
	}
	declineExplicitRow := declined.orderedRootSpanNativeEligibility(orderedRootSpanNativeEligibilityRequest{
		Route:                  OrderedRootSpanNativeRouteDeltaBatchPublish,
		Context:                "auto decline explicit fallback",
		Summary:                summary,
		ExplicitFallbackReason: FlushSpanRunFallbackSpanNativeNotImplemented.String(),
	})
	if declineExplicitRow.FallbackReason != FlushSpanRunFallbackAdmissionPolicyDecline.String() || declineExplicitRow.FallbackClass != OrderedRootSpanNativeFallbackClassPolicy {
		t.Fatalf("auto decline explicit row=%+v want admission-policy decline", declineExplicitRow)
	}

	parallelOnlyOpts := Options{
		FlushAdmissionPolicy:  FlushAdmissionPolicyExplicit,
		FlushApplyConcurrency: 4,
		FlushApplySpanNative:  false,
		FlushApplyMinEntries:  1,
		FlushApplyMinSpans:    1,
		FlushApplyMinBytes:    1,
	}
	parallelOnly := &DB{flushAdmission: computeFlushAdmissionDecisionForHardware(&parallelOnlyOpts, 16, 6)}
	parallelOnlyRow := parallelOnly.orderedRootSpanNativeEligibility(orderedRootSpanNativeEligibilityRequest{
		Route:              OrderedRootSpanNativeRouteDeltaBatchPublish,
		Context:            "parallel-only span-native disabled",
		Summary:            summary,
		SpanNativeEligible: false,
	})
	if !parallelOnlyRow.AdmissionAdmitted {
		t.Fatalf("parallel-only row admission_admitted=false, want admitted policy context: %+v", parallelOnlyRow)
	}
	if parallelOnlyRow.Eligible || parallelOnlyRow.Status != OrderedRootSpanNativeStatusFallback {
		t.Fatalf("parallel-only row eligible/status=%t/%q want fallback: %+v", parallelOnlyRow.Eligible, parallelOnlyRow.Status, parallelOnlyRow)
	}
	if parallelOnlyRow.FallbackReason != FlushSpanRunFallbackDisabled.String() || parallelOnlyRow.FallbackClass != OrderedRootSpanNativeFallbackClassPolicy {
		t.Fatalf("parallel-only row=%+v want disabled policy fallback", parallelOnlyRow)
	}

	admittedOpts := Options{}
	admitted := &DB{flushAdmission: computeFlushAdmissionDecisionForHardware(&admittedOpts, 16, 6)}
	routeRow := admitted.orderedRootSpanNativeEligibility(orderedRootSpanNativeEligibilityRequest{
		Route:   OrderedRootSpanNativeRouteDirectPublish,
		Context: "direct publish",
		Summary: summary,
	})
	if routeRow.FallbackReason != FlushSpanRunFallbackRouteIneligible.String() || routeRow.FallbackClass != OrderedRootSpanNativeFallbackClassRoute {
		t.Fatalf("route-ineligible row=%+v want route fallback", routeRow)
	}
}

func TestOrderedRootSpanNativeEligibilityNamesUnsupportedFallbacks(t *testing.T) {
	opts := Options{}
	db := &DB{flushAdmission: computeFlushAdmissionDecisionForHardware(&opts, 16, 6)}
	base := orderedRootSafeSpanSummary(8, 2)
	cases := []struct {
		name   string
		req    orderedRootSpanNativeEligibilityRequest
		reason FlushSpanRunFallbackReason
	}{
		{
			name:   "validation failure",
			req:    orderedRootSpanNativeEligibilityRequest{Route: OrderedRootSpanNativeRouteDeltaBatchPublish, Summary: base, ReadOnlyPrepareValidationFail: true},
			reason: FlushSpanRunFallbackValidationFailed,
		},
		{
			name: "range delete barrier",
			req: orderedRootSpanNativeEligibilityRequest{Route: OrderedRootSpanNativeRouteDeltaBatchPublish, Summary: func() zipper.ReadOnlyLeafSpanSummary {
				s := base
				s.DeleteRanges = 1
				s.ExactLeafSpans = false
				return s
			}()},
			reason: FlushSpanRunFallbackRangeDeleteBarrier,
		},
		{
			name: "cold build",
			req: orderedRootSpanNativeEligibilityRequest{Route: OrderedRootSpanNativeRouteOverlayColdBuild, Summary: func() zipper.ReadOnlyLeafSpanSummary {
				s := base
				s.ColdBuild = true
				return s
			}()},
			reason: FlushSpanRunFallbackColdBuild,
		},
		{
			name: "maintenance",
			req: orderedRootSpanNativeEligibilityRequest{Route: OrderedRootSpanNativeRouteDeltaBatchPublish, Summary: func() zipper.ReadOnlyLeafSpanSummary {
				s := base
				s.Maintenance = true
				s.PointOps = 0
				s.ExactLeafSpans = false
				return s
			}()},
			reason: FlushSpanRunFallbackMaintenance,
		},
		{
			name: "inexact spans",
			req: orderedRootSpanNativeEligibilityRequest{Route: OrderedRootSpanNativeRouteDeltaBatchPublish, Summary: func() zipper.ReadOnlyLeafSpanSummary {
				s := base
				s.ExactLeafSpans = false
				return s
			}()},
			reason: FlushSpanRunFallbackInexactLeafSpans,
		},
		{
			name:   "output ownership",
			req:    orderedRootSpanNativeEligibilityRequest{Route: OrderedRootSpanNativeRouteDeltaBatchPublish, Summary: base, ExplicitFallbackReason: FlushSpanRunFallbackOutputOwnershipFailure.String()},
			reason: FlushSpanRunFallbackOutputOwnershipFailure,
		},
		{
			name:   "reducer validation",
			req:    orderedRootSpanNativeEligibilityRequest{Route: OrderedRootSpanNativeRouteDeltaBatchPublish, Summary: base, ExplicitFallbackReason: FlushSpanRunFallbackReducerValidationFailed.String()},
			reason: FlushSpanRunFallbackReducerValidationFailed,
		},
		{
			name:   "root mismatch",
			req:    orderedRootSpanNativeEligibilityRequest{Route: OrderedRootSpanNativeRouteDeltaBatchPublish, Summary: base, ExplicitFallbackReason: FlushSpanRunFallbackRootMismatch.String()},
			reason: FlushSpanRunFallbackRootMismatch,
		},
		{
			name:   "memory cap",
			req:    orderedRootSpanNativeEligibilityRequest{Route: OrderedRootSpanNativeRouteDeltaBatchPublish, Summary: base, ExplicitFallbackReason: FlushSpanRunFallbackMemoryEmergencyCap.String()},
			reason: FlushSpanRunFallbackMemoryEmergencyCap,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			row := db.orderedRootSpanNativeEligibility(tc.req)
			if row.FallbackReason != tc.reason.String() {
				t.Fatalf("fallback=%q want %q (row=%+v)", row.FallbackReason, tc.reason, row)
			}
			if row.FallbackReason == "" || row.FallbackReason == FlushSpanRunFallbackUnknown.String() {
				t.Fatalf("unsupported fallback must be named, got row=%+v", row)
			}
		})
	}
}

func TestOrderedRootSpanNativeTriageSnapshotCoversSupportRoutes(t *testing.T) {
	opts := Options{}
	db := &DB{flushAdmission: computeFlushAdmissionDecisionForHardware(&opts, 16, 6)}
	rows := db.OrderedRootSpanNativeTriageSnapshot()
	required := map[OrderedRootSpanNativeRoute]bool{
		OrderedRootSpanNativeRouteDirectPublish:             false,
		OrderedRootSpanNativeRouteGroupedPublish:            false,
		OrderedRootSpanNativeRouteSystemDeltaBuilderPublish: false,
		OrderedRootSpanNativeRouteCommandWALPublish:         false,
		OrderedRootSpanNativeRouteCollectionBufferedRoots:   false,
		OrderedRootSpanNativeRouteOverlayColdBuild:          false,
		OrderedRootSpanNativeRouteMultiIndexGroupPublish:    false,
		OrderedRootSpanNativeRouteDeltaBatchPublish:         false,
		OrderedRootSpanNativeRouteReadOnlyPrepare:           false,
	}
	for _, row := range rows {
		if _, ok := required[row.Route]; !ok {
			t.Fatalf("unexpected triage route %q", row.Route)
		}
		required[row.Route] = true
		if row.Context == "" || row.RouteSupportDetail == "" {
			t.Fatalf("triage row %q missing context/detail: %+v", row.Route, row)
		}
		if row.Status == OrderedRootSpanNativeStatusEligible {
			if !row.Eligible || row.FallbackReason != "" || row.FallbackClass != OrderedRootSpanNativeFallbackClassNone {
				t.Fatalf("eligible triage row %q has fallback state: %+v", row.Route, row)
			}
		} else if row.FallbackReason == "" || row.FallbackReason == FlushSpanRunFallbackUnknown.String() {
			t.Fatalf("triage row %q has unsupported fallback reason %q", row.Route, row.FallbackReason)
		}
		if row.AdmissionPolicy == "" || row.AdmissionReason == "" {
			t.Fatalf("triage row %q missing admission context: %+v", row.Route, row)
		}
	}
	for route, seen := range required {
		if !seen {
			t.Fatalf("missing ordered-root triage route %q", route)
		}
	}
	byRoute := orderedRootSpanNativeTriageRowsByRoute(rows)
	if got := byRoute[OrderedRootSpanNativeRouteDirectPublish].FallbackReason; got != FlushSpanRunFallbackRouteIneligible.String() {
		t.Fatalf("direct publish fallback=%q want route_ineligible", got)
	}
	if got := byRoute[OrderedRootSpanNativeRouteOverlayColdBuild].FallbackReason; got != FlushSpanRunFallbackColdBuild.String() {
		t.Fatalf("overlay cold fallback=%q want cold_build", got)
	}
	if got := byRoute[OrderedRootSpanNativeRouteCommandWALPublish].Status; got != OrderedRootSpanNativeStatusEligible {
		t.Fatalf("command-WAL status=%q want eligible", got)
	}
	if got := byRoute[OrderedRootSpanNativeRouteCommandWALPublish].FallbackReason; got != "" {
		t.Fatalf("command-WAL fallback=%q want empty", got)
	}
}

func TestOrderedRootSpanNativeTriageSnapshotRespectsSpanNativeOptOut(t *testing.T) {
	opts := Options{
		FlushAdmissionPolicy:  FlushAdmissionPolicyExplicit,
		FlushApplyConcurrency: 4,
		FlushApplySpanNative:  false,
		FlushApplyMinEntries:  1,
		FlushApplyMinSpans:    1,
		FlushApplyMinBytes:    1,
	}
	db := &DB{flushAdmission: computeFlushAdmissionDecisionForHardware(&opts, 16, 6)}
	rows := orderedRootSpanNativeTriageRowsByRoute(db.OrderedRootSpanNativeTriageSnapshot())
	for _, route := range []OrderedRootSpanNativeRoute{
		OrderedRootSpanNativeRouteCommandWALPublish,
		OrderedRootSpanNativeRouteDeltaBatchPublish,
		OrderedRootSpanNativeRouteReadOnlyPrepare,
	} {
		row := rows[route]
		if !row.Candidate {
			t.Fatalf("%s candidate=false, want candidate support row: %+v", route, row)
		}
		if !row.AdmissionAdmitted {
			t.Fatalf("%s admission_admitted=false, want admitted explicit policy context: %+v", route, row)
		}
		if row.Eligible || row.Status != OrderedRootSpanNativeStatusFallback {
			t.Fatalf("%s eligible/status=%t/%q want fallback: %+v", route, row.Eligible, row.Status, row)
		}
		if row.FallbackReason != FlushSpanRunFallbackDisabled.String() || row.FallbackClass != OrderedRootSpanNativeFallbackClassPolicy {
			t.Fatalf("%s row=%+v want disabled policy fallback", route, row)
		}
	}
}

func TestOrderedRootSpanNativeStatsExposeSupportTriageSnapshot(t *testing.T) {
	opts := Options{FlushAdmissionPolicy: FlushAdmissionPolicyOff, FlushApplySpanNative: true, FlushApplyConcurrency: 4}
	db := &DB{flushAdmission: computeFlushAdmissionDecisionForHardware(&opts, 16, 6)}
	stats := map[string]string{}
	db.appendOrderedRootSpanNativeStats(stats)
	prefix := "treedb.publish.ordered_root_delta_group.span_native.triage.route.delta_batch_publish."
	if got := stats[prefix+"status"]; got != string(OrderedRootSpanNativeStatusFallback) {
		t.Fatalf("delta batch triage status=%q want fallback", got)
	}
	if got := stats[prefix+"fallback_reason"]; got != FlushSpanRunFallbackDisabled.String() {
		t.Fatalf("delta batch triage fallback=%q want disabled", got)
	}
	if got := stats[prefix+"fallback_class"]; got != string(OrderedRootSpanNativeFallbackClassPolicy) {
		t.Fatalf("delta batch triage class=%q want policy", got)
	}
	if got := stats[prefix+"admission_policy"]; got != FlushAdmissionPolicyOff.String() {
		t.Fatalf("delta batch triage admission policy=%q want off", got)
	}
	if got := stats[prefix+"admission_reason"]; got != FlushAdmissionReasonPolicyOff {
		t.Fatalf("delta batch triage admission reason=%q want policy_off", got)
	}
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

func orderedRootSafeSpanSummary(ops, spans int) zipper.ReadOnlyLeafSpanSummary {
	return zipper.ReadOnlyLeafSpanSummary{
		Ops:            ops,
		PointOps:       ops,
		SpanOps:        ops,
		Spans:          spans,
		ExactLeafSpans: true,
	}
}

func orderedRootSpanNativeTriageRowsByRoute(rows []OrderedRootSpanNativeTriageRow) map[OrderedRootSpanNativeRoute]OrderedRootSpanNativeTriageRow {
	out := make(map[OrderedRootSpanNativeRoute]OrderedRootSpanNativeTriageRow, len(rows))
	for _, row := range rows {
		out[row.Route] = row
	}
	return out
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
