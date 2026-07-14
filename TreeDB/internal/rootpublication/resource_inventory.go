package rootpublication

import "fmt"

// StableProducerDomain is the concrete construction owner for an exact-handle
// resource token. It is separate from ResourceKind: several producers pin the
// same physical kind, while one producer may own several related kinds.
type StableProducerDomain string

const (
	StableProducerDB             StableProducerDomain = "db"
	StableProducerValueLog       StableProducerDomain = "value-log"
	StableProducerOuterLeaf      StableProducerDomain = "outer-leaf"
	StableProducerDictionary     StableProducerDomain = "dictionary"
	StableProducerTemplate       StableProducerDomain = "template"
	StableProducerCollection     StableProducerDomain = "collection"
	StableProducerColumnAsset    StableProducerDomain = "column-asset"
	StableProducerLegacyVector   StableProducerDomain = "legacy-vector"
	StableProducerCommandWAL     StableProducerDomain = "command-wal"
	StableProducerLegacyExcluded StableProducerDomain = "legacy-excluded"
	StableProducerRaftSnapshot   StableProducerDomain = "raft-snapshot"
)

// StableResourceInventoryRow is the reviewed ownership table for every root,
// catalog, and command-frame field in the #3677 closure. Paths are source file
// paths, not durable resource identities.
type StableResourceInventoryRow struct {
	Field              ReachabilityField
	Kind               ResourceKind
	Producer           string
	StableIdentity     string
	FrontierOrDigest   string
	NamespaceOperation string
	Registrar          string
	RecoveryValidator  string
	DeletingOwner      string
	Classification     string
}

// canonicalReachabilityRequirements is the independent closure registry for
// root, catalog, and command-frame policy. The prose inventory below is a
// reviewed implementation mapping and must agree with this registry; deriving
// either side from the other would make all-but-one coverage self-referential.
var canonicalReachabilityRequirements = []struct {
	Field          ReachabilityField
	Kind           ResourceKind
	Classification string
	Producer       StableProducerDomain
}{
	{ReachabilityIndexFile, ResourceIndex, "authoritative", StableProducerDB},
	{ReachabilityMetaPage, ResourceIndex, "adjacent-root-publication", StableProducerDB},
	{ReachabilityUserRoot, ResourceIndex, "adjacent-root-publication", StableProducerDB},
	{ReachabilitySystemRoot, ResourceIndex, "adjacent-root-publication", StableProducerDB},
	{ReachabilityFreelist, ResourceIndex, "adjacent-freelist-publication", StableProducerDB},
	{ReachabilityValueLogPointer, ResourceValueLog, "authoritative", StableProducerValueLog},
	{ReachabilityOuterLeafRawPointer, ResourceOuterLeafLog, "authoritative", StableProducerOuterLeaf},
	{ReachabilityOuterLeafPackedPointer, ResourceOuterLeafPack, "authoritative", StableProducerOuterLeaf},
	{ReachabilityOuterLeafGeneration, ResourceOuterLeafManifest, "authoritative", StableProducerOuterLeaf},
	{ReachabilityDictionaryGeneration, ResourceDictionary, "authoritative-transitive", StableProducerDictionary},
	{ReachabilityTemplateGeneration, ResourceTemplate, "authoritative-transitive", StableProducerTemplate},
	{ReachabilityCollectionSystemRoot, ResourceIndex, "adjacent-root-publication", StableProducerCollection},
	{ReachabilityCollectionPrimaryRoot, ResourceIndex, "adjacent-root-publication", StableProducerCollection},
	{ReachabilityCollectionTemplateRoot, ResourceIndex, "adjacent-root-publication", StableProducerCollection},
	{ReachabilityCollectionIndexStateRoot, ResourceIndex, "adjacent-root-publication", StableProducerCollection},
	{ReachabilityCollectionColumnRoot, ResourceIndex, "adjacent-root-publication", StableProducerCollection},
	{ReachabilityCollectionSecondaryRoot, ResourceIndex, "adjacent-root-publication", StableProducerCollection},
	{ReachabilityCollectionVectorRoot, ResourceIndex, "adjacent-root-publication", StableProducerCollection},
	{ReachabilityCollectionTextDictionary, ResourceIndex, "adjacent-root-publication", StableProducerCollection},
	{ReachabilityCollectionTextPosting, ResourceIndex, "adjacent-root-publication", StableProducerCollection},
	{ReachabilityCollectionTextPosition, ResourceIndex, "adjacent-root-publication", StableProducerCollection},
	{ReachabilityColumnManifest, ResourceColumnAsset, "authoritative", StableProducerColumnAsset},
	{ReachabilityTypedColumnMultipart, ResourceTypedColumnAsset, "authoritative", StableProducerColumnAsset},
	{ReachabilityTypedColumnValue, ResourceTypedColumnAsset, "authoritative", StableProducerColumnAsset},
	{ReachabilityTypedColumnCode, ResourceTypedColumnAsset, "authoritative", StableProducerColumnAsset},
	{ReachabilityHNSWSearchPack, ResourceVectorGraphPack, "authoritative", StableProducerColumnAsset},
	{ReachabilityVectorGraphPack, ResourceVectorGraphPack, "authoritative-transitive", StableProducerColumnAsset},
	{ReachabilityLegacyVectorSnapshot, ResourceLegacyVectorSnapshot, "rebuildable-non-authoritative", StableProducerLegacyVector},
	{ReachabilityCommandWALActive, ResourceCommandWAL, "authoritative", StableProducerCommandWAL},
	{ReachabilityCommandWALRotated, ResourceCommandWAL, "authoritative", StableProducerCommandWAL},
	{ReachabilityCommandWALExternalRIDFence, ResourceCommandWALExternalRID, "authoritative-transitive", StableProducerValueLog},
	{ReachabilityQueryReadyBase, ResourceQueryReadyAsset, "rebuildable-non-authoritative", StableProducerColumnAsset},
	{ReachabilityQueryReadyDelta, ResourceQueryReadyAsset, "rebuildable-non-authoritative", StableProducerColumnAsset},
	{ReachabilityQueryReadyConsolidatedBase, ResourceQueryReadyAsset, "rebuildable-non-authoritative", StableProducerColumnAsset},
	// These are explicit policy exclusions, not unowned durability resources.
	{ReachabilityLegacyActiveSlab, ResourceLegacyTreeDBField, "explicit-legacy-exclusion", StableProducerLegacyExcluded},
	{ReachabilityRaftSnapshot, ResourceSeparateDurability, "explicit-separate-domain", StableProducerRaftSnapshot},
}

var stableResourceInventory = []StableResourceInventoryRow{
	{ReachabilityIndexFile, ResourceIndex, "TreeDB/db/system_root_publish.go; TreeDB/db/ordered_root_publish.go; TreeDB/db/index_swap.go", "pinned index.db handle + platform file ID + database generation", "required page/file byte frontier + format header", "create/rename parent directory token", "root candidate builder before installing target root IDs", "TreeDB/db/root_snapshot.go; TreeDB/pager", "TreeDB/db/index_swap.go; TreeDB/db/vacuum_online.go; TreeDB/db/vacuum_offline.go", "authoritative"},
	{ReachabilityMetaPage, ResourceIndex, "adjacent root publication issue #3679", "no independent external identity; page lives inside pinned index.db", "target meta-page frontier remains adjacent to index-file durability", "none", "not registerable by #3677", "TreeDB/db/root_snapshot.go; TreeDB/page/meta.go", "TreeDB/db/index_swap.go; downstream page owner #3681", "adjacent-root-publication"},
	{ReachabilityUserRoot, ResourceIndex, "adjacent root publication issue #3679", "no independent external identity; root lives inside pinned index.db", "user-root page frontier remains adjacent to index-file durability", "none", "not registerable by #3677", "TreeDB/db/root_snapshot.go", "downstream COW owner #3681", "adjacent-root-publication"},
	{ReachabilitySystemRoot, ResourceIndex, "adjacent root publication issue #3679", "no independent external identity; root lives inside pinned index.db", "system-root page frontier remains adjacent to index-file durability", "none", "not registerable by #3677", "TreeDB/db/root_snapshot.go; collection catalog validators", "downstream COW owner #3681", "adjacent-root-publication"},
	{ReachabilityFreelist, ResourceIndex, "adjacent freelist publication issue #3678", "no independent external identity; freelist pages live inside pinned index.db", "freelist page frontier remains adjacent to index-file durability", "none", "not registerable by #3677", "TreeDB/pager freelist recovery", "downstream COW owner #3681", "adjacent-freelist-publication"},
	{ReachabilityValueLogPointer, ResourceValueLog, "TreeDB/internal/valuelog/writer.go; TreeDB/db/value_log_appender.go; TreeDB/caching/value_log_appender.go", "pinned segment handle + platform file ID + lane/file generation", "greatest referenced byte frontier + immutable segment header", "create and rotation namespace tokens", "vlog append/rotation result before lane replacement", "TreeDB/internal/valuelog/manager.go; TreeDB/internal/valuelog/reader.go; TreeDB/db/wal_recovery.go", "TreeDB/db/vlog_gc.go; TreeDB/db/vlog_rewrite.go; TreeDB/internal/valuelog/manager.go", "authoritative"},
	{ReachabilityOuterLeafRawPointer, ResourceOuterLeafLog, "TreeDB/db/leaf_page_log.go; TreeDB/db/leaf_page_log_lanes.go; TreeDB/caching/leaf_page_log_lanes.go", "pinned raw outer-leaf segment + generation", "greatest raw block byte frontier + header", "create/rotation namespace token", "outer-leaf append/rotation result before lane replacement", "TreeDB/db/leaf_page_read_cache.go; TreeDB/internal/outerleaf/block.go", "TreeDB/db/leaf_generation_gc.go; TreeDB/internal/valuelog/manager.go", "authoritative"},
	{ReachabilityOuterLeafPackedPointer, ResourceOuterLeafPack, "TreeDB/db/leaf_generation_pack.go; TreeDB/db/leaf_generation_pack_rewrite.go", "pinned packed segment + generation", "pack byte frontier + immutable header digest", "staging-to-generation rename namespace token", "pack promotion builder before manifest installation", "TreeDB/db/leaf_page_read_cache.go; TreeDB/internal/outerleaf/block.go", "TreeDB/db/leaf_generation_gc.go; pack rewrite cleanup", "authoritative"},
	{ReachabilityOuterLeafGeneration, ResourceOuterLeafManifest, "TreeDB/db/leaf_generation_manifest.go; TreeDB/db/leaf_generation_pack_rewrite.go", "pinned generation manifest + generation ID", "manifest content digest", "create/replace namespace token", "generation builder before system-root field", "TreeDB/db/leaf_generation_manifest.go reconciliation", "TreeDB/db/leaf_generation_gc.go", "authoritative"},
	{ReachabilityDictionaryGeneration, ResourceDictionary, "TreeDB/internal/dictdb/resource_capture.go; TreeDB/caching/db.go; TreeDB/db/stable_leaf_rewrite.go; TreeDB/db/leaf_generation_pack_authority.go", "stable dictdb index generation + exact manager-owned value-log segment + dictionary ID", "dictionary digest + full captured index frontier + exact value-log record end", "dictdb index namespace proof + installed value-log namespace", "CaptureDictionaryResources merged before cached stable append, raw stable rewrite, or packed promotion mutates its writer namespace", "TreeDB/internal/valuelog/reader.go; TreeDB/internal/valuelog/dict_codec_cache.go", "stable index vacuum fence + value-log identity pin; downstream pin-aware dictionary GC", "authoritative-transitive"},
	{ReachabilityTemplateGeneration, ResourceTemplate, "TreeDB/internal/templatedb/resource_capture.go; TreeDB/db/stable_leaf_rewrite.go; TreeDB/public.go; TreeDB/side_store_lookups.go", "stable templatedb index generation + exact pointer-backed value-log segment + salt-aware template ID", "template definition digest + full captured index frontier + exact value-log record end", "templatedb index namespace proof + installed value-log namespace", "CaptureTemplateResources merged before stable raw outer-leaf namespace mutation; ordinary/offline root activation remains quarantined for #3679", "TreeDB/internal/valuelog/template_lookup.go; TreeDB/internal/valuelog/template_cache.go", "stable index vacuum fence + value-log identity pin; retain forever until a pin-aware template GC exists", "authoritative-transitive"},
	{ReachabilityCollectionSystemRoot, ResourceIndex, "adjacent collection root publication issue #3679", "no independent external identity; named root lives inside pinned index.db", "catalog/root frontier remains adjacent to index-file durability", "none", "not registerable by #3677", "TreeDB/collections/api.go loadCollectionCatalog and descriptor validators", "collection clear/drop plus downstream #3681", "adjacent-root-publication"},
	{ReachabilityCollectionPrimaryRoot, ResourceIndex, "adjacent collection root publication issue #3679", "no independent external identity; named root lives inside pinned index.db", "primary-root page frontier remains adjacent to index-file durability", "none", "not registerable by #3677", "TreeDB/collections/api.go collectionRootNames and root validators", "collection clear/drop plus downstream #3681", "adjacent-root-publication"},
	{ReachabilityCollectionTemplateRoot, ResourceIndex, "adjacent collection root publication issue #3679", "no independent external identity; named root lives inside pinned index.db", "template-root frontier remains adjacent to index-file durability", "none", "not registerable by #3677", "TreeDB/collections/api.go catalog validation", "collection clear/drop plus downstream #3681", "adjacent-root-publication"},
	{ReachabilityCollectionIndexStateRoot, ResourceIndex, "adjacent collection root publication issue #3679", "no independent external identity; named root lives inside pinned index.db", "index-state root frontier remains adjacent to index-file durability", "none", "not registerable by #3677", "TreeDB/collections/api.go catalog validation", "collection clear/drop plus downstream #3681", "adjacent-root-publication"},
	{ReachabilityCollectionColumnRoot, ResourceIndex, "adjacent collection root publication issue #3679", "no independent external identity; named root lives inside pinned index.db", "column-manifest root frontier remains adjacent to index-file durability", "none", "not registerable by #3677", "TreeDB/collections/column_physical_scan.go; TreeDB/collections/api.go", "column lifecycle plus downstream #3681", "adjacent-root-publication"},
	{ReachabilityCollectionSecondaryRoot, ResourceIndex, "adjacent collection root publication issue #3679", "no independent external identity; named root lives inside pinned index.db", "secondary-root frontier remains adjacent to index-file durability", "none", "not registerable by #3677", "TreeDB/collections/api.go secondary index readers", "collection clear/drop plus downstream #3681", "adjacent-root-publication"},
	{ReachabilityCollectionVectorRoot, ResourceIndex, "adjacent collection root publication issue #3679", "no independent external identity; named root lives inside pinned index.db", "vector-root frontier remains adjacent to index-file durability", "none", "not registerable by #3677", "TreeDB/collections/vector_index_search.go", "vector maintenance plus downstream #3681", "adjacent-root-publication"},
	{ReachabilityCollectionTextDictionary, ResourceIndex, "adjacent collection root publication issue #3679", "no independent external identity; named root lives inside pinned index.db", "text-dictionary root frontier remains adjacent to index-file durability", "none", "not registerable by #3677", "TreeDB/collections/text_catalog.go; TreeDB/collections/text_search.go; TreeDB/collections/text_v2_search.go", "TreeDB/collections/text_maintenance.go; TreeDB/collections/text_v2_rewrite.go; downstream #3681", "adjacent-root-publication"},
	{ReachabilityCollectionTextPosting, ResourceIndex, "adjacent collection root publication issue #3679", "no independent external identity; named root lives inside pinned index.db", "text-posting root frontier remains adjacent to index-file durability", "none", "not registerable by #3677", "TreeDB/collections/text_search.go; TreeDB/collections/text_v2_search.go", "TreeDB/collections/text_maintenance.go; TreeDB/collections/text_v2_rewrite.go; downstream #3681", "adjacent-root-publication"},
	{ReachabilityCollectionTextPosition, ResourceIndex, "adjacent collection root publication issue #3679", "no independent external identity; named root lives inside pinned index.db", "text-position root frontier remains adjacent to index-file durability", "none", "not registerable by #3677", "TreeDB/collections/text_v2_search.go; TreeDB/collections/text_index.go", "TreeDB/collections/text_v2_rewrite.go; downstream #3681", "adjacent-root-publication"},
	{ReachabilityColumnManifest, ResourceColumnAsset, "TreeDB/collections/column_publish_write.go; TreeDB/collections/column_asset_manager.go", "pinned tcs1_part_image segment identity + namespace/generation/file ID", "ColumnAssetRef offset/length/checksum + manifest digest", "segment create/rotation token", "column publish plan builder", "TreeDB/collections/column_physical_scan.go; TreeDB/collections/column_physical_asset.go", "TreeDB/collections/column_asset_gc.go; TreeDB/collections/column_asset_rewrite.go", "authoritative"},
	{ReachabilityTypedColumnMultipart, ResourceTypedColumnAsset, "TreeDB/collections/column_asset_manager.go; TreeDB/collections/typed_column_publication.go", "pinned tcs1_typed_column_part identity", "asset offset/length/checksum", "segment create/rotation token", "typed-column child builder", "TreeDB/collections/column_physical_asset.go; TreeDB/internal/typedcolumn", "column asset GC/rewrite/reachability", "authoritative"},
	{ReachabilityTypedColumnValue, ResourceTypedColumnAsset, "TreeDB/collections/column_asset_manager.go", "pinned tcs1_int64_values/tcs1_aggregate_metadata identity", "asset offset/length/checksum", "segment create/rotation token", "typed-column child builder", "typed-column readers and physical checksum validator", "column asset GC/rewrite/reachability", "authoritative"},
	{ReachabilityTypedColumnCode, ResourceTypedColumnAsset, "TreeDB/collections/column_asset_manager.go", "pinned tcs1_dictionary_codes identity", "asset offset/length/checksum", "segment create/rotation token", "typed-column child builder", "typed-column readers and physical checksum validator", "column asset GC/rewrite/reachability", "authoritative"},
	{ReachabilityHNSWSearchPack, ResourceVectorGraphPack, "TreeDB/collections/column_hnsw_search_pack_writer.go; TreeDB/collections/column_asset_manager.go", "pinned tcs1_hnsw_search_pack identity", "pack checksum/base identity", "segment create/rotation token", "HNSW child builder", "TreeDB/collections/column_hnsw_search_pack_reader.go", "column asset GC/rewrite/reachability", "authoritative"},
	{ReachabilityVectorGraphPack, ResourceVectorGraphPack, "TreeDB/collections/column_vector_graph_typed_column.go; TreeDB/collections/column_vector_graph_manifest.go; TreeDB/collections/column_vector_index_state_adjacency.go", "transitive pinned graph part identities", "graph manifest and part checksums", "segment create/rotation tokens", "vector graph child builder", "TreeDB/collections/column_vector_index_state_*; graph asset readers", "column asset GC/rewrite/reachability", "authoritative-transitive"},
	{ReachabilityLegacyVectorSnapshot, ResourceLegacyVectorSnapshot, "TreeDB/collections/vector_index_persist.go; TreeDB/collections/vector_index_rebuild.go", "rebuildable epoch meta/nodes/edges/tombstones/docmap cache identity", "per-file SHA-256 and size + manifest digest", "epoch directory and manifest rename tokens", "not registerable as publication authority", "TreeDB/collections/vector_index_persist.go load validation with exact-search fallback", "TreeDB/collections/vector_index_persist.go serialized old-epoch cleanup", "rebuildable-non-authoritative"},
	{ReachabilityCommandWALActive, ResourceCommandWAL, "TreeDB/internal/commitlog/writer.go; TreeDB/internal/commitlog/journal_owner.go; TreeDB/db/command_wal_raw.go", "pinned active segment handle + segment generation", "required command-frame byte/LSN frontier", "active segment creation token", "command-WAL frame builder before append debt", "TreeDB/internal/commitlog/reader.go; TreeDB/db/wal_recovery.go", "TreeDB/db/command_wal_raw.go; TreeDB/db/command_wal_publish.go", "authoritative"},
	{ReachabilityCommandWALRotated, ResourceCommandWAL, "TreeDB/internal/commitlog/writer.go; TreeDB/internal/commitlog/journal_owner.go", "pinned rotated segment handle captured before lane replacement", "required command-frame byte/LSN frontier", "rotation segment creation token", "journal rotation result", "TreeDB/internal/commitlog/reader.go; TreeDB/db/wal_recovery.go", "TreeDB/db/command_wal_raw.go; TreeDB/db/command_wal_publish.go", "authoritative"},
	{ReachabilityCommandWALExternalRIDFence, ResourceCommandWALExternalRID, "TreeDB/internal/commitlog/command_frame_v2.go; TreeDB/db/command_wal_v2_recovery.go", "exact referenced pinned value-log segment identities", "sorted unique RID SHA-256 + count/min/max + exact segment frontiers", "inherited value-log namespace tokens", "V2 physical-frame builder before WAL append", "TreeDB/db/command_wal_v2_recovery.go classification and missing-RID fence", "command-WAL cleanup plus referenced value-log owners", "authoritative-transitive"},
	{ReachabilityQueryReadyBase, ResourceQueryReadyAsset, "TreeDB/collections/query_ready_build.go; TreeDB/collections/column_asset_manager.go", "pinned query_ready_base_v1 segment identity", "asset checksum + query-ready base identity", "column segment create/rotation token", "query-ready prepared build before manifest ref", "TreeDB/internal/typedcolumn/query_ready_base.go; TreeDB/collections/query_ready_generation_open.go", "column asset GC/rewrite/reachability/lifecycle", "rebuildable-non-authoritative"},
	{ReachabilityQueryReadyDelta, ResourceQueryReadyAsset, "TreeDB/collections/query_ready_build.go; TreeDB/collections/column_asset_manager.go", "pinned query_ready_delta_v1 segment identity", "asset checksum + query-ready delta identity", "column segment create/rotation token", "query-ready prepared build before manifest ref", "TreeDB/internal/typedcolumn/query_ready_delta.go; TreeDB/collections/query_ready_generation_open.go", "column asset GC/rewrite/reachability/lifecycle", "rebuildable-non-authoritative"},
	{ReachabilityQueryReadyConsolidatedBase, ResourceQueryReadyAsset, "TreeDB/collections/query_ready_build.go; TreeDB/collections/column_asset_manager.go", "pinned query_ready_consolidated_base_v1 segment identity", "asset checksum + consolidated base identity", "column segment create/rotation token", "query-ready consolidation before manifest ref", "TreeDB/internal/typedcolumn/query_ready_open.go; TreeDB/collections/query_ready_generation_open.go", "column asset GC/rewrite/reachability/lifecycle", "rebuildable-non-authoritative"},
	{ReachabilityLegacyActiveSlab, ResourceLegacyTreeDBField, "TreeDB/page/meta.go legacy decode only", "N/A: TreeDB no longer owns slab storage", "must remain zero/unreachable", "N/A", "explicit exclusion registrar", "TreeDB/page/meta.go compatibility validation", "N/A: HashDB owns its separate slab path", "explicit-legacy-exclusion"},
	{ReachabilityRaftSnapshot, ResourceSeparateDurability, "TreeDB/internal/raftcluster/snapshot_manifest.go; TreeDB/internal/raftfsm/raft_snapshot_side_stores.go", "snapshot-manifest-owned identities", "snapshot manifest checksums/frontiers", "snapshot protocol namespace operations", "separate raft snapshot builder", "TreeDB/internal/raftcluster snapshot recovery", "raft snapshot retention owner", "explicit-separate-domain"},
}

// StableResourcePolicy is the executable producer policy for one inventory
// field. Registerable is false when the field is owned by an adjacent
// publication milestone, another durability domain, or has been retired.
type StableResourcePolicy struct {
	Kind           ResourceKind
	Classification string
	Registerable   bool
	Producer       StableProducerDomain
	Stability      ResourceStability
}

func stableResourceStabilityForField(field ReachabilityField) (ResourceStability, bool) {
	switch field {
	case ReachabilityIndexFile,
		ReachabilityMetaPage,
		ReachabilityUserRoot,
		ReachabilitySystemRoot,
		ReachabilityFreelist,
		ReachabilityValueLogPointer,
		ReachabilityOuterLeafRawPointer,
		ReachabilityCollectionSystemRoot,
		ReachabilityCollectionPrimaryRoot,
		ReachabilityCollectionTemplateRoot,
		ReachabilityCollectionIndexStateRoot,
		ReachabilityCollectionColumnRoot,
		ReachabilityCollectionSecondaryRoot,
		ReachabilityCollectionVectorRoot,
		ReachabilityCollectionTextDictionary,
		ReachabilityCollectionTextPosting,
		ReachabilityCollectionTextPosition,
		ReachabilityColumnManifest,
		ReachabilityTypedColumnMultipart,
		ReachabilityTypedColumnValue,
		ReachabilityTypedColumnCode,
		ReachabilityHNSWSearchPack,
		ReachabilityVectorGraphPack,
		ReachabilityCommandWALActive,
		ReachabilityCommandWALRotated,
		ReachabilityCommandWALExternalRIDFence,
		ReachabilityQueryReadyBase,
		ReachabilityQueryReadyDelta,
		ReachabilityQueryReadyConsolidatedBase:
		return ResourceMutableAppend, true
	case ReachabilityOuterLeafPackedPointer,
		ReachabilityOuterLeafGeneration,
		ReachabilityDictionaryGeneration,
		ReachabilityTemplateGeneration,
		ReachabilityLegacyVectorSnapshot,
		ReachabilityLegacyActiveSlab,
		ReachabilityRaftSnapshot:
		return ResourceImmutable, true
	default:
		return 0, false
	}
}

func StableResourcePolicyFor(field ReachabilityField) (StableResourcePolicy, bool) {
	for _, requirement := range canonicalReachabilityRequirements {
		if requirement.Field != field {
			continue
		}
		registerable := registerableClassification(requirement.Classification)
		stability, ok := stableResourceStabilityForField(field)
		if !ok {
			return StableResourcePolicy{}, false
		}
		return StableResourcePolicy{
			Kind: requirement.Kind, Classification: requirement.Classification, Registerable: registerable,
			Producer: requirement.Producer, Stability: stability,
		}, true
	}
	return StableResourcePolicy{}, false
}

func registerableClassification(classification string) bool {
	switch classification {
	case "adjacent-root-publication", "adjacent-freelist-publication",
		"explicit-legacy-exclusion", "explicit-separate-domain",
		"rebuildable-non-authoritative":
		return false
	default:
		return true
	}
}

// NewStableProducerResourceTokenForDomain is the production registration
// boundary. It rejects a canonical field when the calling producer does not
// own that field, before validating the field's kind and classification.
func NewStableProducerResourceTokenForDomain(domain StableProducerDomain, spec StableResourceSpec, classification string) (*StableResourceToken, error) {
	policy, ok := StableResourcePolicyFor(spec.Reachability)
	if !ok {
		return nil, fmt.Errorf("%w: unknown reachability field %q", ErrUnresolvedResource, spec.Reachability)
	}
	if policy.Producer != domain {
		return nil, fmt.Errorf("%w: field %q belongs to producer %q, got %q", ErrResourceConflict, spec.Reachability, policy.Producer, domain)
	}
	return NewStableProducerResourceToken(spec, classification)
}

// NewStableProducerResourceToken is the only constructor production resource
// producers use. NewStableResourceToken remains the low-level primitive for
// platform adapters and contract tests.
func NewStableProducerResourceToken(spec StableResourceSpec, classification string) (*StableResourceToken, error) {
	if spec.StableIdentityOverride != (StableIdentity{}) {
		return nil, fmt.Errorf("%w: production resource identity must come from the exact open handle", ErrResourceConflict)
	}
	policy, ok := StableResourcePolicyFor(spec.Reachability)
	if !ok {
		return nil, fmt.Errorf("%w: unknown reachability field %q", ErrUnresolvedResource, spec.Reachability)
	}
	if !policy.Registerable {
		return nil, fmt.Errorf("%w: %s is %s", ErrResourceExcluded, spec.Reachability, policy.Classification)
	}
	if classification != policy.Classification {
		return nil, fmt.Errorf("%w: field %q requires classification %q, got %q", ErrResourceConflict, spec.Reachability, policy.Classification, classification)
	}
	if spec.Kind != policy.Kind {
		return nil, fmt.Errorf("%w: field %q requires kind %q, got %q", ErrResourceConflict, spec.Reachability, policy.Kind, spec.Kind)
	}
	return NewStableResourceToken(spec)
}

func StableResourceInventory() []StableResourceInventoryRow {
	return append([]StableResourceInventoryRow(nil), stableResourceInventory...)
}

func stableResourceInventoryRow(field ReachabilityField) (StableResourceInventoryRow, bool) {
	for _, row := range stableResourceInventory {
		if row.Field == field {
			return row, true
		}
	}
	return StableResourceInventoryRow{}, false
}

func RequiredReachabilityFields() []ReachabilityField {
	fields := make([]ReachabilityField, 0, len(canonicalReachabilityRequirements))
	for _, requirement := range canonicalReachabilityRequirements {
		if registerableClassification(requirement.Classification) {
			fields = append(fields, requirement.Field)
		}
	}
	return fields
}
