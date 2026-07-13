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
	{ReachabilityMetaPage, ResourceMeta, "authoritative", StableProducerDB},
	{ReachabilityUserRoot, ResourceIndex, "authoritative-root-backed", StableProducerDB},
	{ReachabilitySystemRoot, ResourceIndex, "authoritative-root-backed", StableProducerDB},
	{ReachabilityFreelist, ResourceIndex, "authoritative-root-backed", StableProducerDB},
	{ReachabilityValueLogPointer, ResourceValueLog, "authoritative", StableProducerValueLog},
	{ReachabilityOuterLeafRawPointer, ResourceOuterLeafLog, "authoritative", StableProducerOuterLeaf},
	{ReachabilityOuterLeafPackedPointer, ResourceOuterLeafPack, "authoritative", StableProducerOuterLeaf},
	{ReachabilityOuterLeafGeneration, ResourceOuterLeafManifest, "authoritative", StableProducerOuterLeaf},
	{ReachabilityDictionaryGeneration, ResourceDictionary, "authoritative-transitive", StableProducerDictionary},
	{ReachabilityTemplateGeneration, ResourceTemplate, "authoritative-transitive", StableProducerTemplate},
	{ReachabilityCollectionSystemRoot, ResourceCollectionRoot, "authoritative-root-backed", StableProducerCollection},
	{ReachabilityCollectionPrimaryRoot, ResourceCollectionRoot, "authoritative-root-backed", StableProducerCollection},
	{ReachabilityCollectionTemplateRoot, ResourceCollectionRoot, "authoritative-root-backed", StableProducerCollection},
	{ReachabilityCollectionIndexStateRoot, ResourceCollectionRoot, "authoritative-root-backed", StableProducerCollection},
	{ReachabilityCollectionColumnRoot, ResourceCollectionRoot, "authoritative-root-backed", StableProducerCollection},
	{ReachabilityCollectionSecondaryRoot, ResourceCollectionRoot, "authoritative-root-backed", StableProducerCollection},
	{ReachabilityCollectionVectorRoot, ResourceCollectionRoot, "authoritative-root-backed", StableProducerCollection},
	{ReachabilityCollectionTextDictionary, ResourceTextAsset, "authoritative-root-backed", StableProducerCollection},
	{ReachabilityCollectionTextPosting, ResourceTextAsset, "authoritative-root-backed", StableProducerCollection},
	{ReachabilityCollectionTextPosition, ResourceTextAsset, "authoritative-root-backed", StableProducerCollection},
	{ReachabilityColumnManifest, ResourceColumnAsset, "authoritative", StableProducerColumnAsset},
	{ReachabilityTypedColumnMultipart, ResourceTypedColumnAsset, "authoritative", StableProducerColumnAsset},
	{ReachabilityTypedColumnValue, ResourceTypedColumnAsset, "authoritative", StableProducerColumnAsset},
	{ReachabilityTypedColumnCode, ResourceTypedColumnAsset, "authoritative", StableProducerColumnAsset},
	{ReachabilityHNSWSearchPack, ResourceVectorGraphPack, "authoritative", StableProducerColumnAsset},
	{ReachabilityVectorGraphPack, ResourceVectorGraphPack, "authoritative-transitive", StableProducerColumnAsset},
	{ReachabilityLegacyVectorSnapshot, ResourceLegacyVectorSnapshot, "authoritative-legacy", StableProducerLegacyVector},
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
	{ReachabilityMetaPage, ResourceMeta, "TreeDB/pager; TreeDB/page/meta.go", "same pinned index identity as target meta page", "target meta page byte frontier", "none; covered by index namespace token", "root candidate builder", "TreeDB/db/root_snapshot.go; TreeDB/page/meta.go", "TreeDB/db/index_swap.go; downstream page owner #3681", "authoritative"},
	{ReachabilityUserRoot, ResourceIndex, "TreeDB/db/ordered_root_publish.go", "pinned index identity + root page generation", "user-root page frontier", "none", "ordered-root candidate builder", "TreeDB/db/root_snapshot.go", "downstream COW owner #3681", "authoritative-root-backed"},
	{ReachabilitySystemRoot, ResourceIndex, "TreeDB/db/system_root_publish.go; TreeDB/db/ordered_root_publish.go", "pinned index identity + root page generation", "system-root page frontier", "none", "system-root candidate builder", "TreeDB/db/root_snapshot.go; collection catalog validators", "downstream COW owner #3681", "authoritative-root-backed"},
	{ReachabilityFreelist, ResourceIndex, "TreeDB/pager freelist builder", "pinned index identity + freelist generation", "freelist page frontier", "none", "COW candidate extension", "TreeDB/pager freelist recovery", "downstream COW owner #3681", "authoritative-root-backed"},
	{ReachabilityValueLogPointer, ResourceValueLog, "TreeDB/internal/valuelog/writer.go; TreeDB/db/value_log_appender.go; TreeDB/caching/value_log_appender.go", "pinned segment handle + platform file ID + lane/file generation", "greatest referenced byte frontier + immutable segment header", "create and rotation namespace tokens", "vlog append/rotation result before lane replacement", "TreeDB/internal/valuelog/manager.go; TreeDB/internal/valuelog/reader.go; TreeDB/db/wal_recovery.go", "TreeDB/db/vlog_gc.go; TreeDB/db/vlog_rewrite.go; TreeDB/internal/valuelog/manager.go", "authoritative"},
	{ReachabilityOuterLeafRawPointer, ResourceOuterLeafLog, "TreeDB/db/leaf_page_log.go; TreeDB/db/leaf_page_log_lanes.go; TreeDB/caching/leaf_page_log_lanes.go", "pinned raw outer-leaf segment + generation", "greatest raw block byte frontier + header", "create/rotation namespace token", "outer-leaf append/rotation result before lane replacement", "TreeDB/db/leaf_page_read_cache.go; TreeDB/internal/outerleaf/block.go", "TreeDB/db/leaf_generation_gc.go; TreeDB/internal/valuelog/manager.go", "authoritative"},
	{ReachabilityOuterLeafPackedPointer, ResourceOuterLeafPack, "TreeDB/db/leaf_generation_pack.go; TreeDB/db/leaf_generation_pack_rewrite.go", "pinned packed segment + generation", "pack byte frontier + immutable header digest", "staging-to-generation rename namespace token", "pack promotion builder before manifest installation", "TreeDB/db/leaf_page_read_cache.go; TreeDB/internal/outerleaf/block.go", "TreeDB/db/leaf_generation_gc.go; pack rewrite cleanup", "authoritative"},
	{ReachabilityOuterLeafGeneration, ResourceOuterLeafManifest, "TreeDB/db/leaf_generation_manifest.go; TreeDB/db/leaf_generation_pack_rewrite.go", "pinned generation manifest + generation ID", "manifest content digest", "create/replace namespace token", "generation builder before system-root field", "TreeDB/db/leaf_generation_manifest.go reconciliation", "TreeDB/db/leaf_generation_gc.go", "authoritative"},
	{ReachabilityDictionaryGeneration, ResourceDictionary, "TreeDB/internal/dictdb/store.go; TreeDB/public.go; TreeDB/side_store_lookups.go", "transitive pinned dictdb index/vlog identities + dictionary ID", "dictionary content digest and child frontiers", "child DB creation/rotation tokens", "dictdb child builder before frame dictionary ID", "TreeDB/internal/valuelog/reader.go; TreeDB/internal/valuelog/dict_codec_cache.go", "dictdb root/COW/vlog maintenance; downstream #3681", "authoritative-transitive"},
	{ReachabilityTemplateGeneration, ResourceTemplate, "TreeDB/internal/templatedb/store.go; TreeDB/public.go; TreeDB/side_store_lookups.go", "transitive pinned templatedb index/vlog identities + template ID", "template/catalog digest and child frontiers", "child DB creation/rotation tokens", "templatedb child builder before frame template ID", "TreeDB/internal/valuelog/template_lookup.go; TreeDB/internal/valuelog/template_cache.go", "templatedb root/COW/vlog maintenance; downstream #3681", "authoritative-transitive"},
	{ReachabilityCollectionSystemRoot, ResourceCollectionRoot, "TreeDB/collections/api.go; TreeDB/db/ordered_root_publish.go", "pinned main index identity + system-root generation", "root descriptor page frontier/catalog digest", "none", "collection catalog builder before system-root delta", "TreeDB/collections/api.go loadCollectionCatalog and descriptor validators", "collection clear/drop plus downstream #3681", "authoritative-root-backed"},
	{ReachabilityCollectionPrimaryRoot, ResourceCollectionRoot, "TreeDB/collections/api.go", "pinned index identity + primary root generation", "primary-root page frontier", "none", "collection root policy builder", "TreeDB/collections/api.go collectionRootNames and root validators", "collection clear/drop plus downstream #3681", "authoritative-root-backed"},
	{ReachabilityCollectionTemplateRoot, ResourceCollectionRoot, "TreeDB/collections/api.go; TreeDB/collections/template_v1.go", "pinned index identity + template-root generation", "template-root page frontier/catalog digest", "none", "collection root policy builder", "TreeDB/collections/api.go catalog validation", "collection clear/drop plus downstream #3681", "authoritative-root-backed"},
	{ReachabilityCollectionIndexStateRoot, ResourceCollectionRoot, "TreeDB/collections/api.go", "pinned index identity + index-state generation", "index-state root page frontier", "none", "collection root policy builder", "TreeDB/collections/api.go catalog validation", "collection clear/drop plus downstream #3681", "authoritative-root-backed"},
	{ReachabilityCollectionColumnRoot, ResourceCollectionRoot, "TreeDB/collections/api.go; TreeDB/collections/typed_column_publication.go", "pinned index identity + column-manifest root generation", "column-manifest root frontier", "none", "column publish builder before catalog root ID", "TreeDB/collections/column_physical_scan.go; TreeDB/collections/api.go", "column lifecycle plus downstream #3681", "authoritative-root-backed"},
	{ReachabilityCollectionSecondaryRoot, ResourceCollectionRoot, "TreeDB/collections/api.go", "pinned index identity + secondary-root generation", "secondary-root page frontier", "none", "collection root policy builder", "TreeDB/collections/api.go secondary index readers", "collection clear/drop plus downstream #3681", "authoritative-root-backed"},
	{ReachabilityCollectionVectorRoot, ResourceCollectionRoot, "TreeDB/collections/api.go; TreeDB/collections/vector_index_persist.go", "pinned index identity + vector-root generation", "vector-root page frontier", "none", "vector child builder before vector root ID", "TreeDB/collections/vector_index_search.go", "vector maintenance plus downstream #3681", "authoritative-root-backed"},
	{ReachabilityCollectionTextDictionary, ResourceTextAsset, "TreeDB/collections/text_storage.go; TreeDB/collections/text_v2_storage.go; TreeDB/collections/text_v2_write_path.go", "pinned index identity + text dictionary/terms root generation", "text root page frontier", "none", "text child builder before catalog root IDs", "TreeDB/collections/text_catalog.go; TreeDB/collections/text_search.go; TreeDB/collections/text_v2_search.go", "TreeDB/collections/text_maintenance.go; TreeDB/collections/text_v2_rewrite.go; downstream #3681", "authoritative-root-backed"},
	{ReachabilityCollectionTextPosting, ResourceTextAsset, "TreeDB/collections/text_storage.go; TreeDB/collections/text_v2_storage.go; TreeDB/collections/text_v2_write_path.go", "pinned index identity + posting/norm root generation", "posting root page frontier", "none", "text child builder before catalog root IDs", "TreeDB/collections/text_search.go; TreeDB/collections/text_v2_search.go", "TreeDB/collections/text_maintenance.go; TreeDB/collections/text_v2_rewrite.go; downstream #3681", "authoritative-root-backed"},
	{ReachabilityCollectionTextPosition, ResourceTextAsset, "TreeDB/collections/text_v2_storage.go; TreeDB/collections/text_v2_write_path.go", "pinned index identity + position/generation root generation", "position root page frontier", "none", "text child builder before catalog root IDs", "TreeDB/collections/text_v2_search.go; TreeDB/collections/text_index.go", "TreeDB/collections/text_v2_rewrite.go; downstream #3681", "authoritative-root-backed"},
	{ReachabilityColumnManifest, ResourceColumnAsset, "TreeDB/collections/column_publish_write.go; TreeDB/collections/column_asset_manager.go", "pinned segment handle + namespace/generation/file ID", "ColumnAssetRef offset/length/checksum + manifest digest", "segment create/rotation token", "column publish plan builder", "TreeDB/collections/column_physical_scan.go; TreeDB/collections/column_physical_asset.go", "TreeDB/collections/column_asset_gc.go; TreeDB/collections/column_asset_rewrite.go", "authoritative"},
	{ReachabilityTypedColumnMultipart, ResourceTypedColumnAsset, "TreeDB/collections/column_asset_manager.go; TreeDB/collections/typed_column_publication.go", "pinned tcs1_part_image/tcs1_typed_column_part identity", "asset offset/length/checksum", "segment create/rotation token", "typed-column child builder", "TreeDB/collections/column_physical_asset.go; TreeDB/internal/typedcolumn", "column asset GC/rewrite/reachability", "authoritative"},
	{ReachabilityTypedColumnValue, ResourceTypedColumnAsset, "TreeDB/collections/column_asset_manager.go", "pinned tcs1_int64_values/tcs1_aggregate_metadata identity", "asset offset/length/checksum", "segment create/rotation token", "typed-column child builder", "typed-column readers and physical checksum validator", "column asset GC/rewrite/reachability", "authoritative"},
	{ReachabilityTypedColumnCode, ResourceTypedColumnAsset, "TreeDB/collections/column_asset_manager.go", "pinned tcs1_dictionary_codes identity", "asset offset/length/checksum", "segment create/rotation token", "typed-column child builder", "typed-column readers and physical checksum validator", "column asset GC/rewrite/reachability", "authoritative"},
	{ReachabilityHNSWSearchPack, ResourceVectorGraphPack, "TreeDB/collections/column_hnsw_search_pack_writer.go; TreeDB/collections/column_asset_manager.go", "pinned tcs1_hnsw_search_pack identity", "pack checksum/base identity", "segment create/rotation token", "HNSW child builder", "TreeDB/collections/column_hnsw_search_pack_reader.go", "column asset GC/rewrite/reachability", "authoritative"},
	{ReachabilityVectorGraphPack, ResourceVectorGraphPack, "TreeDB/collections/column_vector_graph_typed_column.go; TreeDB/collections/column_vector_graph_manifest.go; TreeDB/collections/column_vector_index_state_adjacency.go", "transitive pinned graph part identities", "graph manifest and part checksums", "segment create/rotation tokens", "vector graph child builder", "TreeDB/collections/column_vector_index_state_*; graph asset readers", "column asset GC/rewrite/reachability", "authoritative-transitive"},
	{ReachabilityLegacyVectorSnapshot, ResourceLegacyVectorSnapshot, "TreeDB/collections/vector_index_persist.go; TreeDB/collections/vector_index_rebuild.go", "pinned epoch meta/nodes/edges/tombstones/docmap identities", "per-file SHA-256 and size + manifest digest", "epoch directory and manifest rename tokens", "legacy vector snapshot builder", "TreeDB/collections/vector_index_persist.go load validation", "TreeDB/collections/vector_index_persist.go old-epoch cleanup", "authoritative-legacy"},
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
// field. Registerable is false only when another durability domain owns the
// field or TreeDB has explicitly retired it.
type StableResourcePolicy struct {
	Kind           ResourceKind
	Classification string
	Registerable   bool
	Producer       StableProducerDomain
}

func StableResourcePolicyFor(field ReachabilityField) (StableResourcePolicy, bool) {
	for _, requirement := range canonicalReachabilityRequirements {
		if requirement.Field != field {
			continue
		}
		registerable := requirement.Classification != "explicit-legacy-exclusion" &&
			requirement.Classification != "explicit-separate-domain"
		return StableResourcePolicy{
			Kind: requirement.Kind, Classification: requirement.Classification, Registerable: registerable,
			Producer: requirement.Producer,
		}, true
	}
	return StableResourcePolicy{}, false
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
	fields := make([]ReachabilityField, len(canonicalReachabilityRequirements))
	for i, requirement := range canonicalReachabilityRequirements {
		fields[i] = requirement.Field
	}
	return fields
}
