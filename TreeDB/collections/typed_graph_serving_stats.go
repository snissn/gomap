package collections

// ColumnGraphServingStats copies bounded metadata from an existing coordinator.
// Publication fields belong to one immutable state. Debt and owner gauges are
// sampled under their existing locks and are not a transactional read frontier.
// These are admission/residency bounds, not process heap or unique mapped bytes.
type ColumnGraphServingStats struct {
	Index                   string                           `json:"index"`
	PublicationPresent      bool                             `json:"publication_present"`
	PublicationUnchanged    bool                             `json:"publication_unchanged"`
	ServingReady            bool                             `json:"serving_ready"`
	Invalid                 bool                             `json:"invalid"`
	Reconciling             bool                             `json:"reconciling"`
	BasePresent             bool                             `json:"base_present"`
	BaseManifest            ColumnManifestIdentity           `json:"base_manifest"`
	CurrentManifest         ColumnManifestIdentity           `json:"current_manifest"`
	BaseCoverageLSN         uint64                           `json:"base_coverage_lsn"`
	CurrentCoverageLSN      uint64                           `json:"current_coverage_lsn"`
	BaseRows                int                              `json:"base_rows"`
	SuffixRows              int                              `json:"suffix_rows"`
	SuffixTombstones        int                              `json:"suffix_tombstones"`
	SuffixValueSlots        int                              `json:"suffix_value_slots"`
	SuffixPayloadBytes      int64                            `json:"suffix_payload_bytes"`
	InstalledAssetBytes     int64                            `json:"installed_asset_bytes"`
	ServingMetadataBytes    int64                            `json:"serving_metadata_bytes"`
	ServingAssetRefs        int                              `json:"serving_asset_refs"`
	Debt                    ColumnGraphPublicationDebtStats  `json:"debt"`
	Pending                 ColumnGraphPublicationDebtStats  `json:"pending"`
	EncodedBytesCharged     int64                            `json:"encoded_bytes_charged"`
	CandidateBytesCharged   int64                            `json:"candidate_bytes_charged"`
	AppenderAttemptsCharged int64                            `json:"appender_attempts_charged"`
	WorkEpoch               uint64                           `json:"work_epoch"`
	Owners                  int                              `json:"owners"`
	States                  int                              `json:"states"`
	StateBytes              int64                            `json:"state_bytes"`
	OwnerAssetBytes         int64                            `json:"owner_asset_bytes"`
	BaseOwners              int                              `json:"base_owners"`
	BaseAssetBytes          int64                            `json:"base_asset_bytes"`
	BaseDescriptorBytes     int64                            `json:"base_descriptor_bytes"`
	BaseBackingBytes        int64                            `json:"base_backing_bytes"`
	Physical                ColumnGraphPhysicalResourceStats `json:"physical"`
	LogicalResources        ColumnGraphLogicalResourceStats  `json:"logical_resources"`
	VectorAssets            []ColumnGraphServingAssetStats   `json:"vector_assets,omitempty"`
	TypedColumnParts        []ColumnGraphServingPartStats    `json:"typed_column_parts,omitempty"`
}

// ColumnGraphServingAssetStats is the immutable persisted vector-state
// inventory. It carries identities and sizes only; diagnostics never read or
// expose vector payloads.
type ColumnGraphServingAssetStats struct {
	Role                string         `json:"role"`
	AssetID             string         `json:"asset_id"`
	LogicalType         string         `json:"logical_type"`
	PhysicalEncoding    string         `json:"physical_encoding"`
	Rows                int            `json:"rows"`
	Bytes               int64          `json:"bytes"`
	LogicalPayloadBytes int64          `json:"logical_payload_bytes,omitempty"`
	Ref                 ColumnAssetRef `json:"ref"`
}

// ColumnGraphServingPartStats identifies persisted typed-column parts so the
// canonical vector authority can be cross-bound to the vector-state asset.
type ColumnGraphServingPartStats struct {
	Rows int                    `json:"rows"`
	Role ColumnManifestPartRole `json:"role"`
	Ref  ColumnAssetRef         `json:"ref"`
}

// ColumnGraphPhysicalResourceStats is coordinator-wide across every Collection
// handle and overlapping holder generation for this DB/collection. Potential
// fields are admission reservations; live/in-flight fields describe current OS
// ownership. InventoryChargeBytes is a deterministic modeled metadata charge,
// not measured heap/RSS. HolderFallbackPotentialBytes is the sum of exact base
// ref lengths: a complete ceiling for pool-owned holder-resource fallback
// buffers and deliberately conservative for materializer-only refs. Request-
// local materializer copies are never included in physical fallback live bytes.
type ColumnGraphPhysicalResourceStats struct {
	Limits                       ColumnGraphPhysicalResourceLimits `json:"limits"`
	ClosedDB                     bool                              `json:"closed_db"`
	InventoryHolders             int                               `json:"inventory_holders"`
	InventoryRefs                int                               `json:"inventory_refs"`
	InventorySegments            int                               `json:"inventory_segments"`
	InventoryChargeBytes         int64                             `json:"inventory_charge_bytes"`
	PotentialSegments            int                               `json:"potential_segments"`
	PotentialDescriptors         int                               `json:"potential_descriptors"`
	PotentialMappedBytes         int64                             `json:"potential_mapped_bytes"`
	HolderFallbackPotentialBytes int64                             `json:"holder_fallback_potential_bytes"`
	DescriptorsInFlight          int                               `json:"descriptors_in_flight"`
	DescriptorsLive              int                               `json:"descriptors_live"`
	UnconfirmedDescriptors       int                               `json:"unconfirmed_descriptors"`
	MappedBackings               int                               `json:"mapped_backings"`
	MappedBytesInFlight          int64                             `json:"mapped_bytes_in_flight"`
	MappedBytes                  int64                             `json:"mapped_bytes"`
	FallbackSegments             int                               `json:"fallback_segments"`
	FallbackBackings             int                               `json:"fallback_backings"`
	FallbackBytesInFlight        int64                             `json:"fallback_bytes_in_flight"`
	FallbackBytes                int64                             `json:"fallback_bytes"`
	CleanupBackings              int                               `json:"cleanup_backings"`
	QuarantinedHolders           int                               `json:"quarantined_holders"`
	TotalBuilds                  uint64                            `json:"total_builds"`
	TotalBuildFailures           uint64                            `json:"total_build_failures"`
	TotalOpens                   uint64                            `json:"total_opens"`
	TotalCloseAttempts           uint64                            `json:"total_close_attempts"`
	TotalConfirmedCloses         uint64                            `json:"total_confirmed_closes"`
	TotalMaps                    uint64                            `json:"total_maps"`
	TotalConfirmedUnmaps         uint64                            `json:"total_confirmed_unmaps"`
	TotalFallbacks               uint64                            `json:"total_fallbacks"`
	TotalCleanupFailures         uint64                            `json:"total_cleanup_failures"`
	TotalQuarantines             uint64                            `json:"total_quarantines"`
	TotalCleanupRetries          uint64                            `json:"total_cleanup_retries"`
	LastBuildError               string                            `json:"last_build_error,omitempty"`
	LastPrimaryError             string                            `json:"last_primary_error,omitempty"`
	LastCleanupError             string                            `json:"last_cleanup_error,omitempty"`
}

// ColumnGraphLogicalResourceStats remains distinctly labelled: these are
// mappedresource handles/bytes in this Collection handle's prepared cache, not
// coordinator-wide physical descriptors, VMAs, or segment backings.
type ColumnGraphLogicalResourceStats struct {
	Entries                    int    `json:"entries"`
	Refs                       int    `json:"refs"`
	BuildingEntries            int    `json:"building_entries"`
	ActiveHandles              int64  `json:"active_handles"`
	ActiveMappedBytes          int64  `json:"active_mapped_bytes"`
	ActiveHeapCopyBytes        int64  `json:"active_heap_copy_bytes"`
	ActiveDerivedMetadataBytes int64  `json:"active_derived_metadata_bytes"`
	TotalAcquires              uint64 `json:"total_acquires"`
	TotalReleases              uint64 `json:"total_releases"`
	FallbackReads              uint64 `json:"fallback_reads"`
}

func columnGraphPhysicalResourceStats(limits typedGraphPhysicalResourceLimits, p typedGraphPhysicalResourceAccounting, closedDB bool) ColumnGraphPhysicalResourceStats {
	return ColumnGraphPhysicalResourceStats{
		Limits: limits, ClosedDB: closedDB,
		InventoryHolders: p.inventoryHolders, InventoryRefs: p.inventoryRefs, InventorySegments: p.inventorySegments, InventoryChargeBytes: p.inventoryBytes,
		PotentialSegments: p.potentialSegments, PotentialDescriptors: p.potentialDescriptors, PotentialMappedBytes: p.potentialMappedBytes,
		HolderFallbackPotentialBytes: p.potentialFallbackBytes,
		DescriptorsInFlight:          p.descriptorsInFlight, DescriptorsLive: p.descriptorsLive, UnconfirmedDescriptors: p.unconfirmedDescriptors,
		MappedBackings: p.mappedBackings, MappedBytesInFlight: p.mappedBytesInFlight, MappedBytes: p.mappedBytes,
		FallbackSegments: p.fallbackSegments, FallbackBackings: p.fallbackBackings,
		FallbackBytesInFlight: p.fallbackBytesInFlight, FallbackBytes: p.fallbackBytes,
		CleanupBackings: p.cleanupBackings, QuarantinedHolders: p.cleanupQuarantines,
		TotalBuilds: p.totalBuilds, TotalBuildFailures: p.totalBuildFailures, TotalOpens: p.totalOpens,
		TotalCloseAttempts: p.totalCloseAttempts, TotalConfirmedCloses: p.totalCloses,
		TotalMaps: p.totalMaps, TotalConfirmedUnmaps: p.totalUnmaps, TotalFallbacks: p.totalFallbacks,
		TotalCleanupFailures: p.totalCleanupFailures, TotalQuarantines: p.totalQuarantines, TotalCleanupRetries: p.totalCleanupRetries,
		LastBuildError: p.lastBuildError, LastPrimaryError: p.lastPrimaryError, LastCleanupError: p.lastCleanupError,
	}
}

type ColumnGraphPublicationDebtStats struct {
	Rows       int   `json:"rows"`
	Tombstones int   `json:"tombstones"`
	ValueSlots int   `json:"value_slots"`
	Bytes      int64 `json:"bytes"`
}

// ColumnGraphServingSnapshot never creates a coordinator, opens a snapshot or
// read owner, loads assets, warms a cache, or reconciles publication state.
// False means this handle has no existing selected serving policy to observe.
func (c *Collection) ColumnGraphServingSnapshot() (out ColumnGraphServingStats, available bool) {
	if c == nil || c.db == nil || c.db.IsClosing() {
		return out, false
	}
	var coord *collectionSchemaCoordinator
	if c.writeDomain != nil {
		coord = c.writeDomain.schemaCoordinator
	}
	if coord == nil {
		registered, ok := collectionSchemaCoordinators.Load(c.db)
		if !ok {
			return out, false
		}
		dbCoord := registered.(*collectionDBSchemaCoordinators)
		dbCoord.mu.Lock()
		coord = dbCoord.collections[c.collectionName()]
		dbCoord.mu.Unlock()
	}
	if coord == nil {
		return out, false
	}
	policy := coord.typedGraphServing.Load()
	if policy == nil {
		return out, false
	}
	out.Index = policy.index
	coord.typedPublicationDebtMu.Lock()
	state := coord.typedPublication.Load()
	out.PublicationPresent = state != nil
	if state != nil {
		out.ServingReady, out.Invalid, out.Reconciling = state.servingAdmitted && !state.invalid && state.servingBase != nil, state.invalid, state.reconciling != nil
		out.SuffixRows, out.SuffixTombstones, out.SuffixValueSlots = state.physicalRows, state.tombstones, state.valueSlots
		out.SuffixPayloadBytes, out.InstalledAssetBytes, out.ServingMetadataBytes = state.admittedPayloadBytes, state.installedAssetBytes, state.servingMetadataBytes
		out.ServingAssetRefs = len(state.servingRefs)
		if state.catalog != nil {
			if cfg := state.catalog.meta.Options.ColumnStore; cfg != nil {
				if cfg.ActiveManifest != nil {
					out.CurrentManifest = *cfg.ActiveManifest
				}
				out.CurrentCoverageLSN = cfg.RecoveryAuthoritativeAppliedCommandLSN
			}
			if base := state.catalog.typedGraphBase; base != nil && base.meta.Options.ColumnStore != nil {
				cfg := base.meta.Options.ColumnStore
				out.BasePresent = true
				if cfg.ActiveManifest != nil {
					out.BaseManifest = *cfg.ActiveManifest
				}
				out.BaseCoverageLSN = cfg.RecoveryAuthoritativeAppliedCommandLSN
			}
		}
		if state.servingBase != nil {
			out.BaseRows = state.servingBase.graph.RowCount
			view := state.servingBase.view
			if view.VectorIndexStateFound {
				out.VectorAssets = make([]ColumnGraphServingAssetStats, len(view.VectorIndexState.Assets))
				for i, asset := range view.VectorIndexState.Assets {
					logicalPayloadBytes := int64(0)
					switch asset.Role {
					case columnVectorIndexStateAssetRoleNormalizedVectors:
						logicalPayloadBytes = int64(asset.RowCount) * int64(view.VectorIndexState.Dimensions) * 4
					case columnVectorIndexStateAssetRoleQuantizedCodes:
						logicalPayloadBytes = int64(asset.RowCount) * int64(view.VectorIndexState.Dimensions)
					}
					out.VectorAssets[i] = ColumnGraphServingAssetStats{
						Role: asset.Role, AssetID: asset.AssetID, LogicalType: asset.LogicalType,
						PhysicalEncoding: asset.PhysicalEncoding, Rows: asset.RowCount,
						Bytes: asset.AssetBytes, LogicalPayloadBytes: logicalPayloadBytes, Ref: asset.Ref,
					}
				}
			}
			materializerView := state.servingBase.materializerView
			out.TypedColumnParts = make([]ColumnGraphServingPartStats, len(materializerView.TypedColumnPartRefs))
			for i, part := range materializerView.TypedColumnPartRefs {
				out.TypedColumnParts[i] = ColumnGraphServingPartStats{Rows: part.Rows, Role: part.Role, Ref: part.Ref}
			}
		}
	}
	debt := func(d typedGraphPublicationCost) ColumnGraphPublicationDebtStats {
		return ColumnGraphPublicationDebtStats{d.rows, d.tombstones, d.slots, d.bytes}
	}
	out.Debt, out.Pending = debt(coord.typedPublicationDebt), debt(coord.typedPublicationPending)
	out.EncodedBytesCharged, out.CandidateBytesCharged, out.AppenderAttemptsCharged = coord.typedPublicationEncodedBytes, coord.typedGraphCandidateBytes, coord.typedGraphCandidateAttempts
	out.WorkEpoch = coord.typedGraphWorkEpoch
	coord.typedPublicationDebtMu.Unlock()
	a := &coord.typedGraphOwners
	a.Lock()
	out.Owners, out.States, out.StateBytes, out.OwnerAssetBytes = a.owners, len(a.states), a.stateBytes, a.assetBytes
	out.BaseOwners, out.BaseAssetBytes, out.BaseDescriptorBytes, out.BaseBackingBytes = a.baseOwners, a.baseAssetBytes, a.baseDescriptorBytes, a.baseBackingBytes
	a.Unlock()
	physical, physicalLimits := coord.typedGraphPhysical.snapshotWithLimits()
	if physicalLimits == (typedGraphPhysicalResourceLimits{}) {
		// Configured limits remain observable before first holder admission and
		// after the final clean close; they are not inferred from logical limits.
		physicalLimits = policy.options.Owners.Physical
	}
	out.Physical = columnGraphPhysicalResourceStats(physicalLimits, physical, false)
	logical := c.columnVectorGraphSharedPreparedSearchCacheSnapshot()
	out.LogicalResources = ColumnGraphLogicalResourceStats{
		Entries: logical.Entries, Refs: logical.Refs, BuildingEntries: logical.BuildingEntries,
		ActiveHandles: logical.ActiveHandles, ActiveMappedBytes: logical.ActiveMappedBytes,
		ActiveHeapCopyBytes: logical.ActiveHeapCopyBytes, ActiveDerivedMetadataBytes: logical.ActiveDerivedMetadataBytes,
		TotalAcquires: logical.TotalAcquires, TotalReleases: logical.TotalReleases, FallbackReads: logical.FallbackReads,
	}
	out.PublicationUnchanged = state == coord.typedPublication.Load()
	return out, true
}
