package collections

// ColumnGraphServingStats copies bounded metadata from an existing coordinator.
// Publication fields belong to one immutable state. Debt and owner gauges are
// sampled under their existing locks and are not a transactional read frontier.
// These are admission/residency bounds, not process heap or unique mapped bytes.
type ColumnGraphServingStats struct {
	Index                   string                          `json:"index"`
	PublicationPresent      bool                            `json:"publication_present"`
	PublicationUnchanged    bool                            `json:"publication_unchanged"`
	ServingReady            bool                            `json:"serving_ready"`
	Invalid                 bool                            `json:"invalid"`
	Reconciling             bool                            `json:"reconciling"`
	BasePresent             bool                            `json:"base_present"`
	BaseManifest            ColumnManifestIdentity          `json:"base_manifest"`
	CurrentManifest         ColumnManifestIdentity          `json:"current_manifest"`
	BaseCoverageLSN         uint64                          `json:"base_coverage_lsn"`
	CurrentCoverageLSN      uint64                          `json:"current_coverage_lsn"`
	BaseRows                int                             `json:"base_rows"`
	SuffixRows              int                             `json:"suffix_rows"`
	SuffixTombstones        int                             `json:"suffix_tombstones"`
	SuffixValueSlots        int                             `json:"suffix_value_slots"`
	SuffixPayloadBytes      int64                           `json:"suffix_payload_bytes"`
	InstalledAssetBytes     int64                           `json:"installed_asset_bytes"`
	ServingMetadataBytes    int64                           `json:"serving_metadata_bytes"`
	ServingAssetRefs        int                             `json:"serving_asset_refs"`
	Debt                    ColumnGraphPublicationDebtStats `json:"debt"`
	Pending                 ColumnGraphPublicationDebtStats `json:"pending"`
	EncodedBytesCharged     int64                           `json:"encoded_bytes_charged"`
	CandidateBytesCharged   int64                           `json:"candidate_bytes_charged"`
	AppenderAttemptsCharged int64                           `json:"appender_attempts_charged"`
	WorkEpoch               uint64                          `json:"work_epoch"`
	Owners                  int                             `json:"owners"`
	States                  int                             `json:"states"`
	StateBytes              int64                           `json:"state_bytes"`
	OwnerAssetBytes         int64                           `json:"owner_asset_bytes"`
	BaseOwners              int                             `json:"base_owners"`
	BaseAssetBytes          int64                           `json:"base_asset_bytes"`
	BaseDescriptorBytes     int64                           `json:"base_descriptor_bytes"`
	BaseBackingBytes        int64                           `json:"base_backing_bytes"`
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
	defer coord.typedPublicationDebtMu.Unlock()
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
		}
	}
	debt := func(d typedGraphPublicationCost) ColumnGraphPublicationDebtStats {
		return ColumnGraphPublicationDebtStats{d.rows, d.tombstones, d.slots, d.bytes}
	}
	out.Debt, out.Pending = debt(coord.typedPublicationDebt), debt(coord.typedPublicationPending)
	out.EncodedBytesCharged, out.CandidateBytesCharged, out.AppenderAttemptsCharged = coord.typedPublicationEncodedBytes, coord.typedGraphCandidateBytes, coord.typedGraphCandidateAttempts
	out.WorkEpoch = coord.typedGraphWorkEpoch
	a := &coord.typedGraphOwners
	a.Lock()
	out.Owners, out.States, out.StateBytes, out.OwnerAssetBytes = a.owners, len(a.states), a.stateBytes, a.assetBytes
	out.BaseOwners, out.BaseAssetBytes, out.BaseDescriptorBytes, out.BaseBackingBytes = a.baseOwners, a.baseAssetBytes, a.baseDescriptorBytes, a.baseBackingBytes
	a.Unlock()
	out.PublicationUnchanged = state == coord.typedPublication.Load()
	return out, true
}
