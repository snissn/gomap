package collections

// This file bridges the public local-M1 lifecycle API to the immutable VCP1
// checkpoint authority. The legacy mutable VPM/VPI/VPR files remain decodable
// only for corruption diagnostics; pre-alpha stores do not migrate or fall
// back to them.

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
)

func (s *VectorPartitionStoreV1) loadVectorPartitionLifecycleAuthorityV1(collection, index string) (vectorPartitionLifecycleCheckpointStoreStateV1, bool, error) {
	var zero vectorPartitionLifecycleCheckpointStoreStateV1
	dir, err := s.openDir()
	if err != nil {
		return zero, false, err
	}
	defer dir.Close()
	entries, err := readVectorPartitionDirEntriesBoundedV1(dir)
	if err != nil {
		return zero, false, err
	}
	identityPrefix := safeVPM(collection) + "-" + safeVPM(index)
	lifecyclePrefix := vectorPartitionLifecycleNamePrefixV1(collection, index)
	present := false
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), identityPrefix) {
			continue
		}
		if !strings.HasPrefix(entry.Name(), lifecyclePrefix) {
			return zero, false, fmt.Errorf("%w: legacy or unexpected vector partition authority %q", ErrVectorPartitionManifestInvalid, entry.Name())
		}
		present = true
	}
	if !present {
		return vectorPartitionLifecycleCheckpointStoreStateV1{
			state: vectorPartitionLifecycleStateV1{
				Collection:  collection,
				IndexName:   index,
				Generations: make(map[uint64]vectorPartitionLifecycleGenerationStateV1),
			},
		}, false, nil
	}
	loaded, err := s.loadVectorPartitionLifecycleCheckpointStateFromDirV1(dir, collection, index)
	if err != nil {
		return zero, false, err
	}
	if loaded.checkpoint.Epoch == 0 {
		return zero, false, fmt.Errorf("%w: lifecycle namespace without checkpoint", ErrVectorPartitionManifestInvalid)
	}
	return loaded, true, nil
}

func vectorPartitionLifecycleManifestV1(state vectorPartitionLifecycleStateV1, generation uint64, allowDeleting bool) (VectorPartitionManifestV1, error) {
	entry, present := state.Generations[generation]
	if !present || entry.Manifest == nil || (!allowDeleting && entry.Deleting) {
		return VectorPartitionManifestV1{}, os.ErrNotExist
	}
	raw, err := EncodeVectorPartitionManifestV1(*entry.Manifest)
	if err != nil {
		return VectorPartitionManifestV1{}, err
	}
	return DecodeVectorPartitionManifestV1(raw, DefaultVectorPartitionManifestLimits())
}

func (s *VectorPartitionStoreV1) vectorPartitionLifecycleGenerationCompleteV1(collection, index string, generation uint64) (bool, error) {
	if generation == 0 {
		return false, nil
	}
	loaded, present, err := s.loadVectorPartitionLifecycleAuthorityV1(collection, index)
	if err != nil {
		return false, err
	}
	if !present ||
		generation < loaded.state.GenerationFloor ||
		generation > loaded.state.GenerationHighWater {
		return false, nil
	}
	_, live := loaded.state.Generations[generation]
	return !live, nil
}

func vectorPartitionManifestCanonicalEqualV1(a, b VectorPartitionManifestV1) bool {
	aRaw, aErr := EncodeVectorPartitionManifestV1(a)
	bRaw, bErr := EncodeVectorPartitionManifestV1(b)
	return aErr == nil && bErr == nil && bytes.Equal(aRaw, bRaw)
}

func (s *VectorPartitionStoreV1) persistVectorPartitionManifestLifecycleV1(m VectorPartitionManifestV1) error {
	loaded, present, err := s.loadVectorPartitionLifecycleAuthorityV1(m.Collection, m.IndexName)
	if err != nil {
		return err
	}
	entry, generationPresent := loaded.state.Generations[m.Generation]
	switch m.State {
	case "building":
		if generationPresent && entry.Deleting {
			return fmt.Errorf("%w: generation %d is deleting", ErrVectorPartitionManifestInvalid, m.Generation)
		}
		if !generationPresent && present && m.Generation <= loaded.state.GenerationHighWater {
			return fmt.Errorf("%w: generation %d lifecycle is already complete", ErrVectorPartitionManifestInvalid, m.Generation)
		}
		raw, err := EncodeVectorPartitionManifestV1(m)
		if err != nil {
			return err
		}
		return s.persistVectorPartitionLifecycleOperationV1(
			m.Collection,
			m.IndexName,
			vectorPartitionLifecycleBuildV1,
			m.Generation,
			raw,
		)
	case "ready":
		if !generationPresent {
			building := cloneVectorPartitionManifestForCheckpointV1(m)
			building.State = "building"
			building.RouterGeneration = 0
			building.RouterAsset = VectorPartitionAssetV1{}
			building.ReadySetDigest = ""
			building.Canonicalize()
			raw, err := EncodeVectorPartitionManifestV1(building)
			if err != nil {
				return err
			}
			if err := s.persistVectorPartitionLifecycleOperationV1(
				m.Collection,
				m.IndexName,
				vectorPartitionLifecycleBuildV1,
				m.Generation,
				raw,
			); err != nil {
				return err
			}
			entry = vectorPartitionLifecycleGenerationStateV1{Manifest: &building}
			generationPresent = true
			present = true
		}
		if !present || entry.Manifest == nil || entry.Deleting {
			return fmt.Errorf("%w: ready publication requires a live generation", ErrVectorPartitionManifestInvalid)
		}
		if entry.Manifest.State == "building" {
			payload, err := makeVectorPartitionReadyPromotionPayloadV1(*entry.Manifest, m)
			if err != nil {
				return err
			}
			if err := s.persistVectorPartitionLifecycleOperationV1(
				m.Collection,
				m.IndexName,
				vectorPartitionLifecycleReadyV1,
				m.Generation,
				payload,
			); err != nil {
				return err
			}
		} else if entry.Manifest.State != "ready" || !vectorPartitionManifestCanonicalEqualV1(*entry.Manifest, m) {
			return fmt.Errorf("%w: generation %d already published with different bytes", ErrVectorPartitionManifestInvalid, m.Generation)
		}
		if loaded.state.ActiveGeneration == m.Generation {
			return nil
		}
		if m.Generation <= loaded.state.ActivationHighWater {
			return fmt.Errorf(
				"%w: generation %d cannot reactivate after generation %d reached lifecycle authority",
				ErrVectorPartitionManifestInvalid,
				m.Generation,
				loaded.state.ActivationHighWater,
			)
		}
		return s.persistVectorPartitionLifecycleOperationV1(
			m.Collection,
			m.IndexName,
			vectorPartitionLifecycleLocalActivateV1,
			m.Generation,
			nil,
		)
	default:
		return fmt.Errorf("%w: unsupported public lifecycle manifest state", ErrVectorPartitionManifestInvalid)
	}
}

func (s *VectorPartitionStoreV1) deactivateVectorPartitionLifecycleV1(collection, index string) error {
	loaded, present, err := s.loadVectorPartitionLifecycleAuthorityV1(collection, index)
	if err != nil {
		return err
	}
	if !present {
		return os.ErrNotExist
	}
	if loaded.state.ActiveGeneration == 0 {
		if loaded.state.RetiredGeneration != 0 {
			return nil
		}
		return os.ErrNotExist
	}
	return s.persistVectorPartitionLifecycleOperationV1(
		collection,
		index,
		vectorPartitionLifecycleDeactivateV1,
		loaded.state.ActiveGeneration,
		nil,
	)
}

func (s *VectorPartitionStoreV1) deleteVectorPartitionLifecycleV1(collection, index string, generation uint64, eligibility VectorPartitionCleanupEligibilityV1) error {
	if !eligibility.Deletable() {
		return fmt.Errorf("collections: vector partition generation %d is still reachable", generation)
	}
	if pins := vectorPartitionReaderPinCountV1(s.root, collection, index, generation); pins != 0 {
		return fmt.Errorf("collections: vector partition generation %d has %d reader pins", generation, pins)
	}
	loaded, present, err := s.loadVectorPartitionLifecycleAuthorityV1(collection, index)
	if err != nil {
		return err
	}
	if !present {
		return os.ErrNotExist
	}
	entry, generationPresent := loaded.state.Generations[generation]
	if !generationPresent || entry.Manifest == nil {
		return os.ErrNotExist
	}
	if loaded.state.ActiveGeneration == generation {
		return fmt.Errorf("collections: vector partition generation %d is active", generation)
	}
	if entry.Deleting {
		if entry.Reclaim == nil {
			return fmt.Errorf("%w: deleting generation without reclaim authority", ErrVectorPartitionManifestInvalid)
		}
		return nil
	}
	reclaim, err := newVectorPartitionReclaimStateV1(*entry.Manifest)
	if err != nil {
		return err
	}
	raw, err := encodeVectorPartitionReclaimRecordV1(reclaim)
	if err != nil {
		return err
	}
	if err := s.persistVectorPartitionLifecycleOperationV1(
		collection,
		index,
		vectorPartitionLifecycleDeletePrepareV1,
		generation,
		raw,
	); err != nil {
		return err
	}
	if hook := vectorPartitionDeleteAfterTombstoneHookV1(); hook != nil {
		hook()
	}
	return nil
}

func (s *VectorPartitionStoreV1) openVectorPartitionLifecyclePointerV1(collection, index string, active bool) (VectorPartitionManifestV1, error) {
	loaded, present, err := s.loadVectorPartitionLifecycleAuthorityV1(collection, index)
	if err != nil {
		return VectorPartitionManifestV1{}, err
	}
	if !present {
		return VectorPartitionManifestV1{}, os.ErrNotExist
	}
	generation := loaded.state.RetiredGeneration
	if active {
		generation = loaded.state.ActiveGeneration
	}
	if generation == 0 {
		return VectorPartitionManifestV1{}, os.ErrNotExist
	}
	manifest, err := vectorPartitionLifecycleManifestV1(loaded.state, generation, false)
	if err != nil {
		return VectorPartitionManifestV1{}, err
	}
	if manifest.State != "ready" {
		return VectorPartitionManifestV1{}, fmt.Errorf("%w: lifecycle pointer targets non-ready generation", ErrVectorPartitionManifestInvalid)
	}
	return manifest, nil
}

func vectorPartitionLifecycleReclaimIDV1(collection, index string, generation uint64) string {
	return collection + "\x00" + index + "\x00" + fmt.Sprintf("%d", generation)
}

func vectorPartitionLifecycleManifestRefsV1(manifest VectorPartitionManifestV1) []ColumnAssetRef {
	refs := make([]ColumnAssetRef, 0, len(manifest.Assets)+1)
	for _, asset := range manifest.Assets {
		if asset.Ref.Kind != "" {
			refs = append(refs, asset.Ref)
		}
	}
	if manifest.RouterAsset.Ref.Kind != "" {
		refs = append(refs, manifest.RouterAsset.Ref)
	}
	return refs
}

func (c *Collection) vectorPartitionLifecycleIndexesV1(store *VectorPartitionStoreV1) ([]string, error) {
	dir, err := store.openDir()
	if err != nil {
		return nil, err
	}
	entries, err := VectorPartitionSnapshotEntriesV1(dir)
	if err != nil {
		_ = dir.Close()
		return nil, err
	}
	present := make(map[string]struct{})
	for _, entry := range entries {
		if !strings.Contains(entry.Name, ".lifecycle.checkpoint.") {
			continue
		}
		raw, err := readVectorPartitionLifecycleSlotV1(dir, entry.Name, vectorPartitionLifecycleCheckpointMaxBytesV1)
		if err != nil {
			_ = dir.Close()
			return nil, err
		}
		collection, index, _, err := vectorPartitionCheckpointEnvelopeIdentityV1(raw)
		if err != nil {
			_ = dir.Close()
			return nil, err
		}
		if collection == c.name {
			present[index] = struct{}{}
		}
	}
	if err := dir.Close(); err != nil {
		return nil, err
	}
	indexes := make([]string, 0, len(present))
	for index := range present {
		indexes = append(indexes, index)
	}
	sort.Strings(indexes)
	return indexes, nil
}

func (c *Collection) vectorPartitionReachabilityRefsV1(releaseReclaimIDs map[string]struct{}) ([]ColumnAssetRef, []ColumnAssetRef, error) {
	if c == nil || c.db == nil {
		return nil, nil, nil
	}
	store, err := OpenExistingVectorPartitionStoreV1(c.db.Dir())
	if os.IsNotExist(err) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	indexes, err := c.vectorPartitionLifecycleIndexesV1(store)
	if err != nil {
		return nil, nil, err
	}
	var prepared, pinned []ColumnAssetRef
	for _, index := range indexes {
		loaded, present, err := store.loadVectorPartitionLifecycleAuthorityV1(c.name, index)
		if err != nil {
			return nil, nil, err
		}
		if !present {
			return nil, nil, fmt.Errorf("%w: discovered lifecycle authority disappeared", ErrVectorPartitionManifestInvalid)
		}
		if loaded.state.ActiveGeneration != 0 {
			active, ok := loaded.state.Generations[loaded.state.ActiveGeneration]
			if !ok || active.Manifest == nil || active.Deleting || active.Manifest.State != "ready" {
				return nil, nil, fmt.Errorf("%w: invalid active lifecycle generation", ErrVectorPartitionManifestInvalid)
			}
		}
		if loaded.state.RetiredGeneration != 0 {
			retired, ok := loaded.state.Generations[loaded.state.RetiredGeneration]
			if !ok || retired.Manifest == nil || retired.Deleting || retired.Manifest.State != "ready" {
				return nil, nil, fmt.Errorf("%w: invalid retired lifecycle generation", ErrVectorPartitionManifestInvalid)
			}
		}
		for generation, state := range loaded.state.Generations {
			if state.Manifest == nil {
				return nil, nil, fmt.Errorf("%w: live lifecycle generation without manifest", ErrVectorPartitionManifestInvalid)
			}
			if state.Deleting {
				if state.Reclaim == nil {
					return nil, nil, fmt.Errorf("%w: deleting lifecycle generation without reclaim state", ErrVectorPartitionManifestInvalid)
				}
				if _, releasing := releaseReclaimIDs[vectorPartitionLifecycleReclaimIDV1(c.name, index, generation)]; !releasing {
					prepared = append(prepared, state.Reclaim.debtRefs()...)
				}
				continue
			}
			refs := vectorPartitionLifecycleManifestRefsV1(*state.Manifest)
			prepared = append(prepared, refs...)
			if loaded.state.ActiveGeneration == generation {
				pinned = append(pinned, refs...)
			}
		}
	}
	return prepared, pinned, nil
}

func (c *Collection) vectorPartitionLifecycleReclaimRecordsV1(store *VectorPartitionStoreV1) ([]vectorPartitionReclaimRecordV1, error) {
	records := make([]vectorPartitionReclaimRecordV1, 0)
	indexes, err := c.vectorPartitionLifecycleIndexesV1(store)
	if err != nil {
		return nil, err
	}
	for _, index := range indexes {
		loaded, present, err := store.loadVectorPartitionLifecycleAuthorityV1(c.name, index)
		if err != nil {
			return nil, err
		}
		if !present {
			continue
		}
		for generation, entry := range loaded.state.Generations {
			if !entry.Deleting {
				continue
			}
			if entry.Manifest == nil || entry.Reclaim == nil {
				return nil, fmt.Errorf("%w: deleting lifecycle generation without reclaim authority", ErrVectorPartitionManifestInvalid)
			}
			records = append(records, vectorPartitionReclaimRecordV1{
				id:    vectorPartitionLifecycleReclaimIDV1(c.name, index, generation),
				state: entry.Reclaim.clone(),
			})
		}
	}
	if len(records) > vectorPartitionStoreMaxEntriesV1 {
		return nil, fmt.Errorf("%w: lifecycle reclaim record cap", ErrVectorPartitionManifestInvalid)
	}
	sort.Slice(records, func(i, j int) bool { return records[i].id < records[j].id })
	return records, nil
}

func (c *Collection) VectorPartitionStatusV1(index string, generation uint64) (VectorPartitionStatusV1, error) {
	if c == nil || c.db == nil {
		return VectorPartitionStatusV1{}, errors.New("collections: closed collection")
	}
	store, err := OpenExistingVectorPartitionStoreV1(c.db.Dir())
	if err != nil {
		return VectorPartitionStatusV1{}, err
	}
	loaded, present, err := store.loadVectorPartitionLifecycleAuthorityV1(c.name, index)
	if err != nil {
		return VectorPartitionStatusV1{}, err
	}
	if !present {
		return VectorPartitionStatusV1{}, os.ErrNotExist
	}
	manifest, err := vectorPartitionLifecycleManifestV1(loaded.state, generation, false)
	if err != nil {
		return VectorPartitionStatusV1{}, err
	}
	groups := make(map[string]struct{}, len(manifest.Placements))
	var assetBytes uint64
	for _, placement := range manifest.Placements {
		groups[placement.GroupID] = struct{}{}
	}
	for _, asset := range manifest.Assets {
		if ^uint64(0)-assetBytes < asset.Bytes {
			return VectorPartitionStatusV1{}, fmt.Errorf("%w: asset byte overflow", ErrVectorPartitionManifestInvalid)
		}
		assetBytes += asset.Bytes
	}
	if ^uint64(0)-assetBytes < manifest.RouterAsset.Bytes {
		return VectorPartitionStatusV1{}, fmt.Errorf("%w: asset byte overflow", ErrVectorPartitionManifestInvalid)
	}
	assetBytes += manifest.RouterAsset.Bytes

	active := loaded.state.ActiveGeneration == generation
	staleReason := "generation_building"
	if manifest.State == "ready" {
		switch {
		case active:
			staleReason = ""
		case loaded.state.RetiredGeneration == generation:
			staleReason = "retired"
		case loaded.state.ActiveGeneration != 0:
			staleReason = "replaced"
		default:
			staleReason = "inactive"
		}
		if c.validateVectorPartitionSourceIdentityV1(manifest) != nil {
			active = false
			staleReason = "source_stale"
		}
	}
	var capacity, overlapBudget, unspentOverlapBudget uint64
	if policy, ok := parseVectorPartitionOverlapPolicyV1(manifest.BalancePolicy); ok {
		capacity = policy.Capacity
		overlapBudget = policy.Budget
		unspentOverlapBudget = policy.Unspent
	}
	var missingAssets, corruptAssets, staleAssets uint64
	statusAssets := append([]VectorPartitionAssetV1(nil), manifest.Assets...)
	if manifest.State == "ready" {
		statusAssets = append(statusAssets, manifest.RouterAsset)
	}
	namespace := ""
	if len(statusAssets) > 0 {
		// Publication already bound every descriptor to the collection's asset
		// namespace. Reuse that immutable binding here and require every status
		// descriptor to agree with it.
		namespace = statusAssets[0].Ref.Namespace
	}
	for _, asset := range statusAssets {
		if asset.Ref.Generation != manifest.Generation {
			staleAssets++
			continue
		}
		if err := verifyVectorPartitionAssetsV1(c.db.ColumnAssetRootDir(), namespace, []VectorPartitionAssetV1{asset}); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				missingAssets++
			} else {
				corruptAssets++
			}
		}
	}
	return VectorPartitionStatusV1{
		Manifest:             manifest,
		Ready:                manifest.State == "ready",
		Active:               active,
		StaleReason:          staleReason,
		PartitionCount:       manifest.PartitionCount,
		GroupCount:           uint32(len(groups)),
		Memberships:          uint64(len(manifest.Memberships)),
		OverlapMemberships:   uint64(len(manifest.OverlapMemberships)),
		AssetBytes:           assetBytes,
		ReaderPins:           vectorPartitionReaderPinCountV1(c.db.Dir(), c.name, index, generation),
		Capacity:             capacity,
		OverlapBudget:        overlapBudget,
		UnspentOverlapBudget: unspentOverlapBudget,
		MissingAssets:        missingAssets,
		CorruptAssets:        corruptAssets,
		StaleAssets:          staleAssets,
	}, nil
}
