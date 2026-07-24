package collections

// This file bridges the public local-M1 lifecycle API to the immutable VCP1
// checkpoint authority. The legacy mutable VPM/VPI/VPR files remain decodable
// only for corruption diagnostics; pre-alpha stores do not migrate or fall
// back to them.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// VectorPartitionActiveAuthorityTokenV1 is an opaque, comparable snapshot of
// one active lifecycle namespace and its live vector-index source identity.
// Serving caches retain the token returned with the manifest and use it to
// reject lifecycle or source changes without rereading the large checkpoint.
type VectorPartitionActiveAuthorityTokenV1 struct {
	lifecycleDigest [sha256.Size]byte
	source          VectorPartitionSourceIdentityV1
	generation      uint64
}

// ValidateActiveVectorPartitionGenerationWithContextV1 is the compatibility
// entrypoint for callers that do not retain an authority token. Serving caches
// should use ValidateActiveVectorPartitionAuthorityTokenWithContextV1.
func (c *Collection) ValidateActiveVectorPartitionGenerationWithContextV1(ctx context.Context, index string, generation uint64) error {
	_, err := c.ActiveVectorPartitionManifestWithContextV1(ctx, index, generation)
	return err
}

func (c *Collection) ActiveVectorPartitionManifestWithContextV1(ctx context.Context, index string, generation uint64) (VectorPartitionManifestV1, error) {
	manifest, _, err := c.ActiveVectorPartitionManifestAndAuthorityTokenWithContextV1(ctx, index, generation)
	return manifest, err
}

// ActiveVectorPartitionManifestAndAuthorityTokenWithContextV1 opens the full
// lifecycle authority once and returns the token used for bounded warm checks.
func (c *Collection) ActiveVectorPartitionManifestAndAuthorityTokenWithContextV1(ctx context.Context, index string, generation uint64) (VectorPartitionManifestV1, VectorPartitionActiveAuthorityTokenV1, error) {
	if c == nil || c.db == nil {
		return VectorPartitionManifestV1{}, VectorPartitionActiveAuthorityTokenV1{}, errors.New("collections: closed collection")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return VectorPartitionManifestV1{}, VectorPartitionActiveAuthorityTokenV1{}, err
	}
	store, err := OpenExistingVectorPartitionStoreV1(c.db.Dir())
	if err != nil {
		return VectorPartitionManifestV1{}, VectorPartitionActiveAuthorityTokenV1{}, err
	}
	loaded, present, err := store.loadVectorPartitionLifecycleAuthorityWithContextV1(ctx, c.name, index)
	if err != nil {
		return VectorPartitionManifestV1{}, VectorPartitionActiveAuthorityTokenV1{}, err
	}
	if err := ctx.Err(); err != nil {
		return VectorPartitionManifestV1{}, VectorPartitionActiveAuthorityTokenV1{}, err
	}
	if !present || loaded.state.ActiveGeneration != generation {
		return VectorPartitionManifestV1{}, VectorPartitionActiveAuthorityTokenV1{}, fmt.Errorf("%w: generation %d is not active", ErrVectorPartitionManifestInvalid, generation)
	}
	entry, ok := loaded.state.Generations[generation]
	if !ok || entry.Manifest == nil || entry.Deleting || entry.Manifest.State != "ready" {
		return VectorPartitionManifestV1{}, VectorPartitionActiveAuthorityTokenV1{}, fmt.Errorf("%w: generation %d is not complete and ready", ErrVectorPartitionManifestInvalid, generation)
	}
	manifest, err := vectorPartitionLifecycleManifestWithContextV1(ctx, loaded.state, generation, false)
	if err != nil {
		return VectorPartitionManifestV1{}, VectorPartitionActiveAuthorityTokenV1{}, err
	}
	source, err := c.VectorPartitionSourceIdentityV1(index)
	if err != nil {
		return VectorPartitionManifestV1{}, VectorPartitionActiveAuthorityTokenV1{}, err
	}
	if manifest.SourceGeneration != source.Generation || manifest.SourceChecksum != source.Checksum || manifest.SourceSchemaHash != source.SchemaHash || manifest.SourceRowCount != source.RowCount {
		return VectorPartitionManifestV1{}, VectorPartitionActiveAuthorityTokenV1{}, errors.New("collections: vector partition source identity mismatch")
	}
	if err := ctx.Err(); err != nil {
		return VectorPartitionManifestV1{}, VectorPartitionActiveAuthorityTokenV1{}, err
	}
	token, err := vectorPartitionActiveAuthorityTokenV1(loaded.entries, source, generation)
	if err != nil {
		return VectorPartitionManifestV1{}, VectorPartitionActiveAuthorityTokenV1{}, err
	}
	return manifest, token, nil
}

// ValidateActiveVectorPartitionAuthorityTokenWithContextV1 validates a cached
// generation using bounded directory metadata and the live source identity.
// Lifecycle files are immutable, so an unchanged set of exact physical
// identities is proof that the previously decoded authority is unchanged.
func (c *Collection) ValidateActiveVectorPartitionAuthorityTokenWithContextV1(ctx context.Context, index string, generation uint64, expected VectorPartitionActiveAuthorityTokenV1) error {
	if c == nil || c.db == nil {
		return errors.New("collections: closed collection")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if expected.generation != generation || generation == 0 {
		return fmt.Errorf("%w: active authority token generation", ErrVectorPartitionManifestInvalid)
	}
	store, err := OpenExistingVectorPartitionStoreV1(c.db.Dir())
	if err != nil {
		return err
	}
	entries, err := store.vectorPartitionLifecycleAuthorityEntriesWithContextV1(ctx, c.name, index)
	if err != nil {
		return err
	}
	source, err := c.VectorPartitionSourceIdentityV1(index)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	got, err := vectorPartitionActiveAuthorityTokenV1(entries, source, generation)
	if err != nil {
		return err
	}
	if got != expected {
		return fmt.Errorf("%w: active lifecycle or source authority changed", ErrVectorPartitionManifestInvalid)
	}
	return nil
}

func vectorPartitionActiveAuthorityTokenV1(entries []vectorPartitionLifecycleCheckpointEntryV1, source VectorPartitionSourceIdentityV1, generation uint64) (VectorPartitionActiveAuthorityTokenV1, error) {
	if len(entries) == 0 || generation == 0 {
		return VectorPartitionActiveAuthorityTokenV1{}, fmt.Errorf("%w: empty active authority token", ErrVectorPartitionManifestInvalid)
	}
	canonical := append([]vectorPartitionLifecycleCheckpointEntryV1(nil), entries...)
	sort.Slice(canonical, func(i, j int) bool { return canonical[i].name < canonical[j].name })
	h := sha256.New()
	h.Write([]byte("treedb/vector-partition-active-authority/v1"))
	var encoded [8]byte
	for _, entry := range canonical {
		if entry.name == "" || entry.bytes == 0 {
			return VectorPartitionActiveAuthorityTokenV1{}, fmt.Errorf("%w: active authority token entry", ErrVectorPartitionManifestInvalid)
		}
		binary.BigEndian.PutUint64(encoded[:], uint64(len(entry.name)))
		h.Write(encoded[:])
		h.Write([]byte(entry.name))
		h.Write([]byte{byte(entry.kind)})
		for _, value := range []uint64{entry.epoch, entry.sequence, entry.bytes, entry.identity.VolumeID, entry.identity.Generation} {
			binary.BigEndian.PutUint64(encoded[:], value)
			h.Write(encoded[:])
		}
		binary.BigEndian.PutUint64(encoded[:], uint64(len(entry.identity.Platform)))
		h.Write(encoded[:])
		h.Write([]byte(entry.identity.Platform))
		h.Write(entry.identity.ObjectID[:])
	}
	var digest [sha256.Size]byte
	copy(digest[:], h.Sum(nil))
	return VectorPartitionActiveAuthorityTokenV1{lifecycleDigest: digest, source: source, generation: generation}, nil
}

func (s *VectorPartitionStoreV1) vectorPartitionLifecycleAuthorityEntriesWithContextV1(ctx context.Context, collection, index string) ([]vectorPartitionLifecycleCheckpointEntryV1, error) {
	dir, err := s.openDir()
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	entries, err := readVectorPartitionDirEntriesBoundedV1(dir)
	if err != nil {
		return nil, err
	}
	identityPrefix := safeVPM(collection) + "-" + safeVPM(index)
	lifecyclePrefix := vectorPartitionLifecycleNamePrefixV1(collection, index)
	selected := make([]vectorPartitionLifecycleCheckpointEntryV1, 0)
	var physicalBytes uint64
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !strings.HasPrefix(entry.Name(), identityPrefix) {
			continue
		}
		if !strings.HasPrefix(entry.Name(), lifecyclePrefix) {
			return nil, fmt.Errorf("%w: legacy or unexpected vector partition authority %q", ErrVectorPartitionManifestInvalid, entry.Name())
		}
		parsed, err := parseVectorPartitionLifecycleCheckpointEntryNameV1(collection, index, entry.Name())
		if err != nil {
			return nil, err
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("%w: lifecycle checkpoint entry %q is a symlink", ErrVectorPartitionManifestInvalid, entry.Name())
		}
		file, identity, exactBytes, err := inspectVectorPartitionLifecycleCheckpointEntryV1(dir, entry.Name(), os.O_RDONLY)
		if err != nil {
			return nil, err
		}
		closeErr := file.Close()
		if closeErr != nil {
			return nil, closeErr
		}
		for _, existing := range selected {
			if rootpublication.SamePhysicalIdentity(existing.identity, identity) {
				return nil, fmt.Errorf("%w: lifecycle checkpoint entries alias one physical file", ErrVectorPartitionManifestInvalid)
			}
		}
		if exactBytes > uint64(vectorPartitionStoreMaxBytesV1)-physicalBytes {
			return nil, fmt.Errorf("%w: lifecycle checkpoint physical bytes cap", ErrVectorPartitionManifestInvalid)
		}
		physicalBytes += exactBytes
		parsed.bytes = exactBytes
		parsed.identity = identity
		selected = append(selected, parsed)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := s.verifyBoundDirV1(dir); err != nil {
		return nil, err
	}
	if len(selected) == 0 {
		return nil, os.ErrNotExist
	}
	return selected, nil
}

func (s *VectorPartitionStoreV1) loadVectorPartitionLifecycleAuthorityV1(collection, index string) (vectorPartitionLifecycleCheckpointStoreStateV1, bool, error) {
	return s.loadVectorPartitionLifecycleAuthorityWithContextV1(context.Background(), collection, index)
}

func (s *VectorPartitionStoreV1) loadVectorPartitionLifecycleAuthorityWithContextV1(ctx context.Context, collection, index string) (vectorPartitionLifecycleCheckpointStoreStateV1, bool, error) {
	var zero vectorPartitionLifecycleCheckpointStoreStateV1
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return zero, false, err
	}
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
		if err := ctx.Err(); err != nil {
			return zero, false, err
		}
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
	loaded, err := s.loadVectorPartitionLifecycleCheckpointStateFromDirWithContextV1(ctx, dir, collection, index)
	if err != nil {
		return zero, false, err
	}
	if loaded.checkpoint.Epoch == 0 {
		return zero, false, fmt.Errorf("%w: lifecycle namespace without checkpoint", ErrVectorPartitionManifestInvalid)
	}
	return loaded, true, nil
}

func vectorPartitionLifecycleManifestV1(state vectorPartitionLifecycleStateV1, generation uint64, allowDeleting bool) (VectorPartitionManifestV1, error) {
	return vectorPartitionLifecycleManifestWithContextV1(context.Background(), state, generation, allowDeleting)
}

func vectorPartitionLifecycleManifestWithContextV1(ctx context.Context, state vectorPartitionLifecycleStateV1, generation uint64, allowDeleting bool) (VectorPartitionManifestV1, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return VectorPartitionManifestV1{}, err
	}
	entry, present := state.Generations[generation]
	if !present || entry.Manifest == nil || (!allowDeleting && entry.Deleting) {
		return VectorPartitionManifestV1{}, os.ErrNotExist
	}
	manifest := *entry.Manifest
	copyChunked := func(length int, copyRange func(int, int)) error {
		for offset := 0; offset < length; offset += 1024 {
			if err := ctx.Err(); err != nil {
				return err
			}
			copyRange(offset, min(offset+1024, length))
		}
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return VectorPartitionManifestV1{}, err
	}
	manifest.Placements = make([]VectorPartitionPlacementV1, len(entry.Manifest.Placements))
	if err := copyChunked(len(manifest.Placements), func(start, end int) {
		copy(manifest.Placements[start:end], entry.Manifest.Placements[start:end])
	}); err != nil {
		return VectorPartitionManifestV1{}, err
	}
	if err := ctx.Err(); err != nil {
		return VectorPartitionManifestV1{}, err
	}
	manifest.Memberships = make([]VectorPartitionMembershipV1, len(entry.Manifest.Memberships))
	if err := copyChunked(len(manifest.Memberships), func(start, end int) {
		copy(manifest.Memberships[start:end], entry.Manifest.Memberships[start:end])
	}); err != nil {
		return VectorPartitionManifestV1{}, err
	}
	if err := ctx.Err(); err != nil {
		return VectorPartitionManifestV1{}, err
	}
	manifest.OverlapMemberships = make([]VectorPartitionMembershipV1, len(entry.Manifest.OverlapMemberships))
	if err := copyChunked(len(manifest.OverlapMemberships), func(start, end int) {
		copy(manifest.OverlapMemberships[start:end], entry.Manifest.OverlapMemberships[start:end])
	}); err != nil {
		return VectorPartitionManifestV1{}, err
	}
	if err := ctx.Err(); err != nil {
		return VectorPartitionManifestV1{}, err
	}
	manifest.Representatives = make([]VectorPartitionMembershipV1, len(entry.Manifest.Representatives))
	if err := copyChunked(len(manifest.Representatives), func(start, end int) {
		copy(manifest.Representatives[start:end], entry.Manifest.Representatives[start:end])
	}); err != nil {
		return VectorPartitionManifestV1{}, err
	}
	if err := ctx.Err(); err != nil {
		return VectorPartitionManifestV1{}, err
	}
	manifest.Assets = make([]VectorPartitionAssetV1, len(entry.Manifest.Assets))
	if err := copyChunked(len(manifest.Assets), func(start, end int) {
		copy(manifest.Assets[start:end], entry.Manifest.Assets[start:end])
	}); err != nil {
		return VectorPartitionManifestV1{}, err
	}
	return manifest, nil
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
			continue
		}
		if asset.ID == vectorPartitionLocalAssetIDV1(asset.PartitionID) {
			singleAssetManifest := manifest
			singleAssetManifest.Assets = []VectorPartitionAssetV1{asset}
			if err := c.validateVectorPartitionAssetMembershipBindingsV1(singleAssetManifest); err != nil {
				staleAssets++
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
