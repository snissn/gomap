package collections

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"
)

// VectorPartitionLocalScopeV1 describes the assets prepared on one immutable
// node. It is local inventory, not catalog authority: callers must compare the
// exact digest pair and placement with fresh catalog state before serving.
type VectorPartitionLocalScopeV1 struct {
	HostedGroup     string
	Router          bool
	ManifestDigest  string
	PlacementDigest string
}

const vectorPartitionLocalScopeMagicV1 = "VLS1"

func encodeVectorPartitionLocalScopeV1(scope VectorPartitionLocalScopeV1) ([]byte, error) {
	if scope.HostedGroup == "" || len(scope.HostedGroup) > DefaultVectorPartitionManifestLimits().MaxStringBytes ||
		len(scope.HostedGroup) > 0xffff || !isSHA256VPM(scope.ManifestDigest) || !isSHA256VPM(scope.PlacementDigest) {
		return nil, fmt.Errorf("%w: local scope identity", ErrVectorPartitionManifestInvalid)
	}
	manifestDigest, err := hex.DecodeString(scope.ManifestDigest)
	if err != nil || hex.EncodeToString(manifestDigest) != scope.ManifestDigest {
		return nil, fmt.Errorf("%w: local scope manifest digest", ErrVectorPartitionManifestInvalid)
	}
	placementDigest, err := hex.DecodeString(scope.PlacementDigest)
	if err != nil || hex.EncodeToString(placementDigest) != scope.PlacementDigest {
		return nil, fmt.Errorf("%w: local scope placement digest", ErrVectorPartitionManifestInvalid)
	}
	out := make([]byte, 4+2+len(scope.HostedGroup)+1+2*sha256.Size)
	copy(out, vectorPartitionLocalScopeMagicV1)
	binary.BigEndian.PutUint16(out[4:6], uint16(len(scope.HostedGroup)))
	off := 6 + copy(out[6:], scope.HostedGroup)
	if scope.Router {
		out[off] = 1
	}
	off++
	copy(out[off:], manifestDigest)
	copy(out[off+sha256.Size:], placementDigest)
	return out, nil
}

func decodeVectorPartitionLocalScopeV1(raw []byte) (VectorPartitionLocalScopeV1, error) {
	var zero VectorPartitionLocalScopeV1
	if len(raw) < 4+2+1+2*sha256.Size || string(raw[:4]) != vectorPartitionLocalScopeMagicV1 {
		return zero, fmt.Errorf("%w: local scope header", ErrVectorPartitionManifestInvalid)
	}
	groupLen := int(binary.BigEndian.Uint16(raw[4:6]))
	if groupLen == 0 || groupLen > DefaultVectorPartitionManifestLimits().MaxStringBytes ||
		len(raw) != 4+2+groupLen+1+2*sha256.Size {
		return zero, fmt.Errorf("%w: local scope length", ErrVectorPartitionManifestInvalid)
	}
	off := 6 + groupLen
	if raw[off] > 1 {
		return zero, fmt.Errorf("%w: local scope flags", ErrVectorPartitionManifestInvalid)
	}
	scope := VectorPartitionLocalScopeV1{
		HostedGroup:     string(raw[6:off]),
		Router:          raw[off] == 1,
		ManifestDigest:  hex.EncodeToString(raw[off+1 : off+1+sha256.Size]),
		PlacementDigest: hex.EncodeToString(raw[off+1+sha256.Size:]),
	}
	canonical, err := encodeVectorPartitionLocalScopeV1(scope)
	if err != nil || !bytes.Equal(canonical, raw) {
		return zero, fmt.Errorf("%w: noncanonical local scope", ErrVectorPartitionManifestInvalid)
	}
	return scope, nil
}

func (scope VectorPartitionLocalScopeV1) validateManifestV1(manifest VectorPartitionManifestV1) error {
	if err := manifest.requireInlineRuntimeV1(); err != nil {
		return err
	}
	if _, err := encodeVectorPartitionLocalScopeV1(scope); err != nil {
		return err
	}
	if manifest.State != "building" && manifest.State != "ready" {
		return fmt.Errorf("%w: local scope manifest state", ErrVectorPartitionManifestInvalid)
	}
	if manifest.State == "ready" {
		raw, err := EncodeVectorPartitionManifestV1(manifest)
		if err != nil || fmt.Sprintf("%x", sha256.Sum256(raw)) != scope.ManifestDigest {
			return fmt.Errorf("%w: local scope manifest digest", ErrVectorPartitionManifestInvalid)
		}
	}
	if _, _, err := vectorPartitionDomainLayoutV1(manifest); err != nil {
		return err
	}
	groups := make([]string, manifest.PartitionCount)
	for _, placement := range manifest.Placements {
		if placement.PartitionID >= manifest.PartitionCount || groups[placement.PartitionID] != "" {
			return fmt.Errorf("%w: local scope placement", ErrVectorPartitionManifestInvalid)
		}
		groups[placement.PartitionID] = placement.GroupID
	}
	if len(manifest.Placements) != int(manifest.PartitionCount) {
		return fmt.Errorf("%w: incomplete local scope placement", ErrVectorPartitionManifestInvalid)
	}
	for _, group := range groups {
		if group == "" {
			return fmt.Errorf("%w: incomplete local scope placement", ErrVectorPartitionManifestInvalid)
		}
	}
	digest, err := VectorPartitionPlacementDigestV1(manifest)
	if err != nil || digest != scope.PlacementDigest {
		return fmt.Errorf("%w: local scope placement digest", ErrVectorPartitionManifestInvalid)
	}
	return nil
}

// VectorPartitionPlacementDigestV1 is the canonical logical-owner commitment
// shared by source-holder catalog preparation and owner-local inventory. It
// deliberately excludes asset filenames and the catalog epoch: the manifest
// digest binds the former, and the lifecycle index identity binds the latter.
func VectorPartitionPlacementDigestV1(manifest VectorPartitionManifestV1) (string, error) {
	if err := manifest.requireInlineRuntimeV1(); err != nil {
		return "", err
	}
	if manifest.PartitionCount == 0 || len(manifest.Placements) != int(manifest.PartitionCount) {
		return "", fmt.Errorf("%w: incomplete placement digest input", ErrVectorPartitionManifestInvalid)
	}
	groups := make([]string, manifest.PartitionCount)
	for _, placement := range manifest.Placements {
		if placement.PartitionID >= manifest.PartitionCount || groups[placement.PartitionID] != "" ||
			placement.GroupID == "" || len(placement.GroupID) > 0xffff {
			return "", fmt.Errorf("%w: noncanonical placement digest input", ErrVectorPartitionManifestInvalid)
		}
		groups[placement.PartitionID] = placement.GroupID
	}
	h := sha256.New()
	_, _ = h.Write([]byte("VPD1"))
	var word [4]byte
	binary.BigEndian.PutUint32(word[:], manifest.PartitionCount)
	_, _ = h.Write(word[:])
	for partitionID, group := range groups {
		if group == "" {
			return "", fmt.Errorf("%w: incomplete placement digest input", ErrVectorPartitionManifestInvalid)
		}
		binary.BigEndian.PutUint32(word[:], uint32(partitionID))
		_, _ = h.Write(word[:])
		binary.BigEndian.PutUint32(word[:], uint32(len(group)))
		_, _ = h.Write(word[:])
		_, _ = h.Write([]byte(group))
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func (scope VectorPartitionLocalScopeV1) localAssetsV1(manifest VectorPartitionManifestV1) ([]VectorPartitionAssetV1, error) {
	if err := scope.validateManifestV1(manifest); err != nil {
		return nil, err
	}
	groups := make([]string, manifest.PartitionCount)
	for _, placement := range manifest.Placements {
		groups[placement.PartitionID] = placement.GroupID
	}
	assets := make([]VectorPartitionAssetV1, 0, len(manifest.Assets)+1)
	segmentOwner := make(map[string]string)
	for _, asset := range manifest.Assets {
		if asset.PartitionID >= manifest.PartitionCount {
			return nil, fmt.Errorf("%w: local scope asset placement", ErrVectorPartitionManifestInvalid)
		}
		owner := groups[asset.PartitionID]
		segment := fmt.Sprintf("%s/%d", asset.Ref.Namespace, asset.Ref.FileID)
		if prior, exists := segmentOwner[segment]; exists && prior != owner {
			return nil, fmt.Errorf("%w: shared foreign segment", ErrVectorPartitionManifestInvalid)
		}
		segmentOwner[segment] = owner
		if owner == scope.HostedGroup {
			assets = append(assets, asset)
		}
	}
	if manifest.State == "ready" && manifest.RouterAsset.Ref.Kind != "" {
		segment := fmt.Sprintf("%s/%d", manifest.RouterAsset.Ref.Namespace, manifest.RouterAsset.Ref.FileID)
		if owner, exists := segmentOwner[segment]; exists && (owner != scope.HostedGroup || !scope.Router) {
			return nil, fmt.Errorf("%w: shared unhosted router segment", ErrVectorPartitionManifestInvalid)
		}
	}
	if scope.Router && manifest.State == "ready" {
		if manifest.RouterAsset.Ref.Kind == "" {
			return nil, fmt.Errorf("%w: missing scoped router", ErrVectorPartitionManifestInvalid)
		}
		assets = append(assets, manifest.RouterAsset)
	}
	if len(assets) == 0 && !scope.Router {
		return nil, fmt.Errorf("%w: scope hosts no assets", ErrVectorPartitionManifestInvalid)
	}
	return assets, nil
}

func vectorPartitionGenerationRefsV1(entry vectorPartitionLifecycleGenerationStateV1) ([]ColumnAssetRef, error) {
	if entry.Manifest == nil {
		return nil, fmt.Errorf("%w: missing generation manifest", ErrVectorPartitionManifestInvalid)
	}
	if entry.Scope == nil {
		return vectorPartitionReclaimRefsFromManifestV1(*entry.Manifest), nil
	}
	assets, err := entry.Scope.localAssetsV1(*entry.Manifest)
	if err != nil {
		return nil, err
	}
	refs := make([]ColumnAssetRef, 0, len(assets))
	for _, asset := range assets {
		refs = append(refs, asset.Ref)
	}
	sort.Slice(refs, func(i, j int) bool { return compareColumnAssetRefs(refs[i], refs[j]) < 0 })
	return refs, nil
}
