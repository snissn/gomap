package raftplacement

import (
	"cmp"
	"encoding/json"
	"slices"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

// VectorPartitionLifecycleSourceIdentityV2 is comparable and contains only
// replica-independent input identity. Physical directory generations and local
// WAL positions must never participate in generation equality.
type VectorPartitionLifecycleSourceIdentityV2 struct {
	SourceMapEpoch     uint64 `json:"source_map_epoch"`
	SourceMapDigest    string `json:"source_map_digest"`
	SnapshotSetDigest  string `json:"snapshot_set_digest"`
	GraphProfileDigest string `json:"graph_profile_digest"`
	PlacementDigest    string `json:"placement_digest"`
}

// VectorPartitionSourceOwnerPreparationV2 is one bounded owner aggregate.
// CompletionEvidenceDigest binds local durable preparation, not source-group
// quorum commitment. Only SnapshotSetDigest participates in semantic identity.
type VectorPartitionSourceOwnerPreparationV2 struct {
	GroupID                  raftcluster.GroupID `json:"group_id"`
	ShardCount               uint64              `json:"shard_count"`
	SnapshotSetDigest        string              `json:"snapshot_set_digest"`
	CompletionEvidenceDigest string              `json:"completion_evidence_digest"`
}

func validateVectorPartitionSourceIdentityV2(identity VectorPartitionLifecycleIdentityV1) error {
	s := identity.SourceV2
	if identity.Generation == 0 || identity.Source != (VectorPartitionLifecycleSourceIdentityV1{}) || s.SourceMapEpoch == 0 ||
		!isSHA256HexVectorPartitionV1(s.SourceMapDigest) || !isSHA256HexVectorPartitionV1(s.SnapshotSetDigest) ||
		!isSHA256HexVectorPartitionV1(s.GraphProfileDigest) || !isSHA256HexVectorPartitionV1(s.PlacementDigest) {
		return ErrVectorPartitionLifecycleIdentity
	}
	return nil
}

func vectorPartitionSourceIdentityLessV2(a, b VectorPartitionLifecycleSourceIdentityV2) bool {
	if a.SourceMapEpoch != b.SourceMapEpoch {
		return a.SourceMapEpoch < b.SourceMapEpoch
	}
	if a.SourceMapDigest != b.SourceMapDigest {
		return a.SourceMapDigest < b.SourceMapDigest
	}
	if a.SnapshotSetDigest != b.SnapshotSetDigest {
		return a.SnapshotSetDigest < b.SnapshotSetDigest
	}
	if a.GraphProfileDigest != b.GraphProfileDigest {
		return a.GraphProfileDigest < b.GraphProfileDigest
	}
	return a.PlacementDigest < b.PlacementDigest
}

// VectorPartitionSourceOwnerSetDigestV2 binds canonical owner coverage and
// semantic snapshot commitments. Local completion evidence is deliberately
// excluded so replicas with different physical layouts agree on the input.
func VectorPartitionSourceOwnerSetDigestV2(owners []VectorPartitionSourceOwnerPreparationV2) (string, error) {
	owners, err := canonicalSourceOwnerPreparationsV2(owners)
	if err != nil {
		return "", err
	}
	type semanticOwner struct {
		GroupID           raftcluster.GroupID `json:"group_id"`
		ShardCount        uint64              `json:"shard_count"`
		SnapshotSetDigest string              `json:"snapshot_set_digest"`
	}
	semantic := make([]semanticOwner, len(owners))
	for i, o := range owners {
		semantic[i] = semanticOwner{o.GroupID, o.ShardCount, o.SnapshotSetDigest}
	}
	raw, err := json.Marshal(struct {
		Domain string          `json:"domain"`
		Owners []semanticOwner `json:"owners"`
	}{"treedb/source-owner-set/v2", semantic})
	if err != nil {
		return "", err
	}
	return sha256HexVectorPartitionLifecycleV1(raw), nil
}

func canonicalSourceOwnerPreparationsV2(input []VectorPartitionSourceOwnerPreparationV2) ([]VectorPartitionSourceOwnerPreparationV2, error) {
	if len(input) == 0 || len(input) > MaxVectorPartitionLifecycleGroupsV1 {
		return nil, ErrVectorPartitionLifecycleLimit
	}
	out := slices.Clone(input)
	slices.SortFunc(out, func(a, b VectorPartitionSourceOwnerPreparationV2) int { return cmp.Compare(a.GroupID, b.GroupID) })
	var total uint64
	for i, o := range out {
		if err := validateVectorPartitionLifecycleNameV1("source owner", string(o.GroupID)); err != nil {
			return nil, err
		}
		if (i > 0 && out[i-1].GroupID == o.GroupID) || o.ShardCount == 0 || o.ShardCount > MaxSourceShardsV2 ||
			!isSHA256HexVectorPartitionV1(o.SnapshotSetDigest) || !isSHA256HexVectorPartitionV1(o.CompletionEvidenceDigest) {
			return nil, ErrVectorPartitionLifecycleIdentity
		}
		total += o.ShardCount
		if total > MaxSourceShardsV2 {
			return nil, ErrVectorPartitionLifecycleLimit
		}
	}
	return out, nil
}

func canonicalVectorPartitionSourceOwnersV2(identity VectorPartitionLifecycleIdentityV1, input []VectorPartitionSourceOwnerPreparationV2, required bool) ([]VectorPartitionSourceOwnerPreparationV2, error) {
	if identity.SourceFormat != 2 || !required {
		if len(input) != 0 {
			return nil, ErrVectorPartitionLifecycleIdentity
		}
		return nil, nil
	}
	owners, err := canonicalSourceOwnerPreparationsV2(input)
	if err != nil {
		return nil, err
	}
	// Source owners are independent of ANN placement/readiness groups. Exact
	// source-map coverage is derived by the trusted preparation boundary.
	root, err := VectorPartitionSourceOwnerSetDigestV2(owners)
	if err != nil {
		return nil, err
	}
	if root != identity.SourceV2.SnapshotSetDigest {
		return nil, ErrVectorPartitionLifecycleIdentity
	}
	return owners, nil
}

func validateCatalogSourceOwnersV2(catalog CatalogV1, owners []VectorPartitionSourceOwnerPreparationV2) error {
	for _, owner := range owners {
		known := false
		for _, group := range catalog.Groups {
			if group.ID == owner.GroupID {
				known = true
				break
			}
		}
		if !known {
			return ErrUnknownGroup
		}
	}
	return nil
}
