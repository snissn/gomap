package collections

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

const VectorPartitionManifestFormatV2 = "vector_partition_paged_manifest_v2"
const MaxVectorPartitionPagedRootBytesV2 = 64 << 10

// ErrVectorPartitionPagedRuntimeUnsupportedV2 is a fail-closed admission gate.
// The schema codec does not imply that publication, serving or peer admission
// supports a paged generation. Those paths must verify all referenced local
// pages, authoritative source snapshots and transitive resource reachability.
var ErrVectorPartitionPagedRuntimeUnsupportedV2 = errors.New("collections: paged vector partition runtime admission is unavailable")

// VectorPartitionPagedRootV2 is the bounded alternative to inline global
// memberships and placements. Counts describe the complete generation; no
// count-derived allocation is permitted while decoding this root. Directory
// assets are immutable page-tree roots, not complete inline directories.
// SourceSnapshotSetDigest binds shard snapshot revisions independently of
// per-document mutation revisions carried by source rows.
type VectorPartitionPagedRootV2 struct {
	PlacementEpoch uint64
	SourceMapEpoch uint64
	SourceMapDigest string
	SourceSnapshotSetDigest string
	GraphProfileDigest string
	DomainCount uint64
	PhysicalPackCount uint64
	SourceRowCount uint64
	MetadataDirectory VectorPartitionAssetV1
	SourceShardDirectory VectorPartitionAssetV1
}

func (m VectorPartitionManifestV1) isPagedRootV2() bool {
	return m.Format == VectorPartitionManifestFormatV2 || m.PagedRootV2 != nil
}

func (m VectorPartitionManifestV1) requireInlineRuntimeV1() error {
	if m.isPagedRootV2() {
		return ErrVectorPartitionPagedRuntimeUnsupportedV2
	}
	return nil
}

func (m VectorPartitionManifestV1) validatePagedRootWithContextV2(ctx context.Context, limits VectorPartitionManifestLimits, digests bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if vectorPartitionManifestIntegrityShapeErrV1 != nil {
		return vectorPartitionManifestIntegrityShapeErrV1
	}
	if m.Format != VectorPartitionManifestFormatV2 || m.PagedRootV2 == nil {
		return fmt.Errorf("%w: paged root discriminant", ErrVectorPartitionManifestInvalid)
	}
	if m.SourceGeneration != 0 || m.SourceChecksum != 0 || m.SourceSchemaHash != 0 || m.SourceRowCount != 0 || m.PartitionCount != 0 || m.DomainCount != 0 || m.BalancePolicy != "" || len(m.DomainPacks) != 0 || len(m.Placements) != 0 || len(m.Memberships) != 0 || len(m.OverlapMemberships) != 0 || len(m.Representatives) != 0 || len(m.Assets) != 0 {
		return fmt.Errorf("%w: mixed inline and paged manifest", ErrVectorPartitionManifestInvalid)
	}
	if (m.State != "building" && m.State != "ready") || m.Collection == "" || m.IndexName == "" || m.Generation == 0 || !isSHA256VPM(m.IndexDefinitionDigest) {
		return fmt.Errorf("%w: paged root identity", ErrVectorPartitionManifestInvalid)
	}
	if limits.MaxStringBytes <= 0 {
		limits = DefaultVectorPartitionManifestLimits()
	}
	for _, s := range []string{m.Collection, m.IndexName, m.IndexDefinitionDigest, m.IntegrityDigest, m.ReadySetDigest} {
		if len(s) > limits.MaxStringBytes {
			return fmt.Errorf("%w: paged root string cap", ErrVectorPartitionManifestInvalid)
		}
	}
	r := m.PagedRootV2
	if r.PlacementEpoch == 0 || r.SourceMapEpoch == 0 || !isSHA256VPM(r.SourceMapDigest) || !isSHA256VPM(r.SourceSnapshotSetDigest) || !isSHA256VPM(r.GraphProfileDigest) || r.DomainCount == 0 || r.PhysicalPackCount < r.DomainCount || r.SourceRowCount == 0 {
		return fmt.Errorf("%w: paged source, placement or graph identity", ErrVectorPartitionManifestInvalid)
	}
	for i, asset := range []VectorPartitionAssetV1{r.MetadataDirectory, r.SourceShardDirectory} {
		wantID := "vector_partition_metadata_root_v2"
		if i == 1 {
			wantID = "vector_partition_source_root_v2"
		}
		if asset.ID != wantID || asset.PartitionID != 0 || asset.Ref.Generation != m.Generation || asset.MembershipDigest != "" || asset.GraphVariant != "" || asset.Bytes == 0 || asset.Bytes > 1<<20 {
			return fmt.Errorf("%w: paged directory root", ErrVectorPartitionManifestInvalid)
		}
		if err := validateAssetVPM(asset, limits); err != nil {
			return err
		}
	}
	if r.MetadataDirectory.Ref == r.SourceShardDirectory.Ref {
		return fmt.Errorf("%w: aliased paged directory roots", ErrVectorPartitionManifestInvalid)
	}
	if m.State == "building" {
		if m.RouterGeneration != 0 || m.RouterAsset != (VectorPartitionAssetV1{}) || m.ReadySetDigest != "" {
			return fmt.Errorf("%w: building paged root has ready references", ErrVectorPartitionManifestInvalid)
		}
	} else {
		if m.RouterGeneration != m.Generation || m.RouterAsset.PartitionID != 0 || m.RouterAsset.Ref.Generation != m.Generation || m.RouterAsset.Ref == r.MetadataDirectory.Ref || m.RouterAsset.Ref == r.SourceShardDirectory.Ref {
			return fmt.Errorf("%w: paged router identity", ErrVectorPartitionManifestInvalid)
		}
		if err := validateAssetVPM(m.RouterAsset, limits); err != nil {
			return err
		}
		if digests {
			want, err := m.pagedReadyDigestV2(ctx)
			if err != nil {
				return err
			}
			if !isSHA256VPM(m.ReadySetDigest) || m.ReadySetDigest != want {
				return fmt.Errorf("%w: paged ready digest", ErrVectorPartitionManifestInvalid)
			}
		}
	}
	if digests {
		want, err := m.pagedIntegrityDigestV2(ctx)
		if err != nil {
			return err
		}
		if !isSHA256VPM(m.IntegrityDigest) || m.IntegrityDigest != want {
			return fmt.Errorf("%w: paged integrity digest", ErrVectorPartitionManifestInvalid)
		}
	}
	return ctx.Err()
}

func (m *VectorPartitionManifestV1) canonicalizePagedRootV2(ctx context.Context) error {
	if err := m.validatePagedRootWithContextV2(ctx, DefaultVectorPartitionManifestLimits(), false); err != nil {
		return err
	}
	// These empty lists have no V2 meaning and one canonical representation.
	m.DomainPacks, m.Placements, m.Memberships, m.OverlapMemberships, m.Representatives, m.Assets = nil, nil, nil, nil, nil, nil
	if m.State == "ready" {
		digest, err := m.pagedReadyDigestV2(ctx)
		if err != nil {
			return err
		}
		m.ReadySetDigest = digest
	}
	digest, err := m.pagedIntegrityDigestV2(ctx)
	if err != nil {
		return err
	}
	m.IntegrityDigest = digest
	return nil
}

func (m VectorPartitionManifestV1) pagedReadyDigestV2(ctx context.Context) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return pagedRootJSONDigestV2(struct {
		Format string
		Collection string
		IndexName string
		IndexDefinitionDigest string
		Generation uint64
		Root *VectorPartitionPagedRootV2
		RouterGeneration uint64
		RouterAsset VectorPartitionAssetV1
	}{m.Format, m.Collection, m.IndexName, m.IndexDefinitionDigest, m.Generation, m.PagedRootV2, m.RouterGeneration, m.RouterAsset})
}

func (m VectorPartitionManifestV1) pagedIntegrityDigestV2(ctx context.Context) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	m.IntegrityDigest = ""
	return pagedRootJSONDigestV2(m)
}

func pagedRootJSONDigestV2(value any) (string, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	if len(raw) > MaxVectorPartitionPagedRootBytesV2-12 {
		return "", fmt.Errorf("%w: paged root bytes cap", ErrVectorPartitionManifestInvalid)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func encodeVectorPartitionPagedRootV2(ctx context.Context, m VectorPartitionManifestV1) ([]byte, error) {
	if err := m.canonicalizePagedRootV2(ctx); err != nil {
		return nil, err
	}
	if err := m.validatePagedRootWithContextV2(ctx, DefaultVectorPartitionManifestLimits(), true); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	if len(payload) > MaxVectorPartitionPagedRootBytesV2-12 {
		return nil, fmt.Errorf("%w: paged root bytes cap", ErrVectorPartitionManifestInvalid)
	}
	raw := make([]byte, 12+len(payload))
	binary.BigEndian.PutUint32(raw[:4], vectorPartitionManifestMagicV1)
	binary.BigEndian.PutUint32(raw[4:8], 7)
	binary.BigEndian.PutUint32(raw[8:12], uint32(len(payload)))
	copy(raw[12:], payload)
	return raw, ctx.Err()
}

func decodeVectorPartitionPagedRootV2(ctx context.Context, raw []byte, limits VectorPartitionManifestLimits) (VectorPartitionManifestV1, error) {
	var m VectorPartitionManifestV1
	if len(raw) < 12 || len(raw) > MaxVectorPartitionPagedRootBytesV2 || uint64(binary.BigEndian.Uint32(raw[8:12])) != uint64(len(raw)-12) {
		return m, fmt.Errorf("%w: paged root bytes or header", ErrVectorPartitionManifestInvalid)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw[12:]))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&m); err != nil {
		return VectorPartitionManifestV1{}, fmt.Errorf("%w: paged root JSON: %v", ErrVectorPartitionManifestInvalid, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return VectorPartitionManifestV1{}, fmt.Errorf("%w: paged root trailing data", ErrVectorPartitionManifestInvalid)
	}
	if err := m.validatePagedRootWithContextV2(ctx, limits, true); err != nil {
		return VectorPartitionManifestV1{}, err
	}
	canonical, err := encodeVectorPartitionPagedRootV2(ctx, m)
	if err != nil || !bytes.Equal(raw, canonical) {
		return VectorPartitionManifestV1{}, fmt.Errorf("%w: noncanonical paged root", ErrVectorPartitionManifestInvalid)
	}
	return m, nil
}
