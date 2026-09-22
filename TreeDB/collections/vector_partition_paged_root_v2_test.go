package collections

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"
	"testing"
)

func testVectorPartitionPagedRootV2(t testing.TB) VectorPartitionManifestV1 {
	t.Helper()
	legacy := testVectorPartitionManifestV1()
	metadata, source := legacy.Assets[0], legacy.Assets[1]
	metadata.ID = "vector_partition_metadata_root_v2"
	source.ID = "vector_partition_source_root_v2"
	metadata.PartitionID, source.PartitionID = 0, 0
	m := VectorPartitionManifestV1{
		Format: VectorPartitionManifestFormatV2,
		State: "building",
		Collection: legacy.Collection,
		IndexName: legacy.IndexName,
		IndexDefinitionDigest: legacy.IndexDefinitionDigest,
		Generation: legacy.Generation,
		PagedRootV2: &VectorPartitionPagedRootV2{
			PlacementEpoch: 11,
			SourceMapEpoch: 3,
			SourceMapDigest: strings.Repeat("c", 64),
			SourceSnapshotSetDigest: strings.Repeat("d", 64),
			GraphProfileDigest: strings.Repeat("e", 64),
			DomainCount: 4,
			PhysicalPackCount: 16,
			SourceRowCount: 1 << 40,
			MetadataDirectory: metadata,
			SourceShardDirectory: source,
		},
	}
	if err := m.canonicalizeWithContextV1(t.Context()); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestVectorPartitionPagedRootCodecV2(t *testing.T) {
	m := testVectorPartitionPagedRootV2(t)
	raw, err := EncodeVectorPartitionManifestV1(m)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) > 8192 || binary.BigEndian.Uint32(raw[4:8]) != 7 {
		t.Fatalf("bounded schema7 root: bytes=%d", len(raw))
	}
	got, err := DecodeVectorPartitionManifestV1(raw, DefaultVectorPartitionManifestLimits())
	if err != nil {
		t.Fatal(err)
	}
	if got.PagedRootV2 == nil || *got.PagedRootV2 != *m.PagedRootV2 || got.IntegrityDigest != m.IntegrityDigest || len(got.Memberships) != 0 || got.SourceRowCount != 0 {
		t.Fatalf("paged identity or invented legacy rows: %+v", got)
	}
	again, err := EncodeVectorPartitionManifestV1(got)
	if err != nil || !bytes.Equal(again, raw) {
		t.Fatalf("noncanonical round trip: %v", err)
	}
	jsonRaw, err := EncodeVectorPartitionManifestJSONV1(m)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeVectorPartitionManifestJSONV1(jsonRaw, DefaultVectorPartitionManifestLimits()); err != nil {
		t.Fatal(err)
	}
	for _, corrupt := range [][]byte{raw[:len(raw)-1], append(bytes.Clone(raw), 0), make([]byte, MaxVectorPartitionPagedRootBytesV2+1)} {
		if _, err := DecodeVectorPartitionManifestV1(corrupt, DefaultVectorPartitionManifestLimits()); !errors.Is(err, ErrVectorPartitionManifestInvalid) {
			t.Fatalf("corrupt root accepted: %v", err)
		}
	}
}

func TestVectorPartitionPagedRootBindsIdentityAndRejectsMixedV2(t *testing.T) {
	for _, tc := range []struct { name string; change func(*VectorPartitionManifestV1) }{
		{"missing root", func(m *VectorPartitionManifestV1) { m.PagedRootV2 = nil }},
		{"legacy format", func(m *VectorPartitionManifestV1) { m.Format = VectorPartitionManifestFormatV1 }},
		{"legacy rows", func(m *VectorPartitionManifestV1) { m.SourceRowCount = 1 }},
		{"legacy ordinal", func(m *VectorPartitionManifestV1) { m.Memberships = []VectorPartitionMembershipV1{{}} }},
		{"legacy placement", func(m *VectorPartitionManifestV1) { m.Placements = []VectorPartitionPlacementV1{{}} }},
		{"placement epoch", func(m *VectorPartitionManifestV1) { m.PagedRootV2.PlacementEpoch++ }},
		{"source map epoch", func(m *VectorPartitionManifestV1) { m.PagedRootV2.SourceMapEpoch++ }},
		{"source map digest", func(m *VectorPartitionManifestV1) { m.PagedRootV2.SourceMapDigest = strings.Repeat("a", 64) }},
		{"source revisions", func(m *VectorPartitionManifestV1) { m.PagedRootV2.SourceSnapshotSetDigest = strings.Repeat("a", 64) }},
		{"profile", func(m *VectorPartitionManifestV1) { m.PagedRootV2.GraphProfileDigest = strings.Repeat("a", 64) }},
		{"page directory", func(m *VectorPartitionManifestV1) { m.PagedRootV2.MetadataDirectory.Checksum = strings.Repeat("a", 64) }},
		{"source directory", func(m *VectorPartitionManifestV1) { m.PagedRootV2.SourceShardDirectory.Checksum = strings.Repeat("a", 64) }},
		{"count", func(m *VectorPartitionManifestV1) { m.PagedRootV2.SourceRowCount++ }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := testVectorPartitionPagedRootV2(t)
			tc.change(&m)
			if err := m.Validate(DefaultVectorPartitionManifestLimits()); !errors.Is(err, ErrVectorPartitionManifestInvalid) {
				t.Fatalf("changed/mixed root accepted: %v", err)
			}
		})
	}
}

func TestVectorPartitionPagedRootLegacyRuntimeRefusalV2(t *testing.T) {
	m := testVectorPartitionPagedRootV2(t)
	if _, err := NewVectorPartitionGenerationSearchOpenPlanWithContextV1(t.Context(), m); !errors.Is(err, ErrVectorPartitionPagedRuntimeUnsupportedV2) {
		t.Fatalf("legacy search plan accepted paged root: %v", err)
	}
	if _, err := NewVectorPartitionGenerationOwnerSearchOpenPlanWithContextV2(t.Context(), m, "group-a"); !errors.Is(err, ErrVectorPartitionPagedRuntimeUnsupportedV2) {
		t.Fatalf("sparse inline plan accepted paged root: %v", err)
	}
	if _, _, err := vectorPartitionDomainLayoutV1(m); !errors.Is(err, ErrVectorPartitionPagedRuntimeUnsupportedV2) {
		t.Fatalf("legacy domain layout accepted paged root: %v", err)
	}
	if _, err := newVectorPartitionReclaimStateV1(m); !errors.Is(err, ErrVectorPartitionPagedRuntimeUnsupportedV2) {
		t.Fatalf("legacy reclaim silently omitted descendant pages: %v", err)
	}
	var col *Collection
	if _, _, err := col.MaterializeVectorPartitionLocalSearchAssetsV1("embedding", m, 1, nil); !errors.Is(err, ErrVectorPartitionPagedRuntimeUnsupportedV2) {
		t.Fatalf("legacy materializer did not refuse before source access: %v", err)
	}
}

func TestVectorPartitionPagedRootCopyAndReadyBindingV2(t *testing.T) {
	building := testVectorPartitionPagedRootV2(t)
	ready := cloneVectorPartitionManifestForCheckpointV1(building)
	ready.State = "ready"
	ready.RouterGeneration = ready.Generation
	ready.RouterAsset = testVectorPartitionManifestV1().RouterAsset
	if err := ready.canonicalizeWithContextV1(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := ready.Validate(DefaultVectorPartitionManifestLimits()); err != nil {
		t.Fatal(err)
	}
	if building.PagedRootV2 == ready.PagedRootV2 {
		t.Fatal("checkpoint clone shares mutable root pointer")
	}
	if !vectorPartitionBuildingPromotionIdentityV1(building, ready) {
		t.Fatal("ready promotion lost immutable root binding")
	}
	ready.PagedRootV2.SourceMapEpoch++
	if vectorPartitionBuildingPromotionIdentityV1(building, ready) {
		t.Fatal("ready promotion changed source authority")
	}
}

func TestVectorPartitionPagedRootDecodeAllocationGrowthV2(t *testing.T) {
	// Root-codec budget only. The ordinary public generation load/page/source
	// budgets remain required before the runtime admission gate can be enabled.
	measure := func(rows uint64) int64 {
		m := testVectorPartitionPagedRootV2(t)
		m.PagedRootV2.SourceRowCount = rows
		raw, err := EncodeVectorPartitionManifestV1(m)
		if err != nil {
			t.Fatal(err)
		}
		result := testing.Benchmark(func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				if _, err := DecodeVectorPartitionManifestV1(raw, DefaultVectorPartitionManifestLimits()); err != nil {
					b.Fatal(err)
				}
			}
		})
		return result.AllocedBytesPerOp()
	}
	small, large := measure(1<<10), measure(1<<40)
	if large > small+2048 {
		t.Fatalf("root decode allocates by corpus row count: %d versus %d B/op", small, large)
	}
}
