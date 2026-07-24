package collections

import (
	"bytes"
	"encoding/binary"
	"math"
	"strings"
	"testing"
)

const (
	vpmBinaryFormatField = iota
	vpmBinaryStateField
	vpmBinaryCollectionField
	vpmBinaryIndexNameField
	vpmBinaryIndexDefinitionDigestField
	vpmBinaryIntegrityDigestField
	vpmBinaryBalancePolicyField
	vpmBinaryReadySetDigestField
	vpmBinaryStringFieldCount
)

const (
	vpmBinarySourceGenerationField = iota
	vpmBinarySourceChecksumField
	vpmBinarySourceSchemaHashField
	vpmBinarySourceRowCountField
	vpmBinaryGenerationField
	vpmBinaryRouterGenerationField
	vpmBinaryU64FieldCount
)

type vpmBinarySpan struct{ start, end int }

type vpmBinaryStringField struct {
	lengthOffset int
	value        vpmBinarySpan
}

type vpmBinaryPlacementItem struct {
	vpmBinarySpan
	partitionOffset int
}

type vpmBinaryMembershipItem struct {
	vpmBinarySpan
	ordinalOffset, partitionOffset int
}

type vpmBinaryAssetItem struct {
	vpmBinarySpan
	partitionOffset int
	ref             vpmBinaryColumnRef
}

type vpmBinaryColumnRef struct{ offsetOffset, lengthOffset int }

type vpmBinaryList[T any] struct {
	countOffset int
	items       []T
	end         int
}

type vpmBinaryLayout struct {
	strings              [vpmBinaryStringFieldCount]vpmBinaryStringField
	u64s                 [vpmBinaryU64FieldCount]int
	partitionCountOffset int
	routerAssets         vpmBinaryList[vpmBinaryAssetItem]
	placements           vpmBinaryList[vpmBinaryPlacementItem]
	memberships          vpmBinaryList[vpmBinaryMembershipItem]
	overlaps             vpmBinaryList[vpmBinaryMembershipItem]
	representatives      vpmBinaryList[vpmBinaryMembershipItem]
	assets               vpmBinaryList[vpmBinaryAssetItem]
	end                  int
}

type vpmBinaryLayoutCursor struct {
	t   testing.TB
	raw []byte
	off int
}

func parseVPMBinaryLayout(t testing.TB, raw []byte) vpmBinaryLayout {
	t.Helper()
	c := vpmBinaryLayoutCursor{t: t, raw: raw}
	if got := c.u32(); got != vectorPartitionManifestMagicV1 {
		t.Fatalf("binary fixture magic=%#x", got)
	}
	if got := c.u32(); got != 3 {
		t.Fatalf("binary fixture version=%d", got)
	}
	var layout vpmBinaryLayout
	for i := range layout.strings {
		layout.strings[i] = c.str()
	}
	for i := range layout.u64s {
		layout.u64s[i] = c.take(8).start
	}
	layout.partitionCountOffset = c.take(4).start
	layout.routerAssets = c.assets()
	layout.placements = c.placements()
	layout.memberships = c.memberships()
	layout.overlaps = c.memberships()
	layout.representatives = c.memberships()
	layout.assets = c.assets()
	layout.end = c.off
	if layout.end != len(raw) {
		t.Fatalf("binary layout consumed %d of %d bytes", layout.end, len(raw))
	}
	return layout
}

func (c *vpmBinaryLayoutCursor) take(n int) vpmBinarySpan {
	c.t.Helper()
	if n < 0 || c.off > len(c.raw)-n {
		c.t.Fatalf("binary layout truncated at %d taking %d of %d", c.off, n, len(c.raw))
	}
	span := vpmBinarySpan{start: c.off, end: c.off + n}
	c.off += n
	return span
}

func (c *vpmBinaryLayoutCursor) u32() uint32 {
	span := c.take(4)
	return binary.BigEndian.Uint32(c.raw[span.start:span.end])
}

func (c *vpmBinaryLayoutCursor) str() vpmBinaryStringField {
	lengthOffset := c.off
	n := c.u32()
	if uint64(n) > uint64(len(c.raw)-c.off) {
		c.t.Fatalf("binary layout string length %d exceeds %d remaining bytes", n, len(c.raw)-c.off)
	}
	return vpmBinaryStringField{lengthOffset: lengthOffset, value: c.take(int(n))}
}

func (c *vpmBinaryLayoutCursor) count() (int, int) {
	offset := c.off
	n := c.u32()
	if uint64(n) > uint64(math.MaxInt) {
		c.t.Fatalf("binary layout count %d exceeds int", n)
	}
	return offset, int(n)
}

func (c *vpmBinaryLayoutCursor) columnRef() vpmBinaryColumnRef {
	_ = c.str()
	_ = c.str()
	c.take(8 + 8 + 4)
	offsetOffset := c.take(8).start
	lengthOffset := c.take(8).start
	c.take(4)
	return vpmBinaryColumnRef{offsetOffset: offsetOffset, lengthOffset: lengthOffset}
}

func (c *vpmBinaryLayoutCursor) asset() vpmBinaryAssetItem {
	start := c.off
	partitionOffset := c.take(4).start
	_ = c.str()
	_ = c.str()
	_ = c.str()
	c.take(8)
	ref := c.columnRef()
	return vpmBinaryAssetItem{vpmBinarySpan: vpmBinarySpan{start: start, end: c.off}, partitionOffset: partitionOffset, ref: ref}
}

func (c *vpmBinaryLayoutCursor) assets() vpmBinaryList[vpmBinaryAssetItem] {
	countOffset, n := c.count()
	list := vpmBinaryList[vpmBinaryAssetItem]{countOffset: countOffset, items: make([]vpmBinaryAssetItem, n)}
	for i := range list.items {
		list.items[i] = c.asset()
	}
	list.end = c.off
	return list
}

func (c *vpmBinaryLayoutCursor) placements() vpmBinaryList[vpmBinaryPlacementItem] {
	countOffset, n := c.count()
	list := vpmBinaryList[vpmBinaryPlacementItem]{countOffset: countOffset, items: make([]vpmBinaryPlacementItem, n)}
	for i := range list.items {
		start := c.off
		partitionOffset := c.take(4).start
		_ = c.str()
		list.items[i] = vpmBinaryPlacementItem{vpmBinarySpan: vpmBinarySpan{start: start, end: c.off}, partitionOffset: partitionOffset}
	}
	list.end = c.off
	return list
}

func (c *vpmBinaryLayoutCursor) memberships() vpmBinaryList[vpmBinaryMembershipItem] {
	countOffset, n := c.count()
	list := vpmBinaryList[vpmBinaryMembershipItem]{countOffset: countOffset, items: make([]vpmBinaryMembershipItem, n)}
	for i := range list.items {
		start := c.off
		ordinalOffset := c.take(8).start
		partitionOffset := c.take(4).start
		list.items[i] = vpmBinaryMembershipItem{vpmBinarySpan: vpmBinarySpan{start: start, end: c.off}, ordinalOffset: ordinalOffset, partitionOffset: partitionOffset}
	}
	list.end = c.off
	return list
}

func cloneVPMForBinaryMutation(m VectorPartitionManifestV1) VectorPartitionManifestV1 {
	m.Placements = append([]VectorPartitionPlacementV1(nil), m.Placements...)
	m.Memberships = append([]VectorPartitionMembershipV1(nil), m.Memberships...)
	m.OverlapMemberships = append([]VectorPartitionMembershipV1(nil), m.OverlapMemberships...)
	m.Representatives = append([]VectorPartitionMembershipV1(nil), m.Representatives...)
	m.Assets = append([]VectorPartitionAssetV1(nil), m.Assets...)
	return m
}

func resealVPMBinaryMutation(t testing.TB, raw []byte, m VectorPartitionManifestV1) {
	t.Helper()
	if m.State == "ready" {
		m.ReadySetDigest = m.readyDigest()
	}
	m.IntegrityDigest = m.integrityDigest()
	layout := parseVPMBinaryLayout(t, raw)
	overwriteVPMBinaryString(t, raw, layout.strings[vpmBinaryReadySetDigestField], m.ReadySetDigest)
	overwriteVPMBinaryString(t, raw, layout.strings[vpmBinaryIntegrityDigestField], m.IntegrityDigest)
}

func overwriteVPMBinaryString(t testing.TB, raw []byte, field vpmBinaryStringField, value string) {
	t.Helper()
	if len(value) != field.value.end-field.value.start {
		t.Fatalf("cannot overwrite %d-byte binary string with %d bytes", field.value.end-field.value.start, len(value))
	}
	copy(raw[field.value.start:field.value.end], value)
}

func swapVPMBinarySpans(raw []byte, a, b vpmBinarySpan) []byte {
	if b.start < a.start {
		a, b = b, a
	}
	out := make([]byte, 0, len(raw))
	out = append(out, raw[:a.start]...)
	out = append(out, raw[b.start:b.end]...)
	out = append(out, raw[a.end:b.start]...)
	out = append(out, raw[a.start:a.end]...)
	out = append(out, raw[b.end:]...)
	return out
}

func replaceVPMBinarySpan(t testing.TB, raw []byte, dst, src vpmBinarySpan) {
	t.Helper()
	if dst.end-dst.start != src.end-src.start {
		t.Fatalf("binary spans differ in size: dst=%d src=%d", dst.end-dst.start, src.end-src.start)
	}
	copy(raw[dst.start:dst.end], append([]byte(nil), raw[src.start:src.end]...))
}

func removeVPMBinaryListItem(raw []byte, countOffset int, item vpmBinarySpan) []byte {
	count := binary.BigEndian.Uint32(raw[countOffset:])
	if count == 0 {
		panic("remove from empty VPM binary list")
	}
	out := append([]byte(nil), raw[:item.start]...)
	out = append(out, raw[item.end:]...)
	binary.BigEndian.PutUint32(out[countOffset:], count-1)
	return out
}

func requireVPMBinaryDecodeError(t testing.TB, raw []byte, limits VectorPartitionManifestLimits, want string) {
	t.Helper()
	_, err := DecodeVectorPartitionManifestV1(raw, limits)
	if err == nil {
		t.Fatal("malformed binary manifest decoded successfully")
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("decode err=%q, want substring %q", err, want)
	}
}

func TestVectorPartitionManifestV1RouterAssetFramingIsExactlyOne(t *testing.T) {
	for _, state := range []string{"ready", "building"} {
		t.Run(state, func(t *testing.T) {
			m := testVectorPartitionManifestV1()
			if state == "building" {
				m.State, m.RouterGeneration, m.RouterAsset, m.ReadySetDigest = "building", 0, VectorPartitionAssetV1{}, ""
				m.Canonicalize()
			}
			raw, err := EncodeVectorPartitionManifestV1(m)
			if err != nil {
				t.Fatal(err)
			}
			layout := parseVPMBinaryLayout(t, raw)
			if len(layout.routerAssets.items) != 1 {
				t.Fatalf("canonical router assets=%d want 1", len(layout.routerAssets.items))
			}
			if got, err := DecodeVectorPartitionManifestV1(raw, DefaultVectorPartitionManifestLimits()); err != nil {
				t.Fatalf("canonical decode err=%v", err)
			} else if round, err := EncodeVectorPartitionManifestV1(got); err != nil || !bytes.Equal(round, raw) {
				t.Fatalf("canonical reencode exact=%t err=%v", bytes.Equal(round, raw), err)
			}
			zero := removeVPMBinaryListItem(raw, layout.routerAssets.countOffset, layout.routerAssets.items[0].vpmBinarySpan)
			requireVPMBinaryDecodeError(t, zero, DefaultVectorPartitionManifestLimits(), "router asset count")
			more := append([]byte(nil), raw[:layout.routerAssets.end]...)
			more = append(more, raw[layout.routerAssets.items[0].start:layout.routerAssets.items[0].end]...)
			more = append(more, raw[layout.routerAssets.end:]...)
			binary.BigEndian.PutUint32(more[layout.routerAssets.countOffset:], 2)
			requireVPMBinaryDecodeError(t, more, DefaultVectorPartitionManifestLimits(), "router asset count")
		})
	}
}

func TestVectorPartitionManifestV1BinaryMutationAndResealMatrix(t *testing.T) {
	base := testVectorPartitionManifestV1()
	base.OverlapMemberships = []VectorPartitionMembershipV1{{VectorOrdinal: 0, PartitionID: 1}, {VectorOrdinal: 1, PartitionID: 0}}
	base.Canonicalize()
	productionRaw, err := EncodeVectorPartitionManifestV1(base)
	if err != nil {
		t.Fatal(err)
	}
	baseLayout := parseVPMBinaryLayout(t, productionRaw)
	if _, err := DecodeVectorPartitionManifestV1(productionRaw, DefaultVectorPartitionManifestLimits()); err != nil {
		t.Fatalf("production fixture does not decode: %v", err)
	}

	t.Run("truncation", func(t *testing.T) {
		cuts := map[string]int{
			"empty":                      0,
			"magic":                      4,
			"header":                     8,
			"format_length":              baseLayout.strings[vpmBinaryFormatField].value.start,
			"fixed_identity":             baseLayout.partitionCountOffset,
			"router_asset":               baseLayout.routerAssets.end,
			"placements":                 baseLayout.placements.end,
			"home_memberships":           baseLayout.memberships.end,
			"overlap_memberships":        baseLayout.overlaps.end,
			"representative_memberships": baseLayout.representatives.end,
			"final_asset":                len(productionRaw) - 1,
		}
		for name, cut := range cuts {
			t.Run(name, func(t *testing.T) {
				want := "truncated, over-cap, or trailing record"
				if cut < 8 {
					want = "magic/version"
				}
				requireVPMBinaryDecodeError(t, productionRaw[:cut], DefaultVectorPartitionManifestLimits(), want)
			})
		}
	})

	type mutationCase struct {
		name, want string
		mutate     func(testing.TB, []byte, VectorPartitionManifestV1, vpmBinaryLayout) []byte
	}
	cases := []mutationCase{
		{
			name: "stale_digest_payload_corruption",
			want: "record integrity digest mismatch",
			mutate: func(t testing.TB, raw []byte, m VectorPartitionManifestV1, layout vpmBinaryLayout) []byte {
				binary.BigEndian.PutUint64(raw[layout.u64s[vpmBinarySourceChecksumField]:], m.SourceChecksum+1)
				return raw
			},
		},
		{
			name: "resealed_noncanonical_placement_order",
			want: "noncanonical placement",
			mutate: func(t testing.TB, raw []byte, m VectorPartitionManifestV1, layout vpmBinaryLayout) []byte {
				raw = swapVPMBinarySpans(raw, layout.placements.items[0].vpmBinarySpan, layout.placements.items[1].vpmBinarySpan)
				m.Placements[0], m.Placements[1] = m.Placements[1], m.Placements[0]
				resealVPMBinaryMutation(t, raw, m)
				return raw
			},
		},
		{
			name: "resealed_noncanonical_membership_order",
			want: "noncanonical overlap",
			mutate: func(t testing.TB, raw []byte, m VectorPartitionManifestV1, layout vpmBinaryLayout) []byte {
				raw = swapVPMBinarySpans(raw, layout.overlaps.items[0].vpmBinarySpan, layout.overlaps.items[1].vpmBinarySpan)
				m.OverlapMemberships[0], m.OverlapMemberships[1] = m.OverlapMemberships[1], m.OverlapMemberships[0]
				resealVPMBinaryMutation(t, raw, m)
				return raw
			},
		},
		{
			name: "resealed_duplicate_placement",
			want: "noncanonical placement",
			mutate: func(t testing.TB, raw []byte, m VectorPartitionManifestV1, layout vpmBinaryLayout) []byte {
				replaceVPMBinarySpan(t, raw, layout.placements.items[1].vpmBinarySpan, layout.placements.items[0].vpmBinarySpan)
				m.Placements[1] = m.Placements[0]
				resealVPMBinaryMutation(t, raw, m)
				return raw
			},
		},
		{
			name: "resealed_duplicate_home_membership",
			want: "noncanonical membership",
			mutate: func(t testing.TB, raw []byte, m VectorPartitionManifestV1, layout vpmBinaryLayout) []byte {
				replaceVPMBinarySpan(t, raw, layout.memberships.items[1].vpmBinarySpan, layout.memberships.items[0].vpmBinarySpan)
				m.Memberships[1] = m.Memberships[0]
				resealVPMBinaryMutation(t, raw, m)
				return raw
			},
		},
		{
			name: "resealed_duplicate_asset",
			want: "noncanonical assets",
			mutate: func(t testing.TB, raw []byte, m VectorPartitionManifestV1, layout vpmBinaryLayout) []byte {
				replaceVPMBinarySpan(t, raw, layout.assets.items[1].vpmBinarySpan, layout.assets.items[0].vpmBinarySpan)
				m.Assets[1] = m.Assets[0]
				resealVPMBinaryMutation(t, raw, m)
				return raw
			},
		},
		{
			name: "resealed_missing_required_placement",
			want: "incomplete ready set or capped list",
			mutate: func(t testing.TB, raw []byte, m VectorPartitionManifestV1, layout vpmBinaryLayout) []byte {
				raw = removeVPMBinaryListItem(raw, layout.placements.countOffset, layout.placements.items[1].vpmBinarySpan)
				m.Placements = m.Placements[:1]
				resealVPMBinaryMutation(t, raw, m)
				return raw
			},
		},
		{
			name: "resealed_missing_home_membership",
			want: "incomplete ready set or capped list",
			mutate: func(t testing.TB, raw []byte, m VectorPartitionManifestV1, layout vpmBinaryLayout) []byte {
				raw = removeVPMBinaryListItem(raw, layout.memberships.countOffset, layout.memberships.items[1].vpmBinarySpan)
				m.Memberships = m.Memberships[:1]
				resealVPMBinaryMutation(t, raw, m)
				return raw
			},
		},
		{
			name: "resealed_missing_ready_asset",
			want: "missing partition asset 1",
			mutate: func(t testing.TB, raw []byte, m VectorPartitionManifestV1, layout vpmBinaryLayout) []byte {
				raw = removeVPMBinaryListItem(raw, layout.assets.countOffset, layout.assets.items[1].vpmBinarySpan)
				m.Assets = m.Assets[:1]
				resealVPMBinaryMutation(t, raw, m)
				return raw
			},
		},
		{
			name: "resealed_ordinal_out_of_range",
			want: "membership ordinal/cap",
			mutate: func(t testing.TB, raw []byte, m VectorPartitionManifestV1, layout vpmBinaryLayout) []byte {
				binary.BigEndian.PutUint64(raw[layout.memberships.items[1].ordinalOffset:], m.SourceRowCount)
				m.Memberships[1].VectorOrdinal = m.SourceRowCount
				resealVPMBinaryMutation(t, raw, m)
				return raw
			},
		},
		{
			name: "resealed_partition_out_of_range",
			want: "membership unknown partition",
			mutate: func(t testing.TB, raw []byte, m VectorPartitionManifestV1, layout vpmBinaryLayout) []byte {
				binary.BigEndian.PutUint32(raw[layout.memberships.items[1].partitionOffset:], m.PartitionCount)
				m.Memberships[1].PartitionID = m.PartitionCount
				resealVPMBinaryMutation(t, raw, m)
				return raw
			},
		},
		{
			name: "resealed_column_ref_offset_int64_overflow",
			want: "column ref int64 overflow",
			mutate: func(t testing.TB, raw []byte, m VectorPartitionManifestV1, layout vpmBinaryLayout) []byte {
				binary.BigEndian.PutUint64(raw[layout.assets.items[0].ref.offsetOffset:], uint64(math.MaxInt64)+1)
				m.Assets[0].Ref.Offset = math.MinInt64
				resealVPMBinaryMutation(t, raw, m)
				return raw
			},
		},
		{
			name: "resealed_mixed_router_generation",
			want: "router generation",
			mutate: func(t testing.TB, raw []byte, m VectorPartitionManifestV1, layout vpmBinaryLayout) []byte {
				m.RouterGeneration++
				binary.BigEndian.PutUint64(raw[layout.u64s[vpmBinaryRouterGenerationField]:], m.RouterGeneration)
				resealVPMBinaryMutation(t, raw, m)
				return raw
			},
		},
		{
			name: "resealed_invalid_ready_structure",
			want: "router asset count",
			mutate: func(t testing.TB, raw []byte, m VectorPartitionManifestV1, layout vpmBinaryLayout) []byte {
				raw = removeVPMBinaryListItem(raw, layout.routerAssets.countOffset, layout.routerAssets.items[0].vpmBinarySpan)
				m.RouterAsset = VectorPartitionAssetV1{}
				resealVPMBinaryMutation(t, raw, m)
				return raw
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw := append([]byte(nil), productionRaw...)
			m := cloneVPMForBinaryMutation(base)
			raw = tc.mutate(t, raw, m, baseLayout)
			requireVPMBinaryDecodeError(t, raw, DefaultVectorPartitionManifestLimits(), tc.want)
		})
	}
}

func TestVectorPartitionManifestV1BinaryDeclaredSizesFailBeforeAllocation(t *testing.T) {
	base := testVectorPartitionManifestV1()
	raw, err := EncodeVectorPartitionManifestV1(base)
	if err != nil {
		t.Fatal(err)
	}
	layout := parseVPMBinaryLayout(t, raw)

	type declaredSizeCase struct {
		name, want string
		limits     VectorPartitionManifestLimits
		offset     int
	}
	permissive := DefaultVectorPartitionManifestLimits()
	permissive.MaxPartitions = math.MaxInt
	permissive.MaxMemberships = math.MaxInt
	permissive.MaxAssets = math.MaxInt
	permissive.MaxStringBytes = math.MaxInt
	const hugeDeclaredSize = uint32(1 << 30)
	for _, tc := range []declaredSizeCase{
		{name: "string_length", want: "string cap/truncated", limits: permissive, offset: layout.strings[vpmBinaryCollectionField].lengthOffset},
		{name: "router_asset_count", want: "asset count exceeds remaining bytes", limits: permissive, offset: layout.routerAssets.countOffset},
		{name: "placement_count", want: "placement count exceeds remaining bytes", limits: permissive, offset: layout.placements.countOffset},
		{name: "membership_count", want: "membership count exceeds remaining bytes", limits: permissive, offset: layout.memberships.countOffset},
		{name: "ready_asset_count", want: "asset count exceeds remaining bytes", limits: permissive, offset: layout.assets.countOffset},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := append([]byte(nil), raw...)
			binary.BigEndian.PutUint32(bad[tc.offset:], hugeDeclaredSize)
			// Without the remaining-byte guard, each permissive count case above
			// reaches make with a billion-element declared length.
			requireVPMBinaryDecodeError(t, bad, tc.limits, tc.want)
		})
	}

	t.Run("configured_count_cap_precedes_allocation", func(t *testing.T) {
		bad := append([]byte(nil), raw...)
		binary.BigEndian.PutUint32(bad[layout.assets.countOffset:], math.MaxUint32)
		requireVPMBinaryDecodeError(t, bad, DefaultVectorPartitionManifestLimits(), "count cap")
	})
}

func TestVectorPartitionManifestV1BinaryConfiguredLimitBoundaries(t *testing.T) {
	base := testVectorPartitionManifestV1()
	raw, err := EncodeVectorPartitionManifestV1(base)
	if err != nil {
		t.Fatal(err)
	}
	maxStringBytes := 0
	for _, value := range []string{
		base.Format,
		base.State,
		base.Collection,
		base.IndexName,
		base.IndexDefinitionDigest,
		base.IntegrityDigest,
		base.BalancePolicy,
		base.ReadySetDigest,
	} {
		if len(value) > maxStringBytes {
			maxStringBytes = len(value)
		}
	}

	type boundaryCase struct {
		name, want     string
		exact, oneOver VectorPartitionManifestLimits
	}
	defaults := DefaultVectorPartitionManifestLimits()
	maxPartitions := defaults
	maxPartitions.MaxPartitions = int(base.PartitionCount)
	maxPartitionsOver := maxPartitions
	maxPartitionsOver.MaxPartitions--
	maxMemberships := defaults
	maxMemberships.MaxMemberships = totalMembershipsVPM(base.Memberships, base.OverlapMemberships, base.Representatives)
	maxMembershipsOver := maxMemberships
	maxMembershipsOver.MaxMemberships--
	maxAssets := defaults
	maxAssets.MaxAssets = len(base.Assets)
	maxAssetsOver := maxAssets
	maxAssetsOver.MaxAssets--
	maxString := defaults
	maxString.MaxStringBytes = maxStringBytes
	maxStringOver := maxString
	maxStringOver.MaxStringBytes--
	maxBytes := defaults
	maxBytes.MaxBytes = len(raw)
	maxBytesOver := maxBytes
	maxBytesOver.MaxBytes--

	for _, tc := range []boundaryCase{
		{name: "MaxPartitions", want: "count cap", exact: maxPartitions, oneOver: maxPartitionsOver},
		{name: "MaxMemberships_aggregate", want: "total membership cap", exact: maxMemberships, oneOver: maxMembershipsOver},
		{name: "MaxAssets", want: "count cap", exact: maxAssets, oneOver: maxAssetsOver},
		{name: "MaxStringBytes", want: "string cap/truncated", exact: maxString, oneOver: maxStringOver},
		{name: "MaxBytes", want: "encoded bytes cap", exact: maxBytes, oneOver: maxBytesOver},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := DecodeVectorPartitionManifestV1(raw, tc.exact); err != nil {
				t.Fatalf("exact boundary rejected: %v", err)
			}
			requireVPMBinaryDecodeError(t, raw, tc.oneOver, tc.want)
		})
	}
}
