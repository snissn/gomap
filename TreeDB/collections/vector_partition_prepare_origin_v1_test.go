package collections

import (
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"strings"
	"testing"
)

func prepareOriginManifestForTestV1() VectorPartitionManifestV1 {
	m := testVectorPartitionManifestV1()
	m.PartitionCount = 1
	m.DomainCount = 1
	m.DomainPacks = []VectorPartitionDomainPackV1{{DomainID: 0, PackID: 0}}
	m.Placements = m.Placements[:1]
	m.Assets = m.Assets[:1]
	m.Assets[0].MembershipDigest = strings.Repeat("e", 64)
	m.Assets[0].ID = vectorPartitionLocalAssetIDV1(0)
	m.Assets[0].GraphVariant = string(vectorPartitionLocalDefaultGraphVariantV1)
	for i := range m.Memberships {
		m.Memberships[i].PartitionID = 0
	}
	m.PrepareOrigin = &VectorPartitionPrepareOriginV1{Term: 3, Index: 17, CommandDigest: strings.Repeat("c", 64)}
	m.Canonicalize()
	return m
}

func TestVectorPartitionPrepareOriginWireSchemaV1(t *testing.T) {
	legacy := testVectorPartitionManifestV1()
	before, err := EncodeVectorPartitionManifestV1(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if binary.BigEndian.Uint32(before[4:8]) != 6 {
		t.Fatal("nil origin changed schema6")
	}
	decoded, err := DecodeVectorPartitionManifestV1(before, DefaultVectorPartitionManifestLimits())
	if err != nil {
		t.Fatal(err)
	}
	after, err := EncodeVectorPartitionManifestV1(decoded)
	if err != nil || !bytes.Equal(before, after) || decoded.PrepareOrigin != nil {
		t.Fatalf("schema6 bytes changed: %v", err)
	}
	m := prepareOriginManifestForTestV1()
	raw, err := EncodeVectorPartitionManifestV1(m)
	if err != nil {
		t.Fatal(err)
	}
	if binary.BigEndian.Uint32(raw[4:8]) != 8 {
		t.Fatal("prepared manifest did not select schema8")
	}
	got, err := DecodeVectorPartitionManifestV1(raw, DefaultVectorPartitionManifestLimits())
	if err != nil || got.PrepareOrigin == nil || *got.PrepareOrigin != *m.PrepareOrigin || got.IntegrityDigest != m.IntegrityDigest {
		t.Fatalf("schema8 roundtrip=%+v err=%v", got, err)
	}
	cloned := cloneVectorPartitionManifestForCheckpointV1(got)
	cloned.PrepareOrigin.Index++
	if cloned.PrepareOrigin.Index == got.PrepareOrigin.Index {
		t.Fatal("origin clone aliases persisted identity")
	}
	for _, mutation := range []func([]byte){
		func(b []byte) { binary.BigEndian.PutUint32(b[4:8], 6) },
		func(b []byte) { b[len(b)-1] ^= 1 },
	} {
		changed := append([]byte(nil), raw...)
		mutation(changed)
		if _, err := DecodeVectorPartitionManifestV1(changed, DefaultVectorPartitionManifestLimits()); err == nil {
			t.Fatal("accepted unauthenticated origin mutation")
		}
	}
	if _, err := DecodeVectorPartitionManifestV1(raw[:len(raw)-1], DefaultVectorPartitionManifestLimits()); err == nil {
		t.Fatal("accepted truncated origin")
	}
}

func TestVectorPartitionPrepareStageReplayRequiresExactOriginV1(t *testing.T) {
	m := prepareOriginManifestForTestV1()
	v := commitlog.VectorPrepareV1{Version: 1, Operation: "prepare", Collection: m.Collection, Index: m.IndexName, Group: m.Placements[0].GroupID, IndexDefinitionDigest: m.IndexDefinitionDigest, Generation: m.Generation, MaxSourceRows: 512, SourceGeneration: m.SourceGeneration, SourceChecksum: m.SourceChecksum, SourceSchemaHash: m.SourceSchemaHash, SourceRowCount: m.SourceRowCount, Term: m.PrepareOrigin.Term, IndexPosition: m.PrepareOrigin.Index, CommandDigest: m.PrepareOrigin.CommandDigest}
	if err := validateVectorPrepareManifestV1(v, m); err != nil {
		t.Fatal(err)
	}
	local := cloneVectorPartitionManifestForCheckpointV1(m)
	logical := VectorPartitionLogicalAssetSetDigestV1(v.Group, m)
	local.Assets[0].Ref.FileID++
	local.RouterAsset.Ref.FileID++
	local.Canonicalize()
	if local.IntegrityDigest == m.IntegrityDigest || local.ReadySetDigest == m.ReadySetDigest || VectorPartitionLogicalAssetSetDigestV1(v.Group, local) != logical {
		t.Fatal("logical closure conflated local physical READY identity")
	}
	for name, mutate := range map[string]func(*VectorPartitionManifestV1){
		"no origin":        func(x *VectorPartitionManifestV1) { x.PrepareOrigin = nil },
		"other term":       func(x *VectorPartitionManifestV1) { x.PrepareOrigin.Term++ },
		"other index":      func(x *VectorPartitionManifestV1) { x.PrepareOrigin.Index++ },
		"other command":    func(x *VectorPartitionManifestV1) { x.PrepareOrigin.CommandDigest = strings.Repeat("d", 64) },
		"other source":     func(x *VectorPartitionManifestV1) { x.SourceChecksum++ },
		"other owner":      func(x *VectorPartitionManifestV1) { x.Placements[0].GroupID = "other" },
		"other generation": func(x *VectorPartitionManifestV1) { x.Generation++ },
	} {
		t.Run(name, func(t *testing.T) {
			changed := cloneVectorPartitionManifestForCheckpointV1(m)
			mutate(&changed)
			changed.Canonicalize()
			if err := validateVectorPrepareManifestV1(v, changed); err == nil {
				t.Fatal("replay adopted another command/source/build")
			}
		})
	}
	changed := cloneVectorPartitionManifestForCheckpointV1(m)
	changed.PrepareOrigin.Index = 0
	changed.Canonicalize()
	if !errors.Is(changed.Validate(DefaultVectorPartitionManifestLimits()), ErrVectorPartitionManifestInvalid) {
		t.Fatal("zero position did not fail closed")
	}
}
