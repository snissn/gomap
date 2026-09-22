package collections

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"testing"
)

// This probe uses only schema6 APIs so the same test can run at the admitted
// baseline and the candidate. The hosted gate compares all four digests.
func TestVectorPartitionLegacyByteCompatibilityProbeV2(t *testing.T) {
	m := testVectorPartitionManifestV1()
	raw, err := EncodeVectorPartitionManifestV1(m)
	if err != nil {
		t.Fatal(err)
	}
	if binary.BigEndian.Uint32(raw[4:8]) != 6 {
		t.Fatal("default encoder changed schema")
	}
	jsonRaw, err := EncodeVectorPartitionManifestJSONV1(m)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("VPM_SCHEMA6_COMPAT=%x/%x/%s/%s\n", sha256.Sum256(raw), sha256.Sum256(jsonRaw), m.IntegrityDigest, m.ReadySetDigest)
}
