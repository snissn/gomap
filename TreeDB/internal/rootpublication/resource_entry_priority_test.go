package rootpublication

import (
	"encoding/binary"
	"hash"
	"hash/fnv"
	"strings"
	"testing"
)

// Freeze the existing stdlib FNV byte stream independently of compiler escape
// behavior in the production priority helpers. Priorities must not change.
func TestStableResourcePrioritiesPreserveFNVByteStream(t *testing.T) {
	writeString := func(h hash.Hash64, s string) {
		var raw [8]byte
		binary.LittleEndian.PutUint64(raw[:], uint64(len(s)))
		h.Write(raw[:])
		h.Write([]byte(s))
	}
	for _, value := range []string{"", "owner", "主/é", strings.Repeat("long", 4096)} {
		for _, number := range []uint64{0, 1, 1 << 63, ^uint64(0)} {
			logical := stableLogicalResourceKey{kind: ResourceKind(value), lane: "lane/" + value, resourceID: "id/" + value, generation: number}
			h := fnv.New64a()
			writeString(h, string(logical.kind))
			writeString(h, logical.lane)
			writeString(h, logical.resourceID)
			var raw [8]byte
			binary.LittleEndian.PutUint64(raw[:], number)
			h.Write(raw[:])
			if got := stableResourceLogicalPriority(logical); got != h.Sum64() {
				t.Fatalf("logical priority=%x want=%x", got, h.Sum64())
			}
			physical := stablePhysicalIdentityKey{platform: value, volumeID: number, objectID: [16]byte{0, 1, 0x80, 0xff, 7, 0x91, 0x82, 0x83, 0x84, 9, 10, 11, 12, 13, 14, 0xff}}
			h.Reset()
			writeString(h, physical.platform)
			h.Write(raw[:])
			h.Write(physical.objectID[:])
			if got := stableResourcePhysicalPriority(physical); got != h.Sum64() {
				t.Fatalf("physical priority=%x want=%x", got, h.Sum64())
			}
		}
	}
}

var stableResourcePriorityBenchmarkSink uint64

func BenchmarkStableResourcePriorities(b *testing.B) {
	logical := stableLogicalResourceKey{kind: ResourceKind("typed-asset"), lane: "owner-namespace", resourceID: "file-identity", generation: 1 << 63}
	physical := stablePhysicalIdentityKey{platform: "unix", volumeID: 1 << 63, objectID: [16]byte{0xff, 0x80, 1}}
	b.ReportAllocs()
	for b.Loop() {
		stableResourcePriorityBenchmarkSink = stableResourceLogicalPriority(logical) ^ stableResourcePhysicalPriority(physical)
	}
}
