package rootpublication

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

func TestOwnedDirectoryRecordSerializationAdmissionAndExactOutput(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	budget := &registryBudget{cap: 1 << 20}
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	baseline := owner.Bytes()
	registry := NewIdentityPinRegistry()
	defer registry.Close()
	file := writeStableResourceFixture(t, t.TempDir(), "directory.bin", strings.Repeat("x", 136))
	identity, err := StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	defer registry.Unobserve(identity)
	values := make([]StableLogicalObligation, 16)
	for i := range values {
		values[i] = appendMutationTestObligation(uint64(16 - i))
		values[i].Reachability = ReachabilityIndexFile
	}
	set := admittedCoalescingFixture(t, &owner, registry, file, "lane", DurableFrontier{Bytes: 136}, values)
	defer set.Release()
	manifest, _, err := set.DependencyManifestV1()
	if err != nil {
		t.Fatal(err)
	}
	defer manifest.ReleaseOwnedMetadataV1()
	var minimum uint64
	if err = manifest.WithEntriesV1(func(entries []DependencyManifestEntryV1) error {
		for _, entry := range entries {
			physical := entry
			physical.LogicalObligations = nil
			key := DependencyPhysicalKeyV2(physical)
			encoded, err := EncodeDependencyPhysicalV2(physical)
			if err != nil {
				return err
			}
			minimum += retainedalloc.AllocationCharge(uint64(cap(key))) + retainedalloc.AllocationCharge(uint64(cap(encoded)))
			for _, value := range entry.LogicalObligations {
				logical := DependencyLogicalKeyV2(value)
				encoded, err := EncodeDependencyLogicalV2(key, value)
				if err != nil {
					return err
				}
				minimum += retainedalloc.AllocationCharge(uint64(cap(logical))) + retainedalloc.AllocationCharge(uint64(cap(encoded)))
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	before := owner.Bytes()
	budget.cap = before
	visited := false
	err = WalkDependencyDirectoryRecordsV2(set, func(_, _ []byte) error { visited = true; return nil })
	if !errors.Is(err, retainedalloc.ErrCapacity) || visited || owner.Bytes() != before {
		t.Fatalf("refusal err=%v visited=%v bytes=%d want=%d", err, visited, owner.Bytes(), before)
	}
	budget.cap = 1 << 20
	var previous []byte
	logical, physical := 0, 0
	err = WalkDependencyDirectoryRecordsV2(set, func(key, value []byte) error {
		if owner.Bytes() < before+minimum {
			t.Fatal("serialization backing not admitted")
		}
		if previous != nil && bytes.Compare(previous, key) >= 0 {
			t.Fatal("unsorted directory records")
		}
		previous = append(previous[:0], key...)
		if key[0] == dependencyPhysicalKeyV2 {
			physical++
			_, err := DecodeDependencyPhysicalV2(key, value)
			return err
		}
		logical++
		_, obligation, err := DecodeDependencyLogicalV2(key, value)
		if err == nil && (obligation.PartID < 1 || obligation.PartID > 16 || obligation != values[16-int(obligation.PartID)]) {
			t.Fatal("different logical obligation")
		}
		return err
	})
	if err != nil || physical != 1 || logical != 16 || owner.Bytes() != before {
		t.Fatalf("serialization err=%v physical=%d logical=%d bytes=%d", err, physical, logical, owner.Bytes())
	}
	stopped := errors.New("stop serialization")
	if err = WalkDependencyDirectoryRecordsV2(set, func(_, _ []byte) error { return stopped }); !errors.Is(err, stopped) || owner.Bytes() != before {
		t.Fatalf("early stop err=%v bytes=%d", err, owner.Bytes())
	}
	manifest.ReleaseOwnedMetadataV1()
	set.Release()
	if owner.Bytes() != baseline {
		t.Fatalf("remaining serialization storage=%d baseline=%d", owner.Bytes(), baseline)
	}
}
