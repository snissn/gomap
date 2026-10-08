package db

import (
	"errors"
	"math"
	"path/filepath"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

func TestPrimaryRuntimeBackingGrowthScrubsOldAliasesAndRefusesOverflow(t *testing.T) {
	a, err := primaryarena.OpenCapsule(filepath.Join(t.TempDir(), "index.db.primary"))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	runtime := &rootPublicationRuntimeV1{idx: &indexGen{primary: a}}
	before := a.MetadataOwner().Bytes()
	var values []*rootPublicationVisibleMemberV1
	if err := growPrimaryRuntimeSlice(runtime, &values, 1); err != nil {
		t.Fatal(err)
	}
	member := &rootPublicationVisibleMemberV1{}
	values = append(values, member)
	old := values[:cap(values)]
	if err := growPrimaryRuntimeSlice(runtime, &values, cap(values)+1); err != nil {
		t.Fatal(err)
	}
	if values[0] != member {
		t.Fatal("replacement lost exact member")
	}
	for _, stale := range old {
		if stale != nil {
			t.Fatal("refunded old backing retained member alias")
		}
	}
	admitted := a.MetadataOwner().Bytes()
	capacity := cap(values)
	if err := growPrimaryRuntimeSlice(runtime, &values, math.MaxInt); !errors.Is(err, retainedalloc.ErrCapacity) {
		t.Fatal("overflow not refused", err)
	}
	if a.MetadataOwner().Bytes() != admitted || cap(values) != capacity || values[0] != member {
		t.Fatal("refusal changed backing/custody")
	}
	charge := retainedalloc.AllocationCharge(uint64(cap(values)) * uint64(unsafe.Sizeof((*rootPublicationVisibleMemberV1)(nil))))
	clear(values[:cap(values)])
	values = nil
	a.MetadataOwner().RemovePending(charge)
	if a.MetadataOwner().Bytes() != before {
		t.Fatal("runtime backing cleanup changed original baseline")
	}
}
