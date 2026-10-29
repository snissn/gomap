package collections

import (
	"bytes"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// Original APIs and original finite owners: four real insert members already
// cross the old total-envelope watermark, but do not fill 75% of the capacity
// remaining after the one shared construction workspace. This is a scheduling
// oracle, not a smaller promise or permission to omit a physical reservation.
func TestNativeTypedProducerPressureSeparatesSharedWorkspace(t *testing.T) {
	if !ordinaryColumnStableAuthorityEnabled() {
		t.Skip("stable relative namespace unavailable")
	}
	dir := t.TempDir()
	f := openNativePressureDrainFixture(t, dir, true, true)
	for member := 0; member < 4; member++ {
		f.append(t, 128)
		if member == 0 {
			var err error
			f.held, err = f.col.OpenCollectionReadView()
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	before, err := f.backend.ObserveNativeProducer()
	if err != nil {
		t.Fatal(err)
	}
	read, err := f.col.nativeJSON.Capture("docs")
	if err != nil {
		t.Fatal(err)
	}
	workspace, capacityErr := nativeTypedOperationWorkspaceCapacity(read.meta, false, false)
	if err = read.Close(); err != nil {
		t.Fatal(err)
	}
	if capacityErr != nil {
		t.Fatal(capacityErr)
	}
	p := before.Semantic
	if p.Sources != 4 || workspace == 0 || workspace >= p.MaxInFlightBytes || p.ConstructionBytes < workspace {
		t.Fatalf("actual pending four-member inventory=%+v workspace=%d", before, workspace)
	}
	old := p.MaxInFlightBytes - p.MaxInFlightBytes/4
	residual := p.MaxInFlightBytes - workspace
	proposed := workspace + residual - residual/4
	if p.ConstructionBytes < old || p.ConstructionBytes >= proposed || p.ConstructionBytes > p.MaxTotalBytes {
		t.Fatalf("fixture must isolate old/new scheduling: construction=%d old=%d proposed=%d inventory=%+v", p.ConstructionBytes, old, proposed, before)
	}
	limits := f.budget.Limits()
	if before.ReservedBytes >= limits.MaxInFlightBytes-limits.MaxInFlightBytes/4 || before.Sources >= limits.MaxSources || before.Generations >= limits.MaxGenerations || p.Sources >= p.MaxSources {
		t.Fatalf("fixture reached an independent pressure operand: %+v", before)
	}
	next := f.backend.CommandWALNextLSN()
	after, err := f.backend.PrepareNativeProducerObservation()
	if err != nil {
		t.Fatal(err)
	}
	if next != f.backend.CommandWALNextLSN() || after.LogicalSequence != before.LogicalSequence {
		t.Fatal("advisory preparation assigned a command or changed logical identity")
	}
	f.assertRows(t)
	ids := make([][]byte, 3)
	for i, row := range []int{0, 127, 128} {
		ids[i], _, _ = nativePressureDrainRow(row)
	}
	held, err := f.held.FetchDocumentsByID(ids, DocumentFetchOptions{})
	if err != nil || len(held.Results) != len(ids) {
		t.Fatalf("held original member: %+v: %v", held, err)
	}
	for i, result := range held.Results {
		_, want, _ := nativePressureDrainRow([]int{0, 127, 128}[i])
		if result.Found != (i < 2) || (i < 2 && !bytes.Equal(result.Document, want)) || (i == 2 && result.Document != nil) {
			t.Fatalf("held cut changed: row=%d result=%+v", i, result)
		}
	}
	// Both implementations must satisfy lifetime, exact content and fresh replay
	// before the final scheduling distinction. The old implementation may have
	// physically drained; it still has to pass these independent obligations.
	f.close(t)
	reopened := openNativePressureDrainFixture(t, dir, true, false)
	reopened.rows = 4 * 128
	reopened.assertRows(t)
	reopened.close(t)
	if before.UnderPressure || after.UnderPressure || after.Semantic != p || after.Basis != before.Basis {
		t.Fatalf("once-per-build workspace caused premature drain: before=%+v after=%+v workspace=%d", before, after, workspace)
	}
}

var _ backenddb.NativeProducerObservation
