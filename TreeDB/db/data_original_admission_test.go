package db

import (
	"bytes"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestDATAActualPublicationUsesOriginalEnrolledOwner(t *testing.T) {
	testDATAActualPublicationUsesOriginalEnrolledOwner(t, false)
}

func TestDependencyDirectoryV2ActualPublicationUsesOriginalEnrolledOwner(t *testing.T) {
	testDATAActualPublicationUsesOriginalEnrolledOwner(t, true)
}

func testDATAActualPublicationUsesOriginalEnrolledOwner(t *testing.T, directory bool) {
	dir := t.TempDir()
	opts := Options{Dir: dir, Durability: DurabilityWALOffRelaxed, ResolvedProfile: ProfileNoWALFast, DisableBackgroundPrune: true, rootPublicationFixedDelay: 100 * time.Millisecond}
	if directory {
		cfg := formatConfigFromOptions(opts)
		cfg.RequiredFeatures = appendRequiredFormatFeature(cfg.RequiredFeatures, RequiredFeatureDependencyDirectoryV2)
		if err := SaveFormatConfig(dir, cfg); err != nil {
			t.Fatal(err)
		}
	}
	d, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if d != nil {
			_ = d.Close()
		}
	}()
	idx, runtime := d.idx.Load(), d.rootPublication
	owner := d.StableResourceIdentityPinRegistry().MetadataOwner()
	if idx == nil || idx.primary != nil || runtime == nil || runtime.metadata != owner || d.StableResourceMetadataOwner() != owner {
		t.Fatal("not actual DATA route with original registry owner")
	}
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	// Plain Open deliberately has no appender. Use the maintained native-root
	// appender lifecycle so this control publishes a real external dependency.
	appender, err := newReplayInlineAppender(d, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = appender.close() }()
	d.SetValueLogAppender(appender)
	defer func() {
		if d != nil {
			d.SetValueLogAppender(nil)
		}
	}()
	pointerValue := bytes.Repeat([]byte("original-DATA-pointer|"), 256)
	ptrs, err := d.AppendValueLogValues([][]byte{pointerValue})
	if err != nil {
		t.Fatal(err)
	}
	if len(ptrs) != 1 || ptrs[0].FileID == 0 {
		t.Fatalf("actual appender pointers=%+v", ptrs)
	}
	pointer := ptrs[0]
	hold, err := d.acquireRootPublicationBuilderV1()
	if err != nil {
		t.Fatal(err)
	}
	held := true
	defer func() {
		if held {
			hold.Release()
		}
	}()
	for _, key := range []string{"one", "two"} {
		if err = d.Set([]byte(key), []byte("value-"+key)); err != nil {
			t.Fatal(err)
		}
	}
	batch := d.NewBatch().(*Batch)
	defer batch.Close()
	if err = batch.SetPointer([]byte("pointer"), pointer); err != nil {
		t.Fatal(err)
	}
	if err = batch.Write(); err != nil {
		t.Fatal(err)
	}
	if err = batch.Close(); err != nil {
		t.Fatal(err)
	}
	batch = nil
	runtime.mu.Lock()
	if len(runtime.visibleMembers)-runtime.visibleHead != 3 {
		runtime.mu.Unlock()
		t.Fatal("held original builder did not preserve three accepted DATA members")
	}
	for _, member := range runtime.visibleMembers[runtime.visibleHead:] {
		if member.primary != nil || member.transaction == nil || member.transaction.Owner() != rootpublication.ResourceOwnerCoordinator || member.install.metadata != owner {
			runtime.mu.Unlock()
			t.Fatal("accepted DATA member/terminal lost same owner")
		}
	}
	latest := runtime.visibleMembers[len(runtime.visibleMembers)-1]
	if runtime.visibleResources != latest.resources || runtime.visibleResources.MetadataOwner() != owner {
		runtime.mu.Unlock()
		t.Fatal("current DATA visible closure lost same owner")
	}
	runtime.mu.Unlock()
	if budget.Stats().TotalBytes == 0 {
		t.Fatal("actual governing budget did not observe DATA charge")
	}
	hold.Release()
	held = false
	if err = d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	selected := d.durableRoot.slotResources[d.durableRoot.slot]
	if (d.durableRoot.record.Directory.RootPageID != 0) != directory || selected.Len() == 0 || selected.MetadataOwner() != owner {
		t.Fatalf("selected physical closure=%p resources=%d owner=%p want=%p directory=%+v", selected, selected.Len(), selected.MetadataOwner(), owner, d.durableRoot.record.Directory)
	}
	if directory {
		pinnedDirectory, e := selected.DependencyDirectoryV2()
		if e != nil || pinnedDirectory == nil || pinnedDirectory.MetadataOwner() != owner {
			t.Fatalf("nonempty V2 directory owner: %p %v", pinnedDirectory, e)
		}
		refusal := &originalClosureAdmissionBudget{limit: math.MaxUint64}
		finite, e := owner.Enroll(refusal)
		if e != nil {
			t.Fatal(e)
		}
		defer finite.Close()
		before, pins, original, originalOwner := owner.Bytes(), d.valueLogIdentityPins.ActivePins(), selected, selected.Owner()
		refusal.setLimit(before)
		bound, bindErr := rootpublication.BindDependencyDirectoryV2(selected, pinnedDirectory)
		recovered, recoverErr := d.recoverDependencyDirectoryResourcesV2(pinnedDirectory)
		refusal.setLimit(math.MaxUint64)
		if bound != nil || recovered != nil || !errors.Is(bindErr, retainedalloc.ErrCapacity) || !errors.Is(recoverErr, retainedalloc.ErrCapacity) || owner.Bytes() != before || selected != original || selected.Owner() != originalOwner || d.durableRoot.slotResources[d.durableRoot.slot] != original || d.valueLogIdentityPins.ActivePins() != pins {
			t.Fatalf("V2 admission changed original closure: bind=%p/%v recovery=%p/%v bytes=%d want=%d pins=%d want=%d", bound, bindErr, recovered, recoverErr, owner.Bytes(), before, d.valueLogIdentityPins.ActivePins(), pins)
		}
		bound, e = rootpublication.BindDependencyDirectoryV2(selected, pinnedDirectory)
		if e != nil {
			t.Fatal(e)
		}
		if bound.MetadataOwner() != owner || bound.Len() == 0 {
			t.Fatal("V2 bound physical closure lost original owner")
		}
		if e = bound.ReleaseWithError(); e != nil {
			t.Fatal(e)
		}
		bound = nil
		recovered, e = d.recoverDependencyDirectoryResourcesV2(pinnedDirectory)
		if e != nil {
			t.Fatal(e)
		}
		if recovered.MetadataOwner() != owner || recovered.Len() == 0 {
			t.Fatal("V2 recovered physical closure lost original owner")
		}
		if e = recovered.ReleaseWithError(); e != nil {
			t.Fatal(e)
		}
		recovered = nil
		if owner.Bytes() != before || d.valueLogIdentityPins.ActivePins() != pins {
			t.Fatalf("V2 temporary closure did not refund: bytes=%d want=%d pins=%d want=%d", owner.Bytes(), before, d.valueLogIdentityPins.ActivePins(), pins)
		}
		finite.Close()
	}
	selected = nil
	snapshot := d.AcquireSnapshot()
	if directory && d.MinPinnedReaderCommitSeq() != snapshot.state.CommitSeq {
		t.Fatal("actual DATA snapshot lost reader-capable protection")
	}
	entry, readErr := snapshot.GetEntryExact([]byte("pointer"))
	got, valueErr := snapshot.Get([]byte("pointer"))
	closeErr := snapshot.Close()
	snapshot = nil
	if readErr != nil || valueErr != nil || closeErr != nil || entry.ValuePtr != pointer || !bytes.Equal(got, pointerValue) {
		t.Fatalf("published pointer=%+v want=%+v value=%q read=%v valueErr=%v close=%v", entry.ValuePtr, pointer, got, readErr, valueErr, closeErr)
	}
	if directory && (d.MinPinnedReaderCommitSeq() != math.MaxUint64 || d.MinPinnedSnapshotCommitSeq() == math.MaxUint64) {
		t.Fatal("actual directory must retain page reuse protection without claiming an asset reader")
	}
	d.SetValueLogAppender(nil)
	if err = appender.close(); err != nil {
		t.Fatal(err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d = nil
	idx = nil
	runtime = nil
	enrollment.Close()
	budget.Close()
	if owner.Bytes() != 0 || budget.Stats().TotalBytes != 0 {
		t.Fatalf("original DATA close retained owner/budget: %d %+v", owner.Bytes(), budget.Stats())
	}
	reopened, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	recoveredOwner := reopened.StableResourceMetadataOwner()
	recoveredBudget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	recoveredEnrollment, err := recoveredOwner.Enroll(recoveredBudget)
	if err != nil {
		t.Fatal(err)
	}
	defer recoveredEnrollment.Close()
	for _, key := range []string{"one", "two"} {
		got, e := reopened.Get([]byte(key))
		if e != nil || !bytes.Equal(got, []byte("value-"+key)) {
			t.Fatalf("reopen %s=%q err=%v", key, got, e)
		}
	}
	selected = reopened.durableRoot.slotResources[reopened.durableRoot.slot]
	if (reopened.durableRoot.record.Directory.RootPageID != 0) != directory || selected.Len() == 0 || selected.MetadataOwner() != recoveredOwner {
		t.Fatalf("recovered physical closure=%p resources=%d owner=%p want=%p directory=%+v", selected, selected.Len(), selected.MetadataOwner(), recoveredOwner, reopened.durableRoot.record.Directory)
	}
	selected = nil
	snapshot = reopened.AcquireSnapshot()
	entry, readErr = snapshot.GetEntryExact([]byte("pointer"))
	got, valueErr = snapshot.Get([]byte("pointer"))
	closeErr = snapshot.Close()
	snapshot = nil
	if readErr != nil || valueErr != nil || closeErr != nil || entry.ValuePtr != pointer || !bytes.Equal(got, pointerValue) {
		t.Fatalf("recovered pointer=%+v want=%+v value=%q read=%v valueErr=%v close=%v", entry.ValuePtr, pointer, got, readErr, valueErr, closeErr)
	}
	if err = reopened.Close(); err != nil {
		t.Fatal(err)
	}
	recoveredEnrollment.Close()
	recoveredBudget.Close()
	if recoveredOwner.Bytes() != 0 || recoveredBudget.Stats().TotalBytes != 0 {
		t.Fatalf("recovered DATA close retained owner/budget: %d %+v", recoveredOwner.Bytes(), recoveredBudget.Stats())
	}
}

func TestDATAActualPublicationAdmissionRefusalPreservesVisibleRoot(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), Durability: DurabilityWALOffRelaxed, ResolvedProfile: ProfileNoWALFast, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	owner := d.StableResourceIdentityPinRegistry().MetadataOwner()
	budget := &originalClosureAdmissionBudget{limit: math.MaxUint64}
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	hold, err := d.acquireRootPublicationBuilderV1()
	if err != nil {
		t.Fatal(err)
	}
	defer hold.Release()
	before, state := owner.Bytes(), d.state.Load()
	budget.setLimit(before)
	err = d.Set([]byte("denied"), []byte("value"))
	budget.setLimit(math.MaxUint64)
	if !errors.Is(err, retainedalloc.ErrCapacity) || CommitPublicationAccepted(err) || d.state.Load() != state || owner.Bytes() != before {
		t.Fatalf("DATA refusal changed original visible custody: err=%v accepted=%t bytes=%d before=%d", err, CommitPublicationAccepted(err), owner.Bytes(), before)
	}
	got, err := d.Get([]byte("denied"))
	if err != nil || got != nil {
		t.Fatalf("refused key visible: %q %v", got, err)
	}
	if err = d.Set([]byte("accepted"), []byte("value")); err != nil {
		t.Fatal(err)
	}
}
