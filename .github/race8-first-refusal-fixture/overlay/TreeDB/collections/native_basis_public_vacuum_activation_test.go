package collections

import (
	"bytes"
	"errors"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

// This is a positive public-path activation control, not a reproduction or a
// repair of the original concurrent lifecycle failure. Unsupported/no-op work
// and an unobserved descriptor/leaf path fail; none of them is a skip.
func TestNativeBasisPublicVacuumActivationV1(t *testing.T) {
	const rows = 8
	const name = "basis_vacuum_activation"
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.COWMemtableLimits = treedb.DefaultCOWMemtableLimits()
	// Retain the profile's default medium, codec, generation, flush, checkpoint
	// and background policies. These ordinary documents must earn real work.
	owner, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	var observer valuelog.COWBasisRefusalObserver
	t.Cleanup(func() {
		// Registered first, so the resource cleanup below runs before this scalar
		// receipt. A failed Close is not a borrower join or permission to retry.
		final := observer.Snapshot()
		t.Logf("basis-first-refusal: %+v", final)
		if final.First != 0 || final.Missing || final.Overflow {
			t.Errorf("original owner recorded refusal/missing/overflow: %+v", final)
		}
	})
	ownerOpen := true
	var manager *CollectionManager
	managerOpen := false
	var held, beforeRead, current *NativeJSONRead
	t.Cleanup(func() {
		for _, slot := range []**NativeJSONRead{&current, &beforeRead, &held} {
			if *slot != nil {
				read := *slot
				*slot = nil
				if err := read.Close(); err != nil {
					t.Errorf("cleanup native read Close: %v", err)
				}
			}
		}
		if managerOpen {
			managerOpen = false
			if err := manager.Close(); err != nil {
				t.Errorf("cleanup manager Close: %v", err)
			}
		}
		if ownerOpen {
			ownerOpen = false
			if err := owner.Close(); err != nil {
				t.Errorf("cleanup owner Close: %v", err)
			}
		}
	})
	if !owner.EnableNativeBasisRefusalObserverForTest(&observer) {
		t.Fatal("first public owner/cache exclusive registration refused")
	}
	if owner.EnableNativeBasisRefusalObserverForTest(&observer) {
		t.Fatal("first public owner/cache allowed duplicate registration")
	}
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager = NewCollectionManager(backend)
	managerOpen = true
	if !manager.UsesNativeCommandWALApply() {
		t.Fatal("first public owner did not activate native command WAL")
	}
	if _, err := manager.CreateCollection(&CollectionMeta{Name: name}); err != nil {
		t.Fatal(err)
	}
	collection, err := manager.OpenCollection(name)
	if err != nil {
		t.Fatal(err)
	}
	if collection.nativeJSON == nil {
		t.Fatal("first public owner did not activate native collection")
	}
	closeRead := func(stage string, slot **NativeJSONRead) {
		t.Helper()
		read := *slot
		*slot = nil
		if read == nil {
			t.Fatalf("%s: missing original read", stage)
		}
		if err := read.Close(); err != nil {
			t.Fatalf("%s: checked read Close: %v", stage, err)
		}
	}
	// Check actual persisted catalog records against the exact captured native
	// descriptors. No alternate owner, manually constructed state or pin exists.
	checkCatalog := func(stage string, read *NativeJSONRead) (backenddb.StateToken, uint64, uint64) {
		t.Helper()
		if read == nil || read.capture == nil || read.collection == nil || read.domain.domain == nil {
			t.Fatalf("%s: missing native capture/collection/domain", stage)
		}
		snapshot := read.capture.Snapshot()
		token, ok := snapshot.StateToken()
		if !ok || token.SystemRootPageID == 0 || snapshot.Pager() == nil {
			t.Fatalf("%s: missing persisted physical system root", stage)
		}
		catalog, err := loadCollectionCatalog(snapshot, name)
		if err != nil || catalog == nil || catalog.meta.Name != name {
			t.Fatalf("%s: persisted catalog: present=%t err=%v", stage, catalog != nil, err)
		}
		root := catalog.rootID(collectionPrimaryRootName(name))
		domain := read.domain.domain
		if root == 0 || domain.catalogRoot != root || domain.baseRoot == 0 || len(catalog.rootOverlays) != 0 {
			t.Fatalf("%s: unsupported/unpersisted primary shape: root=%d catalog=%d base=%d overlays=%d", stage, root, domain.catalogRoot, domain.baseRoot, len(catalog.rootOverlays))
		}
		raw, found, err := getSystemValue(snapshot, nativeCatalogIncarnationPrefix+name)
		if err != nil || !found {
			t.Fatalf("%s: persisted incarnation: found=%t err=%v", stage, found, err)
		}
		incarnation, err := nativeCatalogIdentityValue(raw, false)
		if err != nil || incarnation != read.collection.incarnation {
			t.Fatalf("%s: persisted/native incarnation mismatch: err=%v", stage, err)
		}
		return token, incarnation, root
	}
	var revisions [rows]page.EntryRevision
	acknowledged, matchingIDs := 0, 0
	insert := func(row int) {
		t.Helper()
		id := nativeOrdinaryConcurrentExactID(row)
		gotID, insertErr := collection.Insert(id, nativeOrdinaryConcurrentExactDocument(row))
		if insertErr == nil {
			acknowledged++
		}
		if bytes.Equal(gotID, id) {
			matchingIDs++
		}
		if insertErr != nil || !bytes.Equal(gotID, id) {
			t.Fatalf("Insert row=%d ACK=%t matching_ID=%t uncertain=%t err=%v", row, insertErr == nil, bytes.Equal(gotID, id), backenddb.CommitPublicationAccepted(insertErr), insertErr)
		}
	}
	verify := func(stage string, read *NativeJSONRead, count int, record bool) {
		t.Helper()
		for row := 0; row < count; row++ {
			id := nativeOrdinaryConcurrentExactID(row)
			got, err := read.Get(id)
			if err != nil || !bytes.Equal(got, nativeOrdinaryConcurrentExactDocument(row)) {
				t.Fatalf("%s: acknowledged value row=%d differs: err=%v", stage, row, err)
			}
			entry, err := read.getEntry(id)
			if err != nil {
				t.Fatalf("%s: acknowledged revision row=%d: %v", stage, row, err)
			}
			if record {
				revisions[row] = entry.Revision
			} else if entry.Revision != revisions[row] {
				t.Fatalf("%s: acknowledged revision row=%d changed: got=%d want=%d", stage, row, entry.Revision, revisions[row])
			}
		}
	}
	insert(0)
	if err := owner.Checkpoint(); err != nil {
		t.Fatalf("seed persisted checkpoint: %v", err)
	}
	held, err = collection.nativeJSON.Capture(name)
	if err != nil {
		t.Fatal(err)
	}
	heldToken, incarnation, heldRoot := checkCatalog("held seed", held)
	heldPager := held.capture.Snapshot().Pager()
	verify("held seed", held, 1, true)
	for row := 1; row < rows; row++ {
		insert(row)
	}
	if acknowledged != rows || matchingIDs != rows {
		t.Fatalf("incomplete ACK/identity census: ACK=%d matching_ID=%d", acknowledged, matchingIDs)
	}
	if err := owner.Checkpoint(); err != nil {
		t.Fatalf("persisted preparation checkpoint: %v", err)
	}
	beforeRead, err = collection.nativeJSON.Capture(name)
	if err != nil {
		t.Fatal(err)
	}
	beforeToken, beforeIncarnation, beforeRoot := checkCatalog("before public vacuum", beforeRead)
	if beforeIncarnation != incarnation {
		t.Fatal("preparation changed logical incarnation")
	}
	verify("before public vacuum seed", beforeRead, 1, false)
	verify("before public vacuum", beforeRead, rows, true)
	verifyHeld := func(stage string) {
		t.Helper()
		token, identity, root := checkCatalog(stage, held)
		if token != heldToken || identity != incarnation || root != heldRoot || held.capture.Snapshot().Pager() != heldPager {
			t.Fatalf("%s: original held physical/logical cut changed", stage)
		}
		verify(stage, held, 1, false)
		for row := 1; row < rows; row++ {
			got, err := held.Get(nativeOrdinaryConcurrentExactID(row))
			if len(got) != 0 || !errors.Is(err, tree.ErrKeyNotFound) {
				t.Fatalf("%s: original held cut sees later ACK row=%d err=%v", stage, row, err)
			}
		}
	}
	verifyHeld("held before public vacuum")
	beforePager := beforeRead.capture.Snapshot().Pager()
	attemptBefore := owner.VacuumOnlineStats().AttemptID
	// Only this interval can satisfy activation. Independent atomic scalar
	// reads are not an atomic cut, equality assertion or borrower join proof.
	basisBefore := observer.Snapshot()
	t.Logf("basis-vacuum-before: %+v", basisBefore)
	vacuumErr := owner.VacuumIndexOnline(t.Context())
	basisAfter := observer.Snapshot()
	t.Logf("basis-vacuum-after: %+v", basisAfter)
	if vacuumErr != nil {
		t.Fatalf("original public VacuumIndexOnline: %v", vacuumErr)
	}
	work := owner.VacuumOnlineStats()
	if work.AttemptID <= attemptBefore || !work.WorkCompleted || work.Canceled || work.ReplacementPagerPages == 0 {
		t.Fatalf("public vacuum did not complete actual replacement work: before_attempt=%d after_attempt=%d completed=%t canceled=%t pages=%d", attemptBefore, work.AttemptID, work.WorkCompleted, work.Canceled, work.ReplacementPagerPages)
	}
	if basisBefore.First != 0 || basisAfter.First != 0 || basisBefore.Missing || basisAfter.Missing || basisBefore.Overflow || basisAfter.Overflow {
		t.Fatalf("public vacuum interval refusal/missing/overflow: before=%+v after=%+v", basisBefore, basisAfter)
	}
	if basisAfter.Calls <= basisBefore.Calls || basisAfter.Completed <= basisBefore.Completed {
		t.Fatalf("public vacuum interval has no completed observed calls: before=%+v after=%+v", basisBefore, basisAfter)
	}
	for _, stage := range []valuelog.COWBasisStage{valuelog.COWBasisRefresh, valuelog.COWBasisDescriptor, valuelog.COWBasisLeafRead} {
		if basisAfter.Entered[stage] <= basisBefore.Entered[stage] || basisAfter.Finished[stage] <= basisBefore.Finished[stage] {
			t.Fatalf("public vacuum interval did not enter/finish stage=%d: before=%+v after=%+v", stage, basisBefore, basisAfter)
		}
	}
	current, err = collection.nativeJSON.Capture(name)
	if err != nil {
		t.Fatal(err)
	}
	afterToken, afterIncarnation, afterRoot := checkCatalog("after public vacuum", current)
	if current.capture.Snapshot().Pager() == beforePager {
		t.Fatal("public vacuum did not change the actual captured pager generation")
	}
	if afterIncarnation != incarnation || afterToken.AppliedCommandLSN != beforeToken.AppliedCommandLSN || afterToken.MaxEntryRevision != beforeToken.MaxEntryRevision {
		t.Fatal("physical vacuum changed logical incarnation/command LSN/revision bound")
	}
	verify("after public vacuum", current, rows, false)
	verify("pre-vacuum held current cut", beforeRead, rows, false)
	if token, ok := beforeRead.capture.Snapshot().StateToken(); !ok || token != beforeToken || beforeRead.capture.Snapshot().Pager() != beforePager {
		t.Fatal("pre-vacuum held current physical cut changed")
	}
	verifyHeld("held after public vacuum")
	closeRead("post-vacuum read", &current)
	closeRead("pre-vacuum read", &beforeRead)
	closeRead("original held read", &held)
	managerOpen = false
	if err := manager.Close(); err != nil {
		t.Fatalf("original manager Close: %v", err)
	}
	ownerOpen = false
	if err := owner.Close(); err != nil {
		t.Fatalf("original owner Close: %v", err)
	}
	if pins := backend.ValueLogIdentityPinRegistry().Stats(); pins.ActivePins != 0 || pins.ActiveIdentities != 0 || pins.ActiveStableNamespaceLinks != 0 {
		t.Fatalf("original owner retained physical resources: %+v", pins)
	}
	owner, err = treedb.Open(opts)
	if err != nil {
		t.Fatalf("fresh reopen: %v", err)
	}
	ownerOpen = true
	backend, err = owner.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager = NewCollectionManager(backend)
	managerOpen = true
	if !manager.UsesNativeCommandWALApply() {
		t.Fatal("reopened public owner did not activate native command WAL")
	}
	collection, err = manager.OpenCollection(name)
	if err != nil {
		t.Fatal(err)
	}
	if collection.nativeJSON == nil {
		t.Fatal("reopened public owner did not activate native collection")
	}
	current, err = collection.nativeJSON.Capture(name)
	if err != nil {
		t.Fatal(err)
	}
	reopenedToken, reopenedIncarnation, reopenedRoot := checkCatalog("fresh reopen", current)
	if reopenedIncarnation != incarnation || reopenedToken.AppliedCommandLSN != afterToken.AppliedCommandLSN || reopenedToken.MaxEntryRevision != afterToken.MaxEntryRevision {
		t.Fatal("fresh reopen changed logical incarnation/command LSN/revision bound")
	}
	verify("fresh reopen", current, rows, false)
	closeRead("reopened read", &current)
	managerOpen = false
	if err := manager.Close(); err != nil {
		t.Fatalf("reopened manager Close: %v", err)
	}
	ownerOpen = false
	if err := owner.Close(); err != nil {
		t.Fatalf("reopened owner Close: %v", err)
	}
	if pins := backend.ValueLogIdentityPinRegistry().Stats(); pins.ActivePins != 0 || pins.ActiveIdentities != 0 || pins.ActiveStableNamespaceLinks != 0 {
		t.Fatalf("reopened owner retained physical resources: %+v", pins)
	}
	// Completion flags below are emitted only after their checked original calls.
	// Revisions may legitimately be legacy zero; exact recorded values, not a
	// manufactured revision, must survive vacuum, old reads and fresh reopen.
	t.Logf("basis-public-vacuum-work: Rows=%d ACK=%d MatchingID=%d AttemptBefore=%d AttemptAfter=%d WorkCompleted=%t Canceled=%t ReplacementPagerPages=%d PagerChanged=true Incarnation=%d HeldRoot=%d BeforeRoot=%d AfterRoot=%d ReopenedRoot=%d BeforeSystemRoot=%d AfterSystemRoot=%d ReopenedSystemRoot=%d Revisions=%v HeldPreserved=true HeldClosed=true ManagerClosed=true OwnerClosed=true OriginalPinsEmpty=true ReopenedRead=true ReopenedReadClosed=true ReopenedManagerClosed=true ReopenedOwnerClosed=true ReopenedPinsEmpty=true", rows, acknowledged, matchingIDs, attemptBefore, work.AttemptID, work.WorkCompleted, work.Canceled, work.ReplacementPagerPages, incarnation, heldRoot, beforeRoot, afterRoot, reopenedRoot, beforeToken.SystemRootPageID, afterToken.SystemRootPageID, reopenedToken.SystemRootPageID, revisions)
}
