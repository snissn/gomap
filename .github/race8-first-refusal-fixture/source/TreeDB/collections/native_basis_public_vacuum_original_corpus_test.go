package collections

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

// This focused control sends the original fixed 256-row/16-writer corpus through
// one explicit public vacuum. It preserves the original pre-checkpoint seed
// capture; the qualified eight-row control and original witness stay unchanged.
// A pass covers this route/corpus, not the historical Close failure or its cause.
func TestNativeBasisPublicVacuumOriginalCorpusV1(t *testing.T) {
	const rows = 256
	t.Helper()
	const writers = 16
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.COWMemtableLimits = treedb.DefaultCOWMemtableLimits()
	// Keep the profile's flush/checkpoint settings and the default finite caps.
	// Only producer/background maintenance can progress during the public load.
	owner, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	var refusal valuelog.COWBasisRefusalObserver
	t.Cleanup(func() {
		// Atomic scalar receipt only. Even Close error is not a borrower join.
		final := refusal.Snapshot()
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
		// Writers have joined before any assertion can reach this cleanup.
		for _, slot := range []**NativeJSONRead{&current, &beforeRead} {
			if *slot != nil {
				read := *slot
				*slot = nil
				if err := read.Close(); err != nil {
					t.Errorf("cleanup corpus native read Close: %v", err)
				}
			}
		}
		if held != nil {
			read := held
			held = nil
			if err := read.Close(); err != nil {
				t.Errorf("cleanup held Close: %v", err)
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
	if !owner.EnableNativeBasisRefusalObserverForTest(&refusal) {
		t.Fatal("original owner diagnostic binding refused")
	}
	if owner.EnableNativeBasisRefusalObserverForTest(&refusal) {
		t.Fatal("original owner allowed duplicate diagnostic binding")
	}
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager = NewCollectionManager(backend)
	managerOpen = true
	if !manager.UsesNativeCommandWALApply() {
		t.Fatal("fixture did not select native command WAL owner")
	}
	if _, err := manager.CreateCollection(&CollectionMeta{Name: "exact_docs"}); err != nil {
		t.Fatal(err)
	}
	collection, err := manager.OpenCollection("exact_docs")
	if err != nil {
		t.Fatal(err)
	}
	if collection.nativeJSON == nil {
		t.Fatal("fixture did not select the native collection")
	}

	// Each slot has exactly one writer. Store outcomes, not document bytes. A nil
	// error is an actual ACK even when its returned ID is wrong; those separate
	// facts must not be collapsed into one success count.
	type outcome struct {
		attempted    bool
		acknowledged bool
		idMatches    bool
		uncertain    bool
		wrongID      []byte
		err          error
	}
	outcomes := make([]outcome, rows)
	insert := func(row int) bool {
		id := nativeOrdinaryConcurrentExactID(row)
		gotID, insertErr := collection.Insert(id, nativeOrdinaryConcurrentExactDocument(row))
		result := outcome{attempted: true, acknowledged: insertErr == nil, idMatches: bytes.Equal(gotID, id), err: insertErr}
		if !result.idMatches && len(gotID) != 0 {
			result.wrongID = bytes.Clone(gotID)
		}
		if insertErr != nil {
			result.uncertain = backenddb.CommitPublicationAccepted(insertErr)
		}
		outcomes[row] = result
		return result.acknowledged && result.idMatches
	}
	if !insert(0) {
		t.Fatalf("seed Insert: %+v", outcomes[0])
	}
	held, err = collection.nativeJSON.Capture(collection.name)
	if err != nil {
		t.Fatal(err)
	}
	firstBasis, valid := held.capture.Snapshot().StateToken()
	if !valid {
		t.Fatal("first ACK has no physical basis")
	}
	verifyHeld := func(stage string) {
		t.Helper()
		basis, valid := held.capture.Snapshot().StateToken()
		if !valid || basis != firstBasis {
			t.Fatalf("%s: held physical basis changed: first=%+v current=%+v valid=%t", stage, firstBasis, basis, valid)
		}
		got, err := held.Get(nativeOrdinaryConcurrentExactID(0))
		if err != nil || !bytes.Equal(got, nativeOrdinaryConcurrentExactDocument(0)) {
			t.Fatalf("%s: held seed differs: bytes=%d err=%v", stage, len(got), err)
		}
		// One post-capture key per worker distinguishes this old cut from a
		// fresh capture even though the immutable seed itself is not updated.
		for worker := 0; worker < writers; worker++ {
			got, err := held.Get(nativeOrdinaryConcurrentExactID(worker + 1))
			if len(got) != 0 || !errors.Is(err, tree.ErrKeyNotFound) {
				t.Fatalf("%s: old cut sees later row %d: bytes=%d err=%v", stage, worker+1, len(got), err)
			}
		}
	}
	verifyHeld("before concurrent load")
	start := make(chan struct{})
	var workers sync.WaitGroup
	workers.Add(writers)
	for worker := 0; worker < writers; worker++ {
		go func(worker int) {
			defer workers.Done()
			<-start
			for row := worker + 1; row < rows; row += writers {
				if !insert(row) {
					// Preserve the original outcome and stop this shard. Never
					// retry a capacity refusal, wrong ID, or uncertain mutation.
					return
				}
			}
		}(worker)
	}
	close(start)
	workers.Wait()
	var attempted, acknowledged, matchingIDs, uncertain int
	for row, result := range outcomes {
		if !result.attempted {
			continue
		}
		attempted++
		if result.acknowledged {
			acknowledged++
		}
		if result.acknowledged && result.idMatches {
			matchingIDs++
		}
		if result.uncertain {
			uncertain++
		}
		if !result.acknowledged || !result.idMatches {
			t.Errorf("original Insert row=%d ACK=%t ID_matches=%t wrong_ID=%q uncertain=%t err=%v", row, result.acknowledged, result.idMatches, result.wrongID, result.uncertain, result.err)
		}
	}
	if attempted != rows || acknowledged != rows || matchingIDs != rows || uncertain != 0 {
		t.Errorf("load incomplete: attempted=%d ACK=%d matching_ID=%d uncertain=%d want=%d/zero uncertain", attempted, acknowledged, matchingIDs, uncertain, rows)
	}

	t.Logf("basis-corpus-load: Rows=%d Writers=%d Attempted=%d ACK=%d MatchingID=%d Uncertain=%d", rows, writers, attempted, acknowledged, matchingIDs, uncertain)

	verify := func(stage string) {
		t.Helper()
		for row, result := range outcomes {
			if !result.acknowledged {
				continue // An uncertain error is never invented into an ACK.
			}
			id := nativeOrdinaryConcurrentExactID(row)
			got, err := collection.Get(id)
			if err != nil || !bytes.Equal(got, nativeOrdinaryConcurrentExactDocument(row)) {
				t.Fatalf("%s: ACK row=%d differs: bytes=%d err=%v", stage, row, len(got), err)
			}
		}
		seen := make([]bool, rows)
		count := 0
		truncated, err := collection.ScanDocumentIDsFunc(rows+1, func(id []byte) (bool, error) {
			if len(id) != len("row-000000") || !bytes.HasPrefix(id, []byte("row-")) {
				return false, fmt.Errorf("invented ID %q", id)
			}
			row, err := strconv.Atoi(string(id[4:]))
			if err != nil || row < 0 || row >= rows || !bytes.Equal(id, nativeOrdinaryConcurrentExactID(row)) {
				return false, fmt.Errorf("unexpected ID %q", id)
			}
			if seen[row] || (!outcomes[row].acknowledged && !outcomes[row].uncertain) {
				return false, fmt.Errorf("duplicate or unaccepted ID %q", id)
			}
			seen[row] = true
			count++
			return true, nil
		})
		if err != nil || truncated {
			t.Fatalf("%s: census: count=%d truncated=%t err=%v", stage, count, truncated, err)
		}
		for row, result := range outcomes {
			if result.acknowledged && !seen[row] {
				t.Fatalf("%s: census omitted ACK row=%d", stage, row)
			}
		}
		if acknowledged == rows && count != rows {
			t.Fatalf("%s: census=%d want=%d", stage, count, rows)
		}
	}
	verify("loaded")
	verifyHeld("after concurrent load")

	// Additional reads borrow the original public/native owner. They neither
	// checkpoint the seed nor manufacture a persisted pre-vacuum catalog.
	closeCorpusRead := func(stage string, slot **NativeJSONRead) {
		t.Helper()
		read := *slot
		*slot = nil
		if read == nil {
			t.Fatalf("%s: missing original corpus read", stage)
		}
		if err := read.Close(); err != nil {
			t.Fatalf("%s: checked corpus read Close: %v", stage, err)
		}
	}
	checkPersistedCatalog := func(stage string, read *NativeJSONRead) (backenddb.StateToken, uint64, uint64) {
		t.Helper()
		if read == nil || read.capture == nil || read.collection == nil || read.domain.domain == nil {
			t.Fatalf("%s: missing native capture/collection/domain", stage)
		}
		snapshot := read.capture.Snapshot()
		token, ok := snapshot.StateToken()
		if !ok || token.SystemRootPageID == 0 || snapshot.Pager() == nil {
			t.Fatalf("%s: missing persisted physical system root", stage)
		}
		catalog, err := loadCollectionCatalog(snapshot, collection.name)
		if err != nil || catalog == nil || catalog.meta.Name != collection.name {
			t.Fatalf("%s: persisted catalog: present=%t err=%v", stage, catalog != nil, err)
		}
		root := catalog.rootID(collectionPrimaryRootName(collection.name))
		domain := read.domain.domain
		if root == 0 || domain.catalogRoot != root || domain.baseRoot == 0 || len(catalog.rootOverlays) != 0 {
			t.Fatalf("%s: unsupported/unpersisted primary shape: root=%d catalog=%d base=%d overlays=%d", stage, root, domain.catalogRoot, domain.baseRoot, len(catalog.rootOverlays))
		}
		raw, found, err := getSystemValue(snapshot, nativeCatalogIncarnationPrefix+collection.name)
		if err != nil || !found {
			t.Fatalf("%s: persisted incarnation: found=%t err=%v", stage, found, err)
		}
		identity, err := nativeCatalogIdentityValue(raw, false)
		if err != nil || identity != read.collection.incarnation {
			t.Fatalf("%s: persisted/native incarnation mismatch: err=%v", stage, err)
		}
		return token, identity, root
	}
	var revisions [rows]page.EntryRevision
	verifyCorpusRead := func(stage string, read *NativeJSONRead, record bool) {
		t.Helper()
		for row, result := range outcomes {
			if !result.acknowledged {
				continue
			}
			id := nativeOrdinaryConcurrentExactID(row)
			got, err := read.Get(id)
			if err != nil || !bytes.Equal(got, nativeOrdinaryConcurrentExactDocument(row)) {
				t.Fatalf("%s: acknowledged value row=%d differs: bytes=%d err=%v", stage, row, len(got), err)
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
	beforeRead, err = collection.nativeJSON.Capture(collection.name)
	if err != nil {
		t.Fatal(err)
	}
	if beforeRead.collection == nil || beforeRead.domain.domain == nil {
		t.Fatal("pre-vacuum corpus read has no native identity/domain")
	}
	beforeToken, beforeValid := beforeRead.capture.Snapshot().StateToken()
	beforePager := beforeRead.capture.Snapshot().Pager()
	if !beforeValid || beforePager == nil {
		t.Fatal("pre-vacuum corpus read has no physical basis/pager")
	}
	// These roots may legitimately be zero: the original load has not been
	// explicitly checkpointed. The public vacuum performs its own checkpoint.
	beforeRoot := beforeRead.domain.domain.catalogRoot
	heldRoot := held.domain.domain.catalogRoot
	incarnation := beforeRead.collection.incarnation
	verifyCorpusRead("pre-vacuum original corpus", beforeRead, true)
	attemptBefore := owner.VacuumOnlineStats().AttemptID
	// Directly bracket only the selected public operation. These independent
	// atomics neither identify an atomic cut nor prove a borrower join.
	basisBefore := refusal.Snapshot()
	t.Logf("basis-vacuum-before: %+v", basisBefore)
	vacuumErr := owner.VacuumIndexOnline(t.Context())
	basisAfter := refusal.Snapshot()
	t.Logf("basis-vacuum-after: %+v", basisAfter)
	work := owner.VacuumOnlineStats()
	// Preserve actual post-call counters and work boundaries before any Fatal.
	// The packed First word remains available even when the operation refuses.
	t.Logf("basis-corpus-vacuum-boundary: AttemptBefore=%d AttemptAfter=%d WorkCompleted=%t Canceled=%t ReplacementPagerPages=%d Error=%t", attemptBefore, work.AttemptID, work.WorkCompleted, work.Canceled, work.ReplacementPagerPages, vacuumErr != nil)
	if vacuumErr != nil {
		t.Fatalf("original public VacuumIndexOnline: %v", vacuumErr)
	}
	if work.AttemptID <= attemptBefore || !work.WorkCompleted || work.Canceled || work.ReplacementPagerPages == 0 {
		t.Fatalf("public corpus vacuum did not complete actual replacement work: before_attempt=%d after_attempt=%d completed=%t canceled=%t pages=%d", attemptBefore, work.AttemptID, work.WorkCompleted, work.Canceled, work.ReplacementPagerPages)
	}
	if basisBefore.First != 0 || basisAfter.First != 0 || basisBefore.Missing || basisAfter.Missing || basisBefore.Overflow || basisAfter.Overflow {
		t.Fatalf("public corpus vacuum interval refusal/missing/overflow: before=%+v after=%+v", basisBefore, basisAfter)
	}
	if basisAfter.Calls <= basisBefore.Calls || basisAfter.Completed <= basisBefore.Completed {
		t.Fatalf("public corpus vacuum interval has no completed observed calls: before=%+v after=%+v", basisBefore, basisAfter)
	}
	for _, stage := range []valuelog.COWBasisStage{valuelog.COWBasisRefresh, valuelog.COWBasisDescriptor, valuelog.COWBasisLeafRead} {
		if basisAfter.Entered[stage] <= basisBefore.Entered[stage] || basisAfter.Finished[stage] <= basisBefore.Finished[stage] {
			t.Fatalf("public corpus vacuum interval did not enter/finish stage=%d: before=%+v after=%+v", stage, basisBefore, basisAfter)
		}
	}
	current, err = collection.nativeJSON.Capture(collection.name)
	if err != nil {
		t.Fatal(err)
	}
	afterToken, afterIncarnation, afterRoot := checkPersistedCatalog("after public corpus vacuum", current)
	if afterIncarnation != incarnation {
		t.Fatal("public corpus vacuum changed logical incarnation")
	}
	if current.capture.Snapshot().Pager() == beforePager {
		t.Fatal("public corpus vacuum did not change the actual captured pager generation")
	}
	// Vacuum's preparatory checkpoint may advance the physical applied LSN.
	// Exact logical contents/revisions, not a fabricated physical equality,
	// establish preservation of this pre-checkpoint captured corpus.
	verifyCorpusRead("after public corpus vacuum", current, false)
	verifyCorpusRead("held pre-vacuum corpus", beforeRead, false)
	if token, ok := beforeRead.capture.Snapshot().StateToken(); !ok || token != beforeToken || beforeRead.capture.Snapshot().Pager() != beforePager {
		t.Fatal("pre-vacuum held corpus physical cut changed")
	}
	verify("vacuumed")
	verifyHeld("after public vacuum")
	closeCorpusRead("post-vacuum corpus read", &current)
	closeCorpusRead("pre-vacuum corpus read", &beforeRead)
	if err := owner.Checkpoint(); err != nil {
		t.Fatalf("original checkpoint: %v", err)
	}
	verify("checkpointed")
	verifyHeld("after checkpoint")
	read := held
	held = nil
	if err := read.Close(); err != nil {
		t.Fatalf("original held Close: %v", err)
	}
	managerOpen = false
	if err := manager.Close(); err != nil {
		t.Fatalf("original manager Close: %v", err)
	}
	ownerOpen = false
	if err := owner.Close(); err != nil {
		t.Fatalf("original owner Close: %v", err)
	}
	if stats := backend.ValueLogIdentityPinRegistry().Stats(); stats.ActivePins != 0 || stats.ActiveIdentities != 0 || stats.ActiveStableNamespaceLinks != 0 {
		t.Fatalf("original owner retained physical resources: %+v", stats)
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
		t.Fatal("reopen did not select native command WAL owner")
	}
	collection, err = manager.OpenCollection("exact_docs")
	if err != nil {
		t.Fatal(err)
	}
	if collection.nativeJSON == nil {
		t.Fatal("reopen did not select the native collection")
	}
	verify("fresh reopened")
	current, err = collection.nativeJSON.Capture(collection.name)
	if err != nil {
		t.Fatal(err)
	}
	reopenedToken, reopenedIncarnation, reopenedRoot := checkPersistedCatalog("fresh corpus reopen", current)
	if reopenedIncarnation != incarnation {
		t.Fatal("fresh corpus reopen changed logical incarnation")
	}
	verifyCorpusRead("fresh corpus reopen", current, false)
	closeCorpusRead("reopened corpus read", &current)
	managerOpen = false
	if err := manager.Close(); err != nil {
		t.Fatalf("reopened manager Close: %v", err)
	}
	ownerOpen = false
	if err := owner.Close(); err != nil {
		t.Fatalf("reopened owner Close: %v", err)
	}
	if stats := backend.ValueLogIdentityPinRegistry().Stats(); stats.ActivePins != 0 || stats.ActiveIdentities != 0 || stats.ActiveStableNamespaceLinks != 0 {
		t.Fatalf("reopened owner retained physical resources: %+v", stats)
	}
	t.Logf("native JSON exact lifecycle: rows=%d writers=%d attempted=%d ACK=%d matching_ID=%d uncertain=%d held_basis=%+v", rows, writers, attempted, acknowledged, matchingIDs, uncertain, firstBasis)
	// Completion flags are emitted only after all original checked operations.
	// BeforeRoot/HeldRoot may be unsealed zero; after/reopen catalogs are real.
	t.Logf("basis-public-vacuum-work: Rows=%d Writers=%d Attempted=%d ACK=%d MatchingID=%d Uncertain=%d AttemptBefore=%d AttemptAfter=%d WorkCompleted=%t Canceled=%t ReplacementPagerPages=%d PagerChanged=true Incarnation=%d HeldRoot=%d BeforeRoot=%d AfterRoot=%d ReopenedRoot=%d BeforeSystemRoot=%d AfterSystemRoot=%d ReopenedSystemRoot=%d Revisions=%v HeldPreserved=true HeldClosed=true ManagerClosed=true OwnerClosed=true OriginalPinsEmpty=true ReopenedRead=true ReopenedReadClosed=true ReopenedManagerClosed=true ReopenedOwnerClosed=true ReopenedPinsEmpty=true", rows, writers, attempted, acknowledged, matchingIDs, uncertain, attemptBefore, work.AttemptID, work.WorkCompleted, work.Canceled, work.ReplacementPagerPages, incarnation, heldRoot, beforeRoot, afterRoot, reopenedRoot, beforeToken.SystemRootPageID, afterToken.SystemRootPageID, reopenedToken.SystemRootPageID, revisions)
}
