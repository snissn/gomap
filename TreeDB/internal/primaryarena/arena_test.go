package primaryarena

import (
	"crypto/sha256"
	"fmt"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"path/filepath"
	"testing"
)

func testClaim(t *testing.T, a *Arena, class Class) Ref {
	t.Helper()
	c, ok, e := a.PrepareClaim(class, nil)
	if e != nil || !ok {
		t.Fatalf("prepare: %v %t", e, ok)
	}
	for turn := 0; turn < 100; turn++ {
		w := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		r, ready, e := c.Step(w)
		if e != nil {
			t.Fatal(e)
		}
		if ready {
			return r
		}
	}
	t.Fatal("claim made no bounded progress")
	return Ref{}
}
func testComponent(t *testing.T, a *Arena, value string, rev page.EntryRevision) Ref {
	t.Helper()
	r := testClaim(t, a, Component)
	image := make([]byte, page.PageSize)
	b := node.NewBuilderWithOptions(image, page.PageTypeLeaf, node.BuilderOptions{EntryRevisions: true})
	b.SetPageID(r.PageID)
	if e := b.AddLeafEntryWithRevision([]byte("a"), []byte(value), 0, page.ValuePtr{}, rev); e != nil {
		t.Fatal(e)
	}
	b.FinishNoNode()
	ok, e := a.SealComponent(r, image, nil)
	if e != nil || !ok {
		t.Fatalf("seal: %v %t", e, ok)
	}
	return r
}
func testDirectory(t *testing.T, a *Arena, component Ref, rev page.EntryRevision) Ref {
	t.Helper()
	r := testClaim(t, a, Directory)
	image, e := a.Get(component.PageID)
	if e != nil {
		t.Fatal(e)
	}
	var refs [groupSize]Ref
	refs[0] = component
	g, ok, e := a.NewGroup(refs, nil)
	if !ok || e != nil {
		t.Fatalf("group: %v %t", e, ok)
	}
	var groups [groupCount]*Group
	groups[0] = g
	dir := make([]byte, page.PageSize)
	base := node.PrimaryOperand{Ref: page.PageChildRef(2), Digest: sha256.Sum256([]byte("immutable separately-owned DATA base"))}
	entry := node.PrimaryDirectoryEntry{Key: []byte("a"), Operand: node.PrimaryOperand{Ref: page.PageChildRef(component.PageID), Digest: sha256.Sum256(image)}, Revision: rev, Kind: node.PrimaryPut}
	if e = node.EncodePrimaryDirectory(dir, r.PageID, 1, base, []node.PrimaryDirectoryEntry{entry}); e != nil {
		t.Fatal(e)
	}
	if ok, e = a.SealDirectory(r, dir, groups, nil); !ok || e != nil {
		t.Fatalf("directory: %v %t", e, ok)
	}
	if _, e = a.DropGroup(g, nil); e != nil {
		t.Fatal(e)
	}
	if _, e = a.Drop(component, nil); e != nil {
		t.Fatal(e)
	}
	return r
}
func testDrain(t *testing.T, a *Arena) {
	t.Helper()
	for turn := 0; turn < 200; turn++ {
		w := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		ok, progress, e := a.ReleaseStep(w)
		if e != nil || !ok {
			t.Fatalf("release: %v %t", e, ok)
		}
		if !progress {
			return
		}
	}
	t.Fatal("release made no bounded progress")
}
func TestPrimaryArenaIndependentCustodyAndIncrementalReuse(t *testing.T) {
	a, e := Open(filepath.Join(t.TempDir(), "index.db.primary"))
	if e != nil {
		t.Fatal(e)
	}
	defer a.Close()
	oldComponent := testComponent(t, a, "old", 1)
	old := testDirectory(t, a, oldComponent, 1)
	// The live root, independently sealed A slot and arbitrary old reader each
	// retain an independent incoming reference.
	if _, e = a.Acquire(old, nil); e != nil {
		t.Fatal(e)
	}
	if _, e = a.Acquire(old, nil); e != nil {
		t.Fatal(e)
	}
	newer := testDirectory(t, a, testComponent(t, a, "new", 2), 2)
	if _, e = a.Acquire(newer, nil); e != nil {
		t.Fatal(e)
	}
	if _, e = a.Drop(old, nil); e != nil {
		t.Fatal(e)
	}
	testDrain(t, a)
	if a.valid(old) == nil || a.valid(oldComponent) == nil {
		t.Fatal("current replacement released independent A/old-reader custody")
	}
	if _, e = a.Drop(old, nil); e != nil {
		t.Fatal(e)
	}
	testDrain(t, a)
	if a.valid(old) == nil {
		t.Fatal("overwritten A slot released old reader")
	}
	if _, e = a.Drop(old, nil); e != nil {
		t.Fatal(e)
	}
	before := a.Counters()
	w := &iterator.OrdinalScanWork{RecordLimit: 1, ByteLimit: 1 << 20}
	if ok, progress, e := a.ReleaseStep(w); ok || progress || e != nil {
		t.Fatalf("unadmitted release: %t %t %v", ok, progress, e)
	}
	if a.Counters() != before {
		t.Fatal("failed admission changed custody")
	}
	testDrain(t, a)
	if a.slot(old.PageID).class != 0 || a.slot(oldComponent.PageID).class != 0 {
		t.Fatal("edge-free old banks not reusable")
	}
	reused := testClaim(t, a, Component)
	if reused.PageID != old.PageID && reused.PageID != oldComponent.PageID {
		t.Fatal("released bank not reused")
	}
	if a.valid(old) != nil {
		t.Fatal("stale incarnation acquired recycled bank")
	}
	if a.valid(newer) == nil {
		t.Fatal("new B slot/current root lost")
	}
}
func TestPrimaryArenaRecoveryBothCompleteSlotsAndPrivateTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "index.db.primary")
	a, e := Open(path)
	if e != nil {
		t.Fatal(e)
	}
	old := testDirectory(t, a, testComponent(t, a, "old", 1), 1)
	newer := testDirectory(t, a, testComponent(t, a, "new", 2), 2)
	tail := testComponent(t, a, "unpublished", 3)
	extent := a.next
	uuid := a.UUID()
	roots := make([]RecoveredRoot, 2)
	for i, r := range []Ref{old, newer} {
		image, e := a.Get(r.PageID)
		if e != nil {
			t.Fatal(e)
		}
		roots[i] = RecoveredRoot{PageID: r.PageID, Digest: sha256.Sum256(image)}
	}
	if e = a.pager.Sync(); e != nil {
		t.Fatal(e)
	}
	if e = a.Close(); e != nil {
		t.Fatal(e)
	}
	recovered, e := Open(path)
	if e != nil {
		t.Fatal(e)
	}
	defer recovered.Close()
	if recovered.UUID() != uuid {
		t.Fatal("physical UUID changed")
	}
	if _, _, e = recovered.PrepareClaim(Component, nil); e != ErrUnrecovered {
		t.Fatalf("allocation before slot closure: %v", e)
	}
	if _, e = recovered.RecoverInto(extent, roots, make([]Ref, 0, len(roots)-1)); e != ErrFormat {
		t.Fatalf("insufficient recovery output capacity: %v", e)
	}
	if recovered.recovered || recovered.chunks[Local(roots[0].PageID)/chunkBanks] != nil {
		t.Fatal("output refusal constructed physical custody")
	}
	held, e := recovered.RecoverInto(extent, roots, make([]Ref, 0, len(roots)))
	if e != nil {
		t.Fatal(e)
	}
	for _, r := range held {
		if recovered.valid(r) == nil {
			t.Fatal("complete fallback root missing")
		}
	}
	if recovered.slot(tail.PageID).refs != 0 {
		t.Fatal("private tail became durable custody")
	}
	r := testClaim(t, recovered, Component)
	if r.PageID != tail.PageID {
		t.Fatalf("private tail not reclaimed: got %d want %d", r.PageID, tail.PageID)
	}
	for _, r := range held {
		if _, e = recovered.Drop(r, nil); e != nil {
			t.Fatal(e)
		}
	}
	testDrain(t, recovered)
}

func TestPrimaryArenaExactReplacementPreservesCompleteRootAndPhysicalCertificate(t *testing.T) {
	a, e := Open(filepath.Join(t.TempDir(), "index.db.primary"))
	if e != nil {
		t.Fatal(e)
	}
	defer a.Close()
	var components [node.PrimaryDirectoryMaxEntries]Ref
	entries := make([]node.PrimaryDirectoryEntry, len(components))
	for i := range components {
		r := testClaim(t, a, Component)
		components[i] = r
		key := []byte(fmt.Sprintf("k%02d", i))
		image := make([]byte, page.PageSize)
		b := node.NewBuilderWithOptions(image, page.PageTypeLeaf, node.BuilderOptions{EntryRevisions: true})
		b.SetPageID(r.PageID)
		if e = b.AddLeafEntryWithRevision(key, []byte("old"), 0, page.ValuePtr{}, 1); e != nil {
			t.Fatal(e)
		}
		b.FinishNoNode()
		if _, e = a.SealComponent(r, image, nil); e != nil {
			t.Fatal(e)
		}
		entries[i] = node.PrimaryDirectoryEntry{Key: key, Operand: node.PrimaryOperand{Ref: page.PageChildRef(r.PageID), Digest: sha256.Sum256(image)}, Revision: 1, Kind: node.PrimaryPut, ClassSlot: uint16(i)}
	}
	var groups [groupCount]*Group
	for i := range groups {
		var refs [groupSize]Ref
		for j := range refs {
			if slot := i*groupSize + j; slot < len(components) {
				refs[j] = components[slot]
			}
		}
		groups[i], _, e = a.NewGroup(refs, nil)
		if e != nil {
			t.Fatal(e)
		}
	}
	old := testClaim(t, a, Directory)
	image := make([]byte, page.PageSize)
	base := node.PrimaryOperand{Ref: page.PageChildRef(2), Digest: [32]byte{1}}
	if e = node.EncodePrimaryDirectory(image, old.PageID, 1, base, entries); e != nil {
		t.Fatal(e)
	}
	if _, e = a.SealDirectory(old, image, groups, nil); e != nil {
		t.Fatal(e)
	}
	clear(image) // A sealed certificate must not borrow caller scratch.
	for _, g := range groups {
		if _, e = a.DropGroup(g, nil); e != nil {
			t.Fatal(e)
		}
	}
	for _, r := range components {
		if _, e = a.Drop(r, nil); e != nil {
			t.Fatal(e)
		}
	}
	absence := testClaim(t, a, Component)
	absentImage := make([]byte, page.PageSize)
	b := node.NewBuilderWithOptions(absentImage, page.PageTypeLeaf, node.BuilderOptions{EntryRevisions: true})
	b.SetPageID(absence.PageID)
	if e = b.AddLeafEntryWithRevision(entries[15].Key, nil, node.FlagTombstone, page.ValuePtr{}, 2); e != nil {
		t.Fatal(e)
	}
	b.FinishNoNode()
	if _, e = a.SealComponent(absence, absentImage, nil); e != nil {
		t.Fatal(e)
	}
	replacement := entries[15]
	replacement.Operand = node.PrimaryOperand{Ref: page.PageChildRef(absence.PageID), Digest: sha256.Sum256(absentImage)}
	replacement.Revision = 2
	replacement.Kind = node.PrimaryAbsence
	destination := testClaim(t, a, Directory)
	scratch := make([]byte, page.PageSize)
	if _, e = a.SealExactReplacement(destination, old, replacement, 9, scratch, nil); e != ErrStale {
		t.Fatalf("stale predicate accepted: %v", e)
	}
	before := a.Counters()
	short := &iterator.OrdinalScanWork{RecordLimit: 1, ByteLimit: 1 << 20}
	if ok, e := a.SealExactReplacement(destination, old, replacement, 1, scratch, short); ok || e != nil {
		t.Fatalf("unadmitted replacement: %t %v", ok, e)
	}
	if a.Counters() != before {
		t.Fatal("failed admission changed bank custody")
	}
	w := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	ok, e := a.SealExactReplacement(destination, old, replacement, 1, scratch, w)
	if e != nil || !ok {
		t.Fatalf("physical replacement unit: %t %v work=%+v", ok, e, w)
	}
	t.Logf("isolated physical seal only: records=%d bytes=%d; whole publication remains unqualified", w.Records, w.Bytes)
	clear(scratch)
	currentImage, e := a.Get(destination.PageID)
	if e != nil {
		t.Fatal(e)
	}
	d, e := node.DecodePrimaryDirectory(currentImage)
	if e != nil || d.Count() != len(entries) {
		t.Fatalf("complete root lost: %v", e)
	}
	for i := range entries {
		entry, e := d.ClassEntry(uint16(i))
		if e != nil {
			t.Fatal(e)
		}
		if i == 15 {
			if entry.Kind != node.PrimaryAbsence || entry.Revision != 2 {
				t.Fatal("exact class not replaced")
			}
		} else if entry.Operand != entries[i].Operand {
			t.Fatal("unrelated class changed")
		}
	}
	if entry, e := a.valid(old).directory.ClassEntry(15); e != nil || entry.Kind != node.PrimaryPut {
		t.Fatal("old independent root changed")
	}
	if _, e = a.Drop(absence, nil); e != nil {
		t.Fatal(e)
	}
	makeAbsence := func(slot int) (Ref, node.PrimaryDirectoryEntry) {
		r := testClaim(t, a, Component)
		image := make([]byte, page.PageSize)
		builder := node.NewBuilderWithOptions(image, page.PageTypeLeaf, node.BuilderOptions{EntryRevisions: true})
		builder.SetPageID(r.PageID)
		if e := builder.AddLeafEntryWithRevision(entries[slot].Key, nil, node.FlagTombstone, page.ValuePtr{}, 2); e != nil {
			t.Fatal(e)
		}
		builder.FinishNoNode()
		if _, e := a.SealComponent(r, image, nil); e != nil {
			t.Fatal(e)
		}
		entry := entries[slot]
		entry.Operand = node.PrimaryOperand{Ref: page.PageChildRef(r.PageID), Digest: sha256.Sum256(image)}
		entry.Revision = 2
		entry.Kind = node.PrimaryAbsence
		return r, entry
	}
	secondComponent, secondEntry := makeAbsence(14)
	second := testClaim(t, a, Directory)
	secondWork := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	if ok, e := a.SealExactReplacement(second, destination, secondEntry, 1, scratch, secondWork); !ok || e != nil {
		t.Fatalf("second override %v work=%+v", e, secondWork)
	}
	if _, e = a.Drop(secondComponent, nil); e != nil {
		t.Fatal(e)
	}
	thirdComponent, thirdEntry := makeAbsence(13)
	third := testClaim(t, a, Directory)
	if _, e = a.SealExactReplacement(third, second, thirdEntry, 1, scratch, nil); e != ErrNeedsConsolidation {
		t.Fatalf("unbounded third override: %v", e)
	}
	beforeImage, e := a.Get(second.PageID)
	if e != nil {
		t.Fatal(e)
	}
	beforeDigest := sha256.Sum256(beforeImage)
	// The independently sealed slot and old cut share this bank's exact physical
	// root. Fresh flatten only changes equivalent outgoing runtime custody.
	if _, e = a.Acquire(second, nil); e != nil {
		t.Fatal(e)
	}
	flatWork := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	flatten, ok, e := a.PrepareFlattenCurrentGroup(second, 13, flatWork)
	if !ok || e != nil {
		t.Fatalf("prepare flatten %t %v", ok, e)
	}
	done := false
	turns := 0
	for ; turns < 16; turns++ {
		flatWork = &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		var progress bool
		done, progress, e = flatten.Step(second, flatWork)
		if e != nil || !progress {
			t.Fatalf("standalone flatten %v %t %+v", e, progress, flatWork)
		}
		if done {
			break
		}
	}
	if !done {
		t.Fatal("flatten did not progress without writers")
	}
	if sha256.Sum256(beforeImage) != beforeDigest {
		t.Fatal("flatten changed independent physical directory")
	}
	thirdWork := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	if ok, e := a.SealExactReplacement(third, second, thirdEntry, 1, scratch, thirdWork); !ok || e != nil {
		t.Fatalf("post-flatten replacement %v work=%+v", e, thirdWork)
	}
	if _, e = a.Drop(thirdComponent, nil); e != nil {
		t.Fatal(e)
	}
	if entry, e := a.valid(second).directory.ClassEntry(13); e != nil || entry.Kind != node.PrimaryPut {
		t.Fatal("old slot/cut changed")
	}
	for _, r := range []Ref{old, destination, second, second, third} {
		if _, e = a.Drop(r, nil); e != nil {
			t.Fatal(e)
		}
	}
	testDrain(t, a)
	if a.Counters().GroupsCreated != a.Counters().GroupsFreed {
		t.Fatalf("custody groups leaked: %+v", a.Counters())
	}
	for _, r := range components {
		if a.valid(r) != nil {
			t.Fatalf("obsolete component retained %d", r.PageID)
		}
	}
	t.Logf("isolated second=%dR flatten=%dR third=%dR; complete publication remains unqualified", secondWork.Records, flatWork.Records, thirdWork.Records)
}

func TestPrimaryArenaCompanionRoutingKeepsDataExtentIndependent(t *testing.T) {
	a, e := Open(filepath.Join(t.TempDir(), "index.db.primary"))
	if e != nil {
		t.Fatal(e)
	}
	defer a.Close()
	p, e := pager.Open(filepath.Join(t.TempDir(), "index.db"), 256*page.PageSize)
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	if e = p.GrowTo(4); e != nil {
		t.Fatal(e)
	}
	p.SetPageCount(4)
	if e = p.AttachPrimaryBankPager(a.Pager()); e != nil {
		t.Fatal(e)
	}
	p.SetVerifyOnRead(true)
	component := testComponent(t, a, "physical", 2)
	image, e := p.Get(component.PageID)
	if e != nil || page.DecodeHeader(image).PageID != component.PageID {
		t.Fatalf("routed image %v", e)
	}
	if p.PageCount() != 4 {
		t.Fatalf("bank extent leaked into DATA: %d", p.PageCount())
	}
	w := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	if _, ok, e := p.GetWithWork(component.PageID, w); !ok || e != nil {
		t.Fatalf("routed work %v %t", e, ok)
	}
	p.MarkVerified(component.PageID)
	if !p.IsVerified(component.PageID) {
		t.Fatal("verified bank did not route")
	}
	p.MarkUnverified(component.PageID)
	if p.IsVerified(component.PageID) {
		t.Fatal("unverified bank did not route")
	}
}
