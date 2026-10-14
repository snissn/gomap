package valuelog

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"os"
	"path/filepath"
	"testing"
)

func liveRetirementFixtureV6(t *testing.T) (*Manager, *Set, *File, string) {
	t.Helper()
	dir := t.TempDir()
	id, path := writeIdentityPinTestSegment(t, dir)
	m, e := NewManagerWithStableResourcePinRegistry(dir, rootpublication.NewIdentityPinRegistry())
	if e != nil {
		t.Fatal(e)
	}
	set := m.CurrentSubsetNoRefresh(map[uint32]struct{}{id: {}})
	f := set.Files[id]
	if e = m.MarkZombie(id); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if e := m.Close(); e != nil {
			t.Errorf("physical shutdown %v", e)
		}
	})
	return m, set, f, path
}
func liveWorkV6(n uint64) *iterator.OrdinalScanWork {
	return &iterator.OrdinalScanWork{RecordLimit: n, ByteLimit: 1 << 20}
}
func liveCursorV6(t *testing.T, m *Manager, s *Set) *TerminalSetRetirementV5 {
	t.Helper()
	c, ready, e := m.PrepareLiveSetRetirementV6(s, liveWorkV6(32))
	if e != nil || !ready {
		t.Fatalf("prepare %v %v", ready, e)
	}
	return c
}
func finishLiveV6(t *testing.T, c *TerminalSetRetirementV5) {
	t.Helper()
	var records, bytes uint64
	for turn := 0; turn < 128; turn++ {
		w := liveWorkV6(16)
		done, p, e := c.Step(w)
		records += w.Records
		bytes += w.Bytes
		if e != nil {
			t.Fatal(e)
		}
		if done {
			t.Logf("actual finite retirement %dR/%dB in %d turns", records, bytes, turn+1)
			return
		}
		if !p {
			t.Fatalf("no progress at file phase %d namespace %v", c.filePhase, c.namespace)
		}
	}
	t.Fatal("finite retirement did not finish")
}
func TestLiveSetRetirementV6PinnedDebtAndNamespaceAdmission(t *testing.T) {
	m, set, f, path := liveRetirementFixtureV6(t)
	token := stableManagerToken(t, m, f.ID)
	c := liveCursorV6(t, m, set)
	done, _, e := c.Step(liveWorkV6(32))
	if done || !errors.Is(e, ErrFilePinned) || set.RefCount.Load() != 0 || f.RefCount.Load() != 0 || f.ownedRetirementV6.Load() != c || f.retryDeletePending.Load() {
		t.Fatalf("exact pinned debt %v %v", done, e)
	}
	if _, e = os.Stat(path); e != nil {
		t.Fatal(e)
	}
	if e = m.RemoveSegmentForce(f.ID); !errors.Is(e, ErrLiveRetirementPhysicalV6) {
		t.Fatalf("foreign force %v", e)
	}
	if e = f.Close(); !errors.Is(e, ErrLiveRetirementPhysicalV6) {
		t.Fatalf("foreign close %v", e)
	}
	if e = m.Close(); !errors.Is(e, ErrLiveRetirementPhysicalV6) || m.closeDone != nil {
		t.Fatalf("early teardown %v", e)
	}
	extra := t.TempDir()
	if e = m.AddScanDir(extra); e != nil || len(m.extraScanDirs) != 1 {
		t.Fatalf("ordinary foreign parent configuration %v", e)
	}
	id, _ := EncodeFileID(1, 2)
	newPath := SegmentPath(m.dir, id)
	writer, e := NewWriter(newPath, id)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = writer.Append(0, nil, 2, []byte("independent writer")); e != nil {
		t.Fatal(e)
	}
	if e = writer.Close(); e != nil {
		t.Fatal(e)
	}
	if e = m.RegisterSegment(newPath, id); e != nil {
		t.Fatal("same parent rotation", e)
	}
	unrelated := m.CurrentSubsetNoRefresh(map[uint32]struct{}{id: {}})
	if len(unrelated.Files) != 1 {
		t.Fatal("unrelated capture")
	}
	token.Release()
	finishLiveV6(t, c)
	if _, e = os.Stat(path); !os.IsNotExist(e) {
		t.Fatalf("old file retained %v", e)
	}
	if _, e = os.Stat(newPath); e != nil {
		t.Fatal("unrelated file deleted", e)
	}
	if f.RefCount.Load() != 0 || m.files[f.ID] != nil || f.ownedRetirementV6.Load() != nil || m.ownedSetRetirementsV6 != 0 || !f.closed.Load() {
		t.Fatal("ownership not finished")
	}
	w := liveWorkV6(16)
	if done, _, e = c.Step(w); !done || e != nil {
		t.Fatalf("once completion %v %v", done, e)
	}
	if e = m.Release(unrelated); e != nil {
		t.Fatal(e)
	}
}
func TestLiveSetRetirementV6TerminalTakesPinnedExactCursor(t *testing.T) {
	m, set, f, path := liveRetirementFixtureV6(t)
	token := stableManagerToken(t, m, f.ID)
	defer token.Release()
	c := liveCursorV6(t, m, set)
	if _, _, e := c.Step(liveWorkV6(32)); !errors.Is(e, ErrFilePinned) {
		t.Fatal(e)
	}
	if ready, e := m.BeginTerminalRetirementV5(liveWorkV6(32)); !ready || e != nil {
		t.Fatal(e)
	}
	finishLiveV6(t, c)
	if c.live || f.RefCount.Load() != 0 || set.RefCount.Load() != 0 || f.ownedRetirementV6.Load() != nil || f.closed.Load() {
		t.Fatal("terminal lost exact Manager custody")
	}
	if e := m.Close(); e != nil {
		t.Fatal(e)
	}
	if _, e := os.Stat(path); e != nil {
		t.Fatal("terminal removed pinned persistent path", e)
	}
}
func TestLiveSetRetirementV6NonLastSetAndAdditionalParent(t *testing.T) {
	m, set, f, path := liveRetirementFixtureV6(t)
	set.RefCount.Add(1)
	c := liveCursorV6(t, m, set)
	finishLiveV6(t, c)
	if set.RefCount.Load() != 1 || f.RefCount.Load() != 1 || f.closed.Load() {
		t.Fatal("nonlast owner consumed physical edge")
	}
	extra := t.TempDir()
	id, _ := EncodeFileID(1, 2)
	writer, e := NewWriter(SegmentPath(extra, id), id)
	if e != nil {
		t.Fatal(e)
	}
	if e = writer.Close(); e != nil {
		t.Fatal(e)
	}
	if e = m.AddScanDir(extra); e != nil {
		t.Fatal(e)
	}
	c = liveCursorV6(t, m, set)
	finishLiveV6(t, c)
	if _, e = os.Stat(path); !os.IsNotExist(e) {
		t.Fatal("old zombie retained", e)
	}
	if _, e = os.Stat(SegmentPath(extra, id)); e != nil {
		t.Fatal("foreign parent harmed", e)
	}

}
func TestLiveSetRetirementV6PostUnlinkFailureKeepsExactCustody(t *testing.T) {
	m, set, f, path := liveRetirementFixtureV6(t)
	c := liveCursorV6(t, m, set)
	injected := errors.New("owned namespace cut")
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Namespace == durabilitycut.NamespaceUnlink && event.OldPath == path {
			return injected
		}
		return nil
	})
	var got error
	for turn := 0; turn < 128; turn++ {
		done, _, e := c.Step(liveWorkV6(16))
		if e != nil {
			got = e
			break
		}
		if done {
			t.Fatal("injection did not run")
		}
	}
	restore()
	if !errors.Is(got, injected) || c.namespace == nil || c.namespace.phase != 8 || f.ownedRetirementV6.Load() != c || set.RefCount.Load() != 0 || f.RefCount.Load() != 0 {
		t.Fatalf("uncertain exact owner %v", got)
	}
	if _, e := os.Stat(c.namespace.quarantinePath); e != nil {
		t.Fatal("quarantine lost", e)
	}
	if e := m.Close(); !errors.Is(e, ErrLiveRetirementPhysicalV6) {
		t.Fatal("uncertain teardown", e)
	}
	replacement := []byte("replacement must survive")
	if e := os.WriteFile(path, replacement, 0600); e != nil {
		t.Fatal(e)
	}
	finishLiveV6(t, c)
	actual, e := os.ReadFile(path)
	if e != nil || string(actual) != string(replacement) {
		t.Fatalf("replacement removed %q %v", actual, e)
	}
	matches, e := filepath.Glob(filepath.Join(m.dir, ".treedb-delete-*"))
	if e != nil || len(matches) != 0 {
		t.Fatalf("intent retained %v %v", matches, e)
	}
}

func TestLiveSetRetirementV6MultipleParentsForeignWriterAndSecondFailure(t *testing.T) {
	dirs := []string{t.TempDir(), t.TempDir()}
	ids := []uint32{writeTestSegment(t, dirs[0], 0, 1, 1, []byte("value")), writeTestSegment(t, dirs[1], 1, 1, 1, []byte("leaf"))}
	m, e := NewManagerWithStableResourcePinRegistry(dirs[0], rootpublication.NewIdentityPinRegistry())
	if e != nil {
		t.Fatal(e)
	}
	defer m.Close()
	if e = m.AddScanDir(dirs[1]); e != nil {
		t.Fatal(e)
	}
	set := m.CurrentSetNoRefresh()
	if len(set.Files) != 2 {
		t.Fatal("actual two-parent Set missing")
	}
	for _, id := range ids {
		if e = m.MarkZombie(id); e != nil {
			t.Fatal(e)
		}
	}
	if m.retirementParents.count != 2 {
		t.Fatal("actual physical parents coalesced")
	}
	first := set.retirementFiles[0]
	second := set.retirementFiles[1]
	pin := stableManagerToken(t, m, first.ID)
	c := liveCursorV6(t, m, set)
	if _, _, e = c.Step(liveWorkV6(32)); !errors.Is(e, ErrFilePinned) || c.pending != first || c.parentBorrow == nil {
		t.Fatal("exact first-parent pin debt", e)
	}
	// Ordinary registration in a genuinely third physical parent remains legal.
	foreign := t.TempDir()
	foreignID := writeTestSegment(t, foreign, 2, 1, 1, []byte("future ordinary materialization"))
	if e = m.AddScanDir(foreign); e != nil {
		t.Fatal("foreign writer refused", e)
	}
	held := m.CurrentSubsetNoRefresh(map[uint32]struct{}{foreignID: {}})
	defer m.Release(held)
	pin.Release()
	injected := errors.New("second physical parent unlink")
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Namespace == durabilitycut.NamespaceUnlink && event.OldPath == second.Path {
			return injected
		}
		return nil
	})
	for turn := 0; turn < 128; turn++ {
		done, _, err := c.Step(liveWorkV6(16))
		if err != nil {
			e = err
			break
		}
		if done {
			t.Fatal("second-parent failure was not exercised")
		}
	}
	restore()
	if !errors.Is(e, injected) || c.pending != second || c.parentBorrow == nil || c.parent != second.retirementParent.file || c.namespace == nil || c.namespace.phase != 8 {
		t.Fatal("second exact physical custody", e)
	}
	if _, e = os.Stat(first.Path); !os.IsNotExist(e) {
		t.Fatal("first parent did not finish", e)
	}
	if e = m.Close(); !errors.Is(e, ErrLiveRetirementPhysicalV6) {
		t.Fatal("partial second-parent teardown", e)
	}
	replacement := []byte("replacement of second canonical child")
	if e = os.WriteFile(second.Path, replacement, 0600); e != nil {
		t.Fatal(e)
	}
	finishLiveV6(t, c)
	if actual, err := os.ReadFile(second.Path); err != nil || string(actual) != string(replacement) {
		t.Fatal("replacement lost", err)
	}
	if _, e = os.Stat(SegmentPath(foreign, foreignID)); e != nil {
		t.Fatal("future writer lost", e)
	}
	if m.retirementParents.count != 0 || m.retirementParents.heads != [retirementParentBuckets]*retirementParentHandle{} || m.ownedSetRetirementsV6 != 0 {
		t.Fatal("parent-pool history survived charged completion")
	}
}
