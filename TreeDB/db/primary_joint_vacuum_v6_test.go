package db

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestPrimaryJointVacuumV6SuccessfulReplacement(t *testing.T) {
	d, e := Open(Options{Dir: t.TempDir(), ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	if e = d.SetSync([]byte("first"), []byte("one")); e != nil {
		t.Fatal(e)
	}
	held := d.AcquireSnapshot()
	defer held.Close()
	if e = d.SetSync([]byte("second"), []byte("two")); e != nil {
		t.Fatal(e)
	}
	old := d.idx.Load()
	if e = d.VacuumIndexOnline(context.Background()); e != nil {
		t.Fatal(e)
	}
	current := d.idx.Load()
	if current == old || current.primaryOwner == old.primaryOwner || current.primary.UUID() == old.primary.UUID() {
		t.Fatal("joint replacement retained old pair")
	}
	if v, e := held.Get([]byte("first")); e != nil || !bytes.Equal(v, []byte("one")) {
		t.Fatalf("old reader %q %v", v, e)
	}
	if v, e := d.Get([]byte("second")); e != nil || !bytes.Equal(v, []byte("two")) {
		t.Fatalf("new reader %q %v", v, e)
	}
}

func TestPrimaryJointVacuumV6PointersAndIndependentFallback(t *testing.T) {
	for _, directory := range []bool{false, true} {
		t.Run(fmt.Sprint(directory), func(t *testing.T) {
			dir := t.TempDir()
			if directory {
				if e := SaveFormatConfig(dir, FormatConfig{DurabilityProfile: ProfileNoWALFast, RequiredFeatures: []string{RequiredFeatureDependencyDirectoryV2}}); e != nil {
					t.Fatal(e)
				}
			}
			opts := Options{Dir: dir, ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true}
			d, e := Open(opts)
			if e != nil {
				t.Fatal(e)
			}
			defer func() {
				if d != nil {
					d.Close()
				}
			}()
			values := [][]byte{bytes.Repeat([]byte("first"), 100), bytes.Repeat([]byte("second"), 100)}
			ptrs := appendPointersInNewSegment(t, dir, 0, 1, 10000, 2, func(i int) []byte { return values[i] })
			if e = d.RefreshValueLogSet(); e != nil {
				t.Fatal(e)
			}
			put := func(key string, ptr page.ValuePtr) {
				t.Helper()
				b := d.NewBatch().(*Batch)
				defer b.Close()
				if e := b.SetPointer([]byte(key), ptr); e != nil {
					t.Fatal(e)
				}
				if e := b.WriteSync(); e != nil {
					t.Fatal(e)
				}
			}
			put("first", ptrs[0])
			held := d.AcquireSnapshot()
			defer held.Close()
			put("second", ptrs[1])
			old := d.idx.Load()
			cut, e := d.CapturePhysicalSnapshotCutV1(context.Background())
			if e != nil {
				t.Fatal(e)
			}
			if e = d.VacuumIndexOnline(context.Background()); !errors.Is(e, rootpublication.ErrResourcePinned) {
				t.Fatalf("cut refusal %v", e)
			}
			if d.idx.Load() != old {
				t.Fatal("cut refusal mutated pair")
			}
			cut.Close()
			before := d.durableRoot.slotCommit
			latest := d.durableRoot.slot
			if e = d.VacuumIndexOnline(context.Background()); e != nil {
				t.Fatal(e)
			}
			want := before
			want[latest]++
			if d.durableRoot.slotCommit != want {
				t.Fatalf("commits %v want %v", d.durableRoot.slotCommit, want)
			}
			if d.idx.Load().primaryOwner == old.primaryOwner {
				t.Fatal("V6 shared old companion")
			}
			if got, e := held.Get([]byte("first")); e != nil || !bytes.Equal(got, values[0]) {
				t.Fatalf("held pointer %v", e)
			}
			if _, e := held.Get([]byte("second")); !errors.Is(e, tree.ErrKeyNotFound) {
				t.Fatalf("held newer %v", e)
			}
			held.Close()
			if e = d.Close(); e != nil {
				t.Fatal(e)
			}
			d = nil
			path := filepath.Join(dir, primaryIndexFileName)
			for damaged := 0; damaged < 2; damaged++ {
				f, e := os.OpenFile(path, os.O_RDWR, 0)
				if e != nil {
					t.Fatal(e)
				}
				image := make([]byte, 3*page.PageSize)
				offset := int64(2+damaged*3) * page.PageSize
				if _, e = f.ReadAt(image, offset); e != nil {
					t.Fatal(e)
				}
				bad := append([]byte(nil), image...)
				bad[48] ^= 1
				if _, e = f.WriteAt(bad, offset); e != nil {
					t.Fatal(e)
				}
				if e = f.Sync(); e != nil {
					t.Fatal(e)
				}
				f.Close()
				ro := opts
				ro.ReadOnly = true
				r, e := Open(ro)
				if e != nil {
					t.Fatal(e)
				}
				if r.State().CommitSeq != want[damaged^1] {
					t.Fatalf("wrong fallback %d", r.State().CommitSeq)
				}
				got, e := r.Get([]byte("first"))
				if e != nil || !bytes.Equal(got, values[0]) {
					t.Fatalf("fallback pointer %v", e)
				}
				got, e = r.Get([]byte("second"))
				if e != nil || (damaged == int(latest) && got != nil) || (damaged != int(latest) && !bytes.Equal(got, values[1])) {
					t.Fatalf("fallback second %q %v", got, e)
				}
				r.Close()
				f, e = os.OpenFile(path, os.O_RDWR, 0)
				if e != nil {
					t.Fatal(e)
				}
				f.WriteAt(image, offset)
				f.Sync()
				f.Close()
			}
			d, e = Open(opts)
			if e != nil {
				t.Fatal(e)
			}
			if e = d.SetSync([]byte("after"), []byte("reuse")); e != nil {
				t.Fatal(e)
			}
		})
	}
}

func TestPrimaryJointVacuumV6PostCommitRecoveryAndCustody(t *testing.T) {
	for _, phase := range []string{"after-commit", "before-data-rename", "after-data-rename", "before-data-barrier", "after-data-barrier", "before-primary-rename", "after-primary-rename", "before-primary-barrier", "after-primary-barrier", "before-decision-delete", "after-decision-delete", "before-deletion-barrier", "after-deletion-barrier"} {
		t.Run(phase, func(t *testing.T) {
			dir := t.TempDir()
			opts := Options{Dir: dir, ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true}
			d, e := Open(opts)
			if e != nil {
				t.Fatal(e)
			}
			if e = d.SetSync([]byte("first"), []byte("one")); e != nil {
				t.Fatal(e)
			}
			if e = d.SetSync([]byte("second"), []byte("two")); e != nil {
				t.Fatal(e)
			}
			prior := d.State().CommitSeq
			injected := errors.New("joint cut")
			testHookPrimaryJointSwapV6 = func(p string) error {
				if p == phase {
					return injected
				}
				return nil
			}
			e = d.VacuumIndexOnline(context.Background())
			testHookPrimaryJointSwapV6 = nil
			if !errors.Is(e, injected) || !errors.Is(e, ErrRecoveryRequired) {
				t.Fatalf("cut %v", e)
			}
			if e = d.SetSync([]byte("blocked"), []byte("write")); !errors.Is(e, ErrRecoveryRequired) {
				t.Fatalf("writer %v", e)
			}
			d.idxMu.Lock()
			generations := len(d.idxAll)
			d.idxMu.Unlock()
			if generations != 2 {
				t.Fatalf("lost failure custody: %d", generations)
			}
			d.idxMu.Lock()
			var retained *indexGen
			for _, gen := range d.idxAll {
				if gen.unpublishedPrimary != nil {
					retained = gen
				}
			}
			d.idxMu.Unlock()
			if retained == nil || retained.unpublishedSelection == nil || retained.primaryOwner == d.idx.Load().primaryOwner {
				t.Fatal("missing independently retained unpublished pair/closures")
			}
			if _, e = retained.primary.Get(retained.unpublishedPrimary.current.ref.PageID); e != nil {
				t.Fatalf("private root custody: %v", e)
			}
			e = retained.pager.WithStableResourceFile(func(data *os.File) error {
				return retained.primary.Pager().WithStableResourceFile(func(primary *os.File) error {
					if _, e := data.Stat(); e != nil {
						return e
					}
					if _, e := primary.Stat(); e != nil {
						return e
					}
					parent, e := rootpublication.OpenStableParent(dir)
					if e != nil {
						return e
					}
					defer parent.Close()
					decision, e := primaryJointDecisionForV6(parent, retained, *retained.unpublishedSelection)
					if e != nil {
						return e
					}
					return validateJointPairV6(data, primary, decision)
				})
			})
			if e != nil {
				t.Fatalf("retained physical closure: %v", e)
			}

			if _, e = os.Stat(filepath.Join(dir, indexReadyFileName)); e == nil {
				ro := opts
				ro.ReadOnly = true
				// Separate direct read-only admission is checked after Close releases LOCK.
				d.Close()
				d = nil
				r, e := Open(ro)
				if r != nil {
					r.Close()
				}
				if !errors.Is(e, ErrRecoveryRequired) {
					t.Fatalf("read-only repaired %v", e)
				}
			} else {
				d.Close()
				d = nil
			}
			r, e := Open(opts)
			if e != nil {
				t.Fatal(e)
			}
			defer r.Close()
			if r.State().CommitSeq != prior+1 {
				t.Fatalf("did not roll forward %d want %d", r.State().CommitSeq, prior+1)
			}
			if v, e := r.Get([]byte("second")); e != nil || !bytes.Equal(v, []byte("two")) {
				t.Fatalf("pair view %q %v", v, e)
			}
			if _, e = os.Stat(filepath.Join(dir, indexReadyFileName)); !os.IsNotExist(e) {
				t.Fatalf("decision survived success %v", e)
			}
			if e = r.SetSync([]byte("after"), []byte("write")); e != nil {
				t.Fatal(e)
			}
		})
	}
}

// The child deliberately exits without DB.Close: no cleanup or clean seal can
// supply the recovery ordering being tested.
func TestPrimaryJointCrashProcessV6(t *testing.T) {
	dir := os.Getenv("TREEDB_JOINT_CRASH_DIR")
	if dir == "" {
		t.Skip("subprocess entry")
	}
	phase, mode := os.Getenv("TREEDB_JOINT_CRASH_PHASE"), os.Getenv("TREEDB_JOINT_CRASH_MODE")
	testHookPrimaryJointSwapV6 = func(p string) error {
		if p == phase {
			os.Exit(81)
		}
		return nil
	}
	d, e := Open(Options{Dir: dir, ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if e != nil {
		t.Fatal(e)
	}
	if mode == "vacuum" {
		e = d.VacuumIndexOnline(context.Background())
		if e != nil {
			t.Fatal(e)
		}
	}
	t.Fatalf("phase %s was not reached", phase)
}
func runPrimaryJointCrashV6(t *testing.T, dir, mode, phase string) {
	t.Helper()
	exe, e := os.Executable()
	if e != nil {
		t.Fatal(e)
	}
	command := exec.Command(exe, "-test.run=^TestPrimaryJointCrashProcessV6$", "-test.timeout=20s")
	command.Env = append(os.Environ(), "TREEDB_JOINT_CRASH_DIR="+dir, "TREEDB_JOINT_CRASH_MODE="+mode, "TREEDB_JOINT_CRASH_PHASE="+phase)
	out, e := command.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(e, &exit) || exit.ExitCode() != 81 {
		t.Fatalf("child %s/%s: %v\n%s", mode, phase, e, out)
	}
}
func preparePrimaryJointCrashV6(t *testing.T, dir string) uint64 {
	t.Helper()
	d, e := Open(Options{Dir: dir, ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if e != nil {
		t.Fatal(e)
	}
	for _, k := range []string{"first", "second"} {
		if e = d.SetSync([]byte(k), []byte(k)); e != nil {
			t.Fatal(e)
		}
	}
	seq := d.State().CommitSeq
	if e = d.Close(); e != nil {
		t.Fatal(e)
	}
	return seq
}
func assertPrimaryJointRecoveredV6(t *testing.T, dir string, want uint64) {
	t.Helper()
	d, e := Open(Options{Dir: dir, ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	if d.State().CommitSeq != want {
		t.Fatalf("sequence %d want %d", d.State().CommitSeq, want)
	}
	for _, k := range []string{"first", "second"} {
		if v, e := d.Get([]byte(k)); e != nil || !bytes.Equal(v, []byte(k)) {
			t.Fatalf("%s: %q %v", k, v, e)
		}
	}
	for _, name := range []string{indexReadyFileName, indexNewFileName, primaryNewFileName} {
		if _, e = os.Stat(filepath.Join(dir, name)); !os.IsNotExist(e) {
			t.Fatalf("left %s: %v", name, e)
		}
	}
	if e = d.SetSync([]byte("after-recovery"), []byte("write")); e != nil {
		t.Fatal(e)
	}
}
func TestPrimaryJointVacuumV6UncleanPublicationAndRecovery(t *testing.T) {
	phases := []string{"before-staging-barrier", "after-staging-barrier", "before-commit", "before-commit-barrier", "after-commit-barrier", "after-commit", "before-data-rename", "after-data-rename", "before-data-barrier", "after-data-barrier", "before-primary-rename", "after-primary-rename", "before-primary-barrier", "after-primary-barrier", "before-decision-delete", "after-decision-delete", "before-deletion-barrier", "after-deletion-barrier"}
	for _, phase := range phases {
		t.Run("publication/"+phase, func(t *testing.T) {
			dir := t.TempDir()
			prior := preparePrimaryJointCrashV6(t, dir)
			runPrimaryJointCrashV6(t, dir, "vacuum", phase)
			want := prior + 1
			if phase == "before-commit" || phase == "before-staging-barrier" || phase == "after-staging-barrier" {
				want = prior
			}
			assertPrimaryJointRecoveredV6(t, dir, want)
		})
	}
	for _, phase := range phases[6:] {
		t.Run("recovery/"+phase, func(t *testing.T) {
			dir := t.TempDir()
			prior := preparePrimaryJointCrashV6(t, dir)
			runPrimaryJointCrashV6(t, dir, "vacuum", "after-commit")
			runPrimaryJointCrashV6(t, dir, "recovery", phase)
			assertPrimaryJointRecoveredV6(t, dir, prior+1)
		})
	}
}
func TestPrimaryJointVacuumV6MalformedDecisionDoesNotCleanPair(t *testing.T) {
	for _, kind := range []string{"truncated", "unknown", "checksum", "version", "missing-identity"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			preparePrimaryJointCrashV6(t, dir)
			runPrimaryJointCrashV6(t, dir, "vacuum", "after-commit")
			path := filepath.Join(dir, indexReadyFileName)
			image, e := os.ReadFile(path)
			if e != nil {
				t.Fatal(e)
			}
			switch kind {
			case "truncated":
				image = image[:12]
			case "unknown":
				copy(image[:8], "UNKNOWN!")
			case "checksum":
				image[len(image)-1] ^= 1
			default:
				decision, e := decodePrimaryJointDecisionV6(image)
				if e != nil {
					t.Fatal(e)
				}
				if kind == "version" {
					decision.Version = 100
				} else {
					decision.Data.ObjectID[0] ^= 1
				}
				image, e = encodePrimaryJointDecisionV6(decision)
				if e != nil {
					t.Fatal(e)
				}
			}
			if e = os.WriteFile(path, image, 0600); e != nil {
				t.Fatal(e)
			}
			before := make(map[string][32]byte)
			for _, name := range []string{indexFileName, primaryIndexFileName, indexNewFileName, primaryNewFileName, indexReadyFileName} {
				b, e := os.ReadFile(filepath.Join(dir, name))
				if e != nil {
					t.Fatal(e)
				}
				before[name] = sha256.Sum256(b)
			}
			opts := Options{Dir: dir, ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true}
			for _, ro := range []bool{true, false} {
				opts.ReadOnly = ro
				d, e := Open(opts)
				if d != nil {
					d.Close()
				}
				if !errors.Is(e, ErrRecoveryRequired) {
					t.Fatalf("open ro=%v: %v", ro, e)
				}
			}
			opts.ReadOnly = false
			if e = VacuumIndexOffline(opts); !errors.Is(e, ErrRecoveryRequired) {
				t.Fatalf("offline: %v", e)
			}
			for name, want := range before {
				b, e := os.ReadFile(filepath.Join(dir, name))
				if e != nil || sha256.Sum256(b) != want {
					t.Fatalf("mutated %s: %v", name, e)
				}
			}
		})
	}
}

func TestPrimaryJointVacuumV6OfflineUsesPairAuthority(t *testing.T) {
	dir := t.TempDir()
	prior := preparePrimaryJointCrashV6(t, dir)
	opts := Options{Dir: dir, ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true}
	if e := VacuumIndexOffline(opts); e != nil {
		t.Fatal(e)
	}
	assertPrimaryJointRecoveredV6(t, dir, prior+1)
}

func TestPrimaryJointVacuumV6CancellationAfterCommitRetainsDecision(t *testing.T) {
	dir := t.TempDir()
	preparePrimaryJointCrashV6(t, dir)
	d, e := Open(Options{Dir: dir, ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if e != nil {
		t.Fatal(e)
	}
	prior := d.State().CommitSeq
	ctx, cancel := context.WithCancel(context.Background())
	testHookPrimaryJointSwapV6 = func(p string) error {
		if p == "after-commit" {
			cancel()
		}
		return nil
	}
	e = d.VacuumIndexOnline(ctx)
	testHookPrimaryJointSwapV6 = nil
	if !errors.Is(e, context.Canceled) || !errors.Is(e, ErrRecoveryRequired) {
		t.Fatalf("cancel: %v", e)
	}
	if _, e = os.Stat(filepath.Join(dir, indexReadyFileName)); e != nil {
		t.Fatalf("lost decision: %v", e)
	}
	if e = d.SetSync([]byte("blocked"), []byte("write")); !errors.Is(e, ErrRecoveryRequired) {
		t.Fatalf("write: %v", e)
	}
	d.Close()
	assertPrimaryJointRecoveredV6(t, dir, prior+1)
}

func TestPrimaryJointVacuumV6CommandFrontierAndRevisions(t *testing.T) {
	opts := Options{Dir: t.TempDir(), CommandWAL: true, IndexPrimaryDirectory: true, DisableBackgroundPrune: true}
	d, e := Open(opts)
	if e != nil {
		t.Fatal(e)
	}
	defer func() {
		if d != nil {
			d.Close()
		}
	}()
	for i, k := range []string{"first", "second"} {
		b := d.NewBatch().(*Batch)
		if e = b.SetWithRevision([]byte(k), []byte(k), page.EntryRevision(100+i)); e != nil {
			t.Fatal(e)
		}
		if e = b.WriteSync(); e != nil {
			t.Fatal(e)
		}
		b.Close()
		// Command-WAL WriteSync acknowledges its durable command; Checkpoint
		// establishes the independently eligible complete index root.
		if e = d.Checkpoint(); e != nil {
			t.Fatal(e)
		}
	}
	prior := d.durableRoot.slotRecord
	priorSeq := d.State().CommitSeq
	latest := d.durableRoot.slot
	if prior[latest].AppliedCommandLSN == 0 || prior[latest].MaxEntryRevision != 101 {
		t.Fatalf("missing actual command/revision frontier: %+v, commandWAL=%v", prior[latest], d.commandWAL)
	}
	held := d.AcquireSnapshot()
	defer held.Close()
	if e = d.VacuumIndexOnline(context.Background()); e != nil {
		t.Fatal(e)
	}
	for slot, r := range d.durableRoot.slotRecord {
		if r.AppliedCommandLSN != prior[slot].AppliedCommandLSN || r.MaxEntryRevision != prior[slot].MaxEntryRevision {
			t.Fatalf("lost slot frontier: %+v vs %+v", r, prior[slot])
		}
	}
	if d.State().CommitSeq != priorSeq+1 {
		t.Fatal("wrong vacuum commit")
	}
	for _, snap := range []*Snapshot{held, d.AcquireSnapshot()} {
		for i, k := range []string{"first", "second"} {
			v, revision, e := snap.GetVersioned([]byte(k))
			if e != nil || !bytes.Equal(v, []byte(k)) || revision != page.EntryRevision(100+i) {
				t.Fatalf("version %s %d %v", k, revision, e)
			}
		}
		if snap != held {
			snap.Close()
		}
	}
	held.Close()
	if e = d.Close(); e != nil {
		t.Fatal(e)
	}
	d = nil
	d, e = Open(opts)
	if e != nil {
		t.Fatal(e)
	}
	if d.State().AppliedCommandLSN != prior[latest].AppliedCommandLSN || uint64(d.State().MaxEntryRevision) != 101 {
		t.Fatal("reopen frontier lost")
	}
	if e = d.SetSync([]byte("after"), []byte("command")); e != nil {
		t.Fatal(e)
	}
}

// COMMIT binds physical pair identity, not whole PRIMARY bytes: corruption of
// one independently eligible capsule still selects the exact other slot.
func TestPrimaryJointVacuumV6CommittedPairPreservesIndependentFallback(t *testing.T) {
	for damaged := uint64(0); damaged < 2; damaged++ {
		t.Run(fmt.Sprint(damaged), func(t *testing.T) {
			dir := t.TempDir()
			prior := preparePrimaryJointCrashV6(t, dir)
			runPrimaryJointCrashV6(t, dir, "vacuum", "after-commit")
			path := filepath.Join(dir, primaryNewFileName)
			image, e := os.ReadFile(path)
			if e != nil {
				t.Fatal(e)
			}
			var uuid [16]byte
			copy(uuid[:], image[32:48])
			other, e := rootpublication.DecodePrimaryCapsuleV6(image[(2+(damaged^1)*3)*page.PageSize:(5+(damaged^1)*3)*page.PageSize], damaged^1, uuid)
			if e != nil {
				t.Fatal(e)
			}
			image[(2+damaged*3)*page.PageSize+48] ^= 1
			if e = os.WriteFile(path, image, 0600); e != nil {
				t.Fatal(e)
			}
			d, e := Open(Options{Dir: dir, ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
			if e != nil {
				t.Fatal(e)
			}
			defer d.Close()
			want := other.Current().Record.CommitSeq
			if d.State().CommitSeq != want {
				t.Fatalf("selected %d want independent %d", d.State().CommitSeq, want)
			}
			if v, e := d.Get([]byte("first")); e != nil || string(v) != "first" {
				t.Fatalf("first %q %v", v, e)
			}
			v, e := d.Get([]byte("second"))
			if e != nil || (want == prior+1 && string(v) != "second") || (want != prior+1 && v != nil) {
				t.Fatalf("second seq%d: %q %v", want, v, e)
			}
			if _, e = os.Stat(filepath.Join(dir, indexReadyFileName)); !os.IsNotExist(e) {
				t.Fatalf("decision survived %v", e)
			}
		})
	}
}

func TestPrimaryJointVacuumV6NamespaceObserverUncertainty(t *testing.T) {
	for _, phase := range []string{"create", "data", "primary", "unlink"} {
		t.Run(phase, func(t *testing.T) {
			dir := t.TempDir()
			prior := preparePrimaryJointCrashV6(t, dir)
			opts := Options{Dir: dir, ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true}
			d, e := Open(opts)
			if e != nil {
				t.Fatal(e)
			}
			cut := errors.New("joint namespace observer uncertainty")
			injected := false
			restore := durabilitycut.Install(func(event durabilitycut.Event) error {
				matched := phase == "create" && event.Namespace == durabilitycut.NamespaceCreate && filepath.Base(event.NewPath) == indexReadyFileName ||
					phase == "data" && event.Namespace == durabilitycut.NamespaceRename && filepath.Base(event.OldPath) == indexNewFileName ||
					phase == "primary" && event.Namespace == durabilitycut.NamespaceRename && filepath.Base(event.OldPath) == primaryNewFileName ||
					phase == "unlink" && event.Namespace == durabilitycut.NamespaceUnlink && filepath.Base(event.OldPath) == indexReadyFileName
				if matched && !injected {
					injected = true
					return cut
				}
				return nil
			})
			e = d.VacuumIndexOnline(context.Background())
			restore()
			if !injected || !errors.Is(e, cut) || !errors.Is(e, ErrRecoveryRequired) {
				t.Fatalf("observer %s: %v injected=%v", phase, e, injected)
			}
			if e = d.SetSync([]byte("blocked"), []byte("write")); !errors.Is(e, ErrRecoveryRequired) {
				t.Fatalf("later write: %v", e)
			}
			d.idxMu.Lock()
			var retained *indexGen
			for _, g := range d.idxAll {
				if g.unpublishedSelection != nil {
					retained = g
				}
			}
			d.idxMu.Unlock()
			if retained == nil || retained.unpublishedPrimary == nil {
				t.Fatal("lost exact private pair custody")
			}
			if e = retained.pager.WithStableResourceFile(func(f *os.File) error { _, e := f.Stat(); return e }); e != nil {
				t.Fatal(e)
			}
			if e = retained.primary.Pager().WithStableResourceFile(func(f *os.File) error { _, e := f.Stat(); return e }); e != nil {
				t.Fatal(e)
			}
			d.Close()
			if phase == "create" {
				before := make(map[string][32]byte)
				for _, n := range []string{indexFileName, primaryIndexFileName, indexNewFileName, primaryNewFileName, indexReadyFileName} {
					b, e := os.ReadFile(filepath.Join(dir, n))
					if e != nil {
						t.Fatal(e)
					}
					before[n] = sha256.Sum256(b)
				}
				failed, e := Open(opts)
				if failed != nil {
					failed.Close()
				}
				if !errors.Is(e, ErrRecoveryRequired) {
					t.Fatalf("empty decision accepted: %v", e)
				}
				for n, want := range before {
					b, e := os.ReadFile(filepath.Join(dir, n))
					if e != nil || sha256.Sum256(b) != want {
						t.Fatalf("uncertain create mutated %s: %v", n, e)
					}
				}
			} else {
				assertPrimaryJointRecoveredV6(t, dir, prior+1)
			}
		})
	}
}

// Failure while retiring the stopped old runtime occurs after real publication,
// so custody belongs to the installed replacement and independent old readers.
func TestPrimaryJointVacuumV6OldRuntimeRetirementErrorRetainsPublishedPair(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir, ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true}
	d, e := Open(opts)
	if e != nil {
		t.Fatal(e)
	}
	defer func() {
		if d != nil {
			d.Close()
		}
	}()
	value := bytes.Repeat([]byte("persistent-old-stop"), 100)
	pointers := appendPointersInNewSegment(t, dir, 0, 1, 20000, 1, func(int) []byte { return value })
	if e = d.RefreshValueLogSet(); e != nil {
		t.Fatal(e)
	}
	b := d.NewBatch().(*Batch)
	if e = b.SetPointer([]byte("first"), pointers[0]); e != nil {
		t.Fatal(e)
	}
	if e = b.WriteSync(); e != nil {
		t.Fatal(e)
	}
	b.Close()
	held := d.AcquireSnapshot()
	defer held.Close()
	if e = d.SetSync([]byte("second"), []byte("second")); e != nil {
		t.Fatal(e)
	}
	prior, old := d.State().CommitSeq, d.idx.Load()
	cut := errors.New("old runtime retirement error")
	d.vacuumOldRootPublicationStopHook = func() error { return cut }
	e = d.VacuumIndexOnline(context.Background())
	d.vacuumOldRootPublicationStopHook = nil
	if !errors.Is(e, cut) || !errors.Is(e, ErrRecoveryRequired) {
		t.Fatalf("retirement: %v", e)
	}
	if d.State().CommitSeq != prior+1 || d.idx.Load() == old || d.idx.Load().primaryOwner == old.primaryOwner {
		t.Fatal("lost installed replacement")
	}
	if d.idx.Load().unpublishedSelection != nil || d.idx.Load().unpublishedPrimary != nil {
		t.Fatal("published custody still private")
	}
	if v, e := held.Get([]byte("first")); e != nil || !bytes.Equal(v, value) {
		t.Fatalf("old pointer: %v", e)
	}
	if e = d.SetSync([]byte("blocked"), []byte("write")); !errors.Is(e, ErrRecoveryRequired) {
		t.Fatalf("later write: %v", e)
	}
	if _, e = os.Stat(filepath.Join(dir, indexReadyFileName)); !os.IsNotExist(e) {
		t.Fatalf("decision not settled: %v", e)
	}
	held.Close()
	d.Close()
	d = nil
	d, e = Open(opts)
	if e != nil {
		t.Fatal(e)
	}
	if d.State().CommitSeq != prior+1 {
		t.Fatal("published root not recovered")
	}
	if v, e := d.Get([]byte("first")); e != nil || !bytes.Equal(v, value) {
		t.Fatalf("recovered pointer: %v", e)
	}
	if v, e := d.Get([]byte("second")); e != nil || string(v) != "second" {
		t.Fatalf("recovered second: %v", e)
	}
	if e = d.SetSync([]byte("after"), []byte("write")); e != nil {
		t.Fatal(e)
	}
}
