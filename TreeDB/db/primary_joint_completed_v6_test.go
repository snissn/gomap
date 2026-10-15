package db

import (
	"bytes"
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/lockfile"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"os"
	"path/filepath"
	"testing"
)

// Restore the exact original COMMIT bytes after a completed canonical pair has
// advanced through ordinary system-root publications. This models a retained
// optional decision name; it does not forge a newer generation receipt.
func prepareAdvancedCompletedJointDecisionV6(t *testing.T, dir string) (Options, uint64) {
	t.Helper()
	prepareOfflinePointerPairV6(t, dir)
	opts := Options{Dir: dir, ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true}
	injected := errors.New("retain original joint decision")
	testHookPrimaryJointSwapV6 = func(phase string) error {
		if phase == "after-commit" {
			return injected
		}
		return nil
	}
	_, err := ValueLogRewriteOffline(opts)
	testHookPrimaryJointSwapV6 = nil
	if !errors.Is(err, injected) {
		t.Fatal(err)
	}
	image, err := os.ReadFile(filepath.Join(dir, indexReadyFileName))
	if err != nil {
		t.Fatal(err)
	}
	decision, err := decodePrimaryJointDecisionV6(image)
	if err != nil {
		t.Fatal(err)
	}
	d, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if _, err = d.PublishSystemRootIterator(mustFrozenSystemMemtable(t, "system", "advance"+string(rune(97+i))).NewIterator(nil, nil)); err != nil {
			t.Fatal(err)
		}
		if err = d.Checkpoint(); err != nil {
			t.Fatal(err)
		}
	}

	matched := false
	for _, view := range d.durableRoot.primary.capsuleViews {
		if view.Current().Record.Freelist == decision.Generation {
			matched = true
		}
	}
	if matched {
		t.Fatal("fixture did not retire original decision generation from both slots")
	}
	seq := d.State().CommitSeq
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(dir, indexReadyFileName), image, 0600); err != nil {
		t.Fatal(err)
	}

	return opts, seq
}

func TestPrimaryJointCompletedV6AdvancedCanonicalPair(t *testing.T) {
	dir := t.TempDir()
	opts, seq := prepareAdvancedCompletedJointDecisionV6(t, dir)
	d, err := Open(opts)
	if err != nil {
		t.Fatalf("complete same canonical pair rejected after ordinary advance: %v", err)
	}
	if d.State().CommitSeq != seq {
		t.Fatalf("selected seq %d want %d", d.State().CommitSeq, seq)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(filepath.Join(dir, indexReadyFileName)); !os.IsNotExist(err) {
		t.Fatalf("completed decision retained: %v", err)
	}
}

func TestPrimaryJointCompletedV6PhysicalProofRefusal(t *testing.T) {
	for _, phase := range []string{"before-proof-close", "proof-pager-close", "before-dependency-close", "before-parent-close"} {
		t.Run(phase, func(t *testing.T) {
			dir := t.TempDir()
			opts, seq := prepareAdvancedCompletedJointDecisionV6(t, dir)
			var captured *primaryJointRecoveryProofV6
			calls := 0
			testHookPrimaryJointRecoveryProofV6 = func(at string, p *primaryJointRecoveryProofV6) {
				if (at != phase && !(phase == "proof-pager-close" && at == "before-proof-close")) || captured != nil {
					return
				}
				var f *os.File
				switch phase {
				case "proof-pager-close":
					if p.primaryPager == nil {
						return
					}
					captured = p
					calls++
					if err := p.primaryPager.WithStableResourceFile(func(f *os.File) error { return f.Close() }); err != nil {
						t.Fatal(err)
					}
					return
				case "before-proof-close":
					if p.primaryPager == nil {
						return
					}
					f = p.data
				case "before-dependency-close":
					f = p.dependency
				case "before-parent-close":
					if !p.decisionDeleted {
						return
					}
					f = p.parent
				}
				if f == nil {
					return
				}
				captured = p
				calls++
				if err := f.Close(); err != nil {
					t.Fatal(err)
				}
			}
			d, err := Open(opts)
			testHookPrimaryJointRecoveryProofV6 = nil
			if d != nil || err == nil || captured == nil {
				t.Fatalf("actual Close fault not reached: dbPresent=%v err=%v proofPresent=%v", d != nil, err, captured != nil)
			}
			var receipt *OpenRecoveryCleanupError
			if !errors.As(err, &receipt) || receipt.proof != captured || captured.failure == nil || captured.metadata.Bytes() < captured.charge+captured.pagerCharge {
				t.Fatalf("original failure custody missing: %v", err)
			}
			debt := captured.metadata.Bytes()
			first := receipt.Close()
			if receipt.Close() != first || captured.metadata.Bytes() != debt || calls != 1 {
				t.Fatal("repeated observation retried/refunded original failure")
			}
			l, e := lockfile.Acquire(filepath.Join(dir, "LOCK"))
			if l != nil || !errors.Is(e, lockfile.ErrLocked) {
				t.Fatalf("original LOCK escaped: %v", e)
			}
			_, e = os.Stat(filepath.Join(dir, indexReadyFileName))
			deleted := phase == "before-parent-close"
			if captured.decisionDeleted != deleted || (deleted != os.IsNotExist(e)) {
				t.Fatalf("actual disposition mismatch: deleted=%v stat=%v", captured.decisionDeleted, e)
			}
			// Test teardown simulates process shutdown. It does not invoke the fatal
			// receipt or replay any failed physical action.
			if e = captured.lock.Close(); e != nil {
				t.Fatal(e)
			}
			if deleted {
				reopened, e := Open(opts)
				if e != nil {
					t.Fatal(e)
				}
				if reopened.State().CommitSeq != seq {
					t.Fatal("canonical pair changed after authorized deletion")
				}
				if e = reopened.Close(); e != nil {
					t.Fatal(e)
				}
			}
		})
	}
}

func TestPrimaryJointRecoveryV6OriginalFailurePropagation(t *testing.T) {
	for _, caller := range []string{"Open", "ReadOnly", "Vacuum", "Rewrite"} {
		t.Run(caller, func(t *testing.T) {
			dir := t.TempDir()
			opts, _ := prepareAdvancedCompletedJointDecisionV6(t, dir)
			var captured *primaryJointRecoveryProofV6
			testHookPrimaryJointRecoveryProofV6 = func(phase string, p *primaryJointRecoveryProofV6) {
				if phase == "before-marker-close" && p.marker != nil && captured == nil {
					captured = p
					if err := p.marker.Close(); err != nil {
						t.Fatal(err)
					}
				}
			}
			var err error
			switch caller {
			case "Open":
				_, err = Open(opts)
			case "ReadOnly":
				opts.ReadOnly = true
				_, err = Open(opts)
			case "Vacuum":
				err = VacuumIndexOffline(opts)
			case "Rewrite":
				_, err = ValueLogRewriteOffline(opts)
			}
			testHookPrimaryJointRecoveryProofV6 = nil
			var receipt *OpenRecoveryCleanupError
			if !errors.As(err, &receipt) || captured == nil || receipt.proof != captured || captured.lock == nil {
				t.Fatalf("caller lost actual original custody: %v", err)
			}
			if _, err = os.Stat(filepath.Join(dir, indexReadyFileName)); err != nil {
				t.Fatalf("proof refusal changed marker: %v", err)
			}
			if err = captured.lock.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPrimaryJointCompletedV6RejectsPhysicalChange(t *testing.T) {
	for _, change := range []string{"DATA-incarnation", "PRIMARY-capsules", "missing-dependency"} {
		t.Run(change, func(t *testing.T) {
			dir := t.TempDir()
			if !rootpublication.StableRelativeNamespaceSupported() {
				prior := preparePrimaryJointCrashV6(t, dir)
				before := primaryPairImages(t, dir)
				e := VacuumIndexOffline(Options{Dir: dir, ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
				if !errors.Is(e, rootpublication.ErrNamespacePersistenceUnsupported) {
					t.Fatalf("completed-pair platform refusal: %v", e)
				}
				requirePrimaryPairImages(t, dir, before)
				requireNoPrivatePrimaryPair(t, dir)
				assertPrimaryJointRecoveredV6(t, dir, prior)
				return
			}
			opts, _ := prepareAdvancedCompletedJointDecisionV6(t, dir)
			marker, err := os.ReadFile(filepath.Join(dir, indexReadyFileName))
			if err != nil {
				t.Fatal(err)
			}
			switch change {
			case "DATA-incarnation":
				path := filepath.Join(dir, indexFileName)
				image, e := os.ReadFile(path)
				if e != nil {
					t.Fatal(e)
				}
				if e = os.WriteFile(path+".replacement", image, 0600); e != nil {
					t.Fatal(e)
				}
				if e = os.Rename(path+".replacement", path); e != nil {
					t.Fatal(e)
				}
			case "PRIMARY-capsules":
				f, e := os.OpenFile(filepath.Join(dir, primaryIndexFileName), os.O_RDWR, 0)
				if e != nil {
					t.Fatal(e)
				}
				for _, off := range []int64{2 * 4096, 5 * 4096} {
					if _, e = f.WriteAt([]byte{0xff}, off+48); e != nil {
						t.Fatal(e)
					}
				}
				if e = f.Close(); e != nil {
					t.Fatal(e)
				}
			case "missing-dependency":
				files, e := os.ReadDir(ValueLogDirPath(dir))
				if e != nil {
					t.Fatal(e)
				}
				removed := false
				for _, f := range files {
					if !f.IsDir() {
						if e = os.Remove(filepath.Join(ValueLogDirPath(dir), f.Name())); e != nil {
							t.Fatal(e)
						}
						removed = true
					}
				}
				if !removed {
					t.Fatal("fixture has no actual value-log dependency")
				}
			}
			d, err := Open(opts)
			t.Logf("physical-change refusal: %v", err)
			if d != nil || err == nil {
				if d != nil {
					_ = d.Close()
				}
				t.Fatal("changed physical closure accepted")
			}
			after, e := os.ReadFile(filepath.Join(dir, indexReadyFileName))
			if e != nil || !bytes.Equal(after, marker) {
				t.Fatalf("refusal changed exact decision: %v", e)
			}
		})
	}
}
