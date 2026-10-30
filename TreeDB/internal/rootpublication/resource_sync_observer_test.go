package rootpublication

import (
	"errors"
	"os"
	"reflect"
	"runtime"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/recoverytrace"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

func TestStableResourceFileSyncObserverExactPhysicalBoundary(t *testing.T) {
	for _, tc := range []struct {
		name              string
		covered, imported bool
		fail              string
	}{
		{name: "direct"}, {name: "imported", imported: true}, {name: "covered", covered: true},
		{name: "before-error", fail: "before"}, {name: "sync-error", fail: "sync"},
		{name: "after-error", fail: "after"}, {name: "imported-after-error", imported: true, fail: "after"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sentinel := errors.New(tc.fail)
			var order []string
			source := stableTokenFixture(t, t.TempDir(), "observed", 1, 4, ReachabilityIndexFile, "index", func(s *StableResourceSpec) {
				s.ContentSynced = tc.covered
				s.SyncThrough = func(f *os.File, _ DurableFrontier) error {
					order = append(order, "sync")
					if tc.fail == "sync" {
						return sentinel
					}
					return f.Sync()
				}
			})
			token := source
			if tc.imported {
				builder := NewStableResourceSetBuilder()
				if err := builder.Add(source); err != nil {
					t.Fatal(err)
				}
				set, err := builder.Freeze()
				if err != nil {
					t.Fatal(err)
				}
				defer set.Release()
				var root *stableResourceEntryNode
				for _, view := range set.kindViews.all() {
					root = view.root
				}
				owner := new(retainedalloc.Owner)
				owner.Initialize(0)
				token, err = newImportedResourceToken(owner, source, root, source.logicalLane, source.resourceID, source.diagnosticPath, source.frontier, source.reachability, source.logicalObligations, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer token.Release()
			} else {
				defer source.Release()
			}
			beforePhysical := source.metrics.physicalFileSyncs.Load()
			beforeImported := token.metrics.physicalFileSyncs.Load()
			observe := func(point string) func(*os.File) error {
				return func(f *os.File) error {
					// The deepest original owner supplies the same pinned descriptor to the
					// operation and both callbacks, even through an imported token.
					if f != source.pinned {
						t.Fatal("observer received a different retained handle")
					}
					order = append(order, point)
					if tc.fail == point {
						return sentinel
					}
					return nil
				}
			}
			ledger := recoverytrace.New(8)
			restoreLedger, installed := recoverytrace.Install(ledger)
			if !installed {
				t.Fatal("diagnostic ledger overlap")
			}
			defer restoreLedger()
			err := token.SyncThroughWithFileSyncObserver(observe("before"), observe("after"))
			wantRecords := uint64(2)
			if tc.covered || tc.fail == "before" {
				wantRecords = 0
			}
			if ledger.Attempted() != wantRecords || ledger.Dropped() != 0 {
				t.Fatalf("physical ledger records=%d want=%d", ledger.Attempted(), wantRecords)
			}
			if wantRecords != 0 {
				start, ok := ledger.Read(0)
				if !ok || start.Stage != recoverytrace.PhysicalSync || start.Outcome != recoverytrace.Started ||
					start.IdentityObject != source.identity.ObjectID || start.IdentityVolume != source.identity.VolumeID ||
					start.IdentityPlatform.String() != source.identity.Platform || start.IdentityGeneration != source.identity.Generation ||
					start.ResourceID.String() != source.resourceID || start.Path.String() != source.diagnosticPath || !start.HandlePresent ||
					start.Required != recoveryTraceFrontier(source.frontier) || start.Certified != recoveryTraceFrontier(source.syncedFrontier) || start.HasCertified != source.hasSyncedFrontier {
					t.Fatal("physical ledger did not capture original producer identity/frontier")
				}
				end, ok := ledger.Read(1)
				wantOutcome := recoverytrace.Returned
				if tc.fail == "sync" {
					wantOutcome = recoverytrace.ReturnedError
				}
				if !ok || end.Parent != start.Sequence || end.Outcome != wantOutcome {
					t.Fatal("physical return differed from original sync outcome")
				}
			}

			if tc.fail != "" {
				if err != sentinel {
					t.Fatalf("error=%v, want original sentinel", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			var want []string
			if !tc.covered {
				want = []string{"before"}
				if tc.fail != "before" {
					want = append(want, "sync")
					if tc.fail != "sync" {
						want = append(want, "after")
					}
				}
			}
			if !reflect.DeepEqual(order, want) {
				t.Fatalf("order=%v want=%v", order, want)
			}
			delta := uint64(0)
			if !tc.covered && tc.fail != "before" && tc.fail != "sync" {
				delta = 1
			}
			if got := source.metrics.physicalFileSyncs.Load() - beforePhysical; got != delta {
				t.Fatalf("source physical delta=%d want=%d", got, delta)
			}
			if got := token.metrics.physicalFileSyncs.Load() - beforeImported; got != delta {
				t.Fatalf("outer physical delta=%d want=%d", got, delta)
			}
		})
	}
}

func TestRecoveryLedgerPhysicalAbnormalExitHasNoReturn(t *testing.T) {
	for _, mode := range []string{"panic", "goexit"} {
		t.Run(mode, func(t *testing.T) {
			source := stableTokenFixture(t, t.TempDir(), "abnormal", 1, 4, ReachabilityIndexFile, "index", func(spec *StableResourceSpec) {
				spec.SyncThrough = func(_ *os.File, _ DurableFrontier) error {
					if mode == "panic" {
						panic("original producer panic")
					}
					runtime.Goexit()
					return nil
				}
			})
			defer source.Release()
			ledger := recoverytrace.New(4)
			restore, ok := recoverytrace.Install(ledger)
			if !ok {
				t.Fatal("diagnostic ledger overlap")
			}
			defer restore()
			done := make(chan any, 1)
			go func() {
				defer func() { done <- recover() }()
				_ = source.SyncThrough()
			}()
			caught := <-done
			if mode == "panic" && caught != "original producer panic" {
				t.Fatalf("panic changed: %v", caught)
			}
			if mode == "goexit" && caught != nil {
				t.Fatalf("Goexit became panic: %v", caught)
			}
			if ledger.Attempted() != 1 {
				t.Fatal("abnormal exit fabricated a return")
			}
			first, ready := ledger.Read(0)
			if !ready || first.Outcome != recoverytrace.Started || first.IdentityObject != source.identity.ObjectID {
				t.Fatal("missing original physical entry")
			}
		})
	}
}
