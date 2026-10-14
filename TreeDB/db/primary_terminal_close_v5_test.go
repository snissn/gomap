package db

import (
	"bytes"
	"context"
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"os"
	"path/filepath"
	"testing"
)

func TestPrimaryTerminalCloseRetiresPrivateSnapshotAndPreservesPointerCut(t *testing.T) {
	for _, failure := range []bool{false, true} {
		t.Run(map[bool]string{false: "complete", true: "partial-failure-retry"}[failure], func(t *testing.T) {
			dir := t.TempDir()
			opts := Options{Dir: dir, ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true, ValueLog: ValueLogOptions{PointerThreshold: 1}}
			database, e := Open(opts)
			if e != nil {
				t.Fatal(e)
			}
			defer database.Close()
			value := bytes.Repeat([]byte("persistent-pointer"), 100)
			ptr := appendPointersInNewSegment(t, dir, 0, 1, 18000, 1, func(int) []byte { return value })[0]
			if e = database.RefreshValueLogSet(); e != nil {
				t.Fatal(e)
			}
			batch := database.NewBatch().(*Batch)
			if e = batch.SetPointer([]byte("pointer"), ptr); e != nil {
				t.Fatal(e)
			}
			if e = batch.WriteSync(); e != nil {
				t.Fatal(e)
			}
			batch.Close()
			snapshot := database.AcquireSnapshot()
			if snapshot == nil || snapshot.state.ValueLogSet == nil || !snapshot.vlogPinned {
				t.Fatal("missing actual vlog owner")
			}
			if got, e := snapshot.Get([]byte("pointer")); e != nil || !bytes.Equal(got, value) {
				t.Fatalf("pointer %d %v", len(got), e)
			}
			// Give this exclusive construction capture its own genuine immutable Set so
			// the last-Set path is observable independently of DB/cut resource owners.
			isolated := snapshot.vlogManager.CurrentSubsetNoRefresh(map[uint32]struct{}{ptr.FileID: {}})
			if e = snapshot.vlogManager.Release(snapshot.state.ValueLogSet); e != nil {
				t.Fatal(e)
			}
			state := *snapshot.state
			state.ValueLogSet = isolated
			snapshot.state = &state
			snapshot.reader.vlogs = isolated
			if e = snapshot.beginRead(); e != nil {
				t.Fatal(e)
			}
			// Exclusive construction harness; no public capture/native producer claim.
			snapshot.privatePrimaryOwnerV5 = true
			lease := &primaryBaseReadLeaseV5{snapshot: snapshot, pinned: true, wrapperGeneration: snapshot.generation.Load(), root: snapshot.primaryRoot}
			custody, ready, e := claimPrimarySnapshotCustodyV5(lease, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
			if e != nil || !ready {
				t.Fatalf("claim %v %v", ready, e)
			}
			oldGen, vm, rootRuntime := snapshot.idx, snapshot.vlogManager, database.rootPublication
			ownedSet := snapshot.state.ValueLogSet
			beforeRefs := ownedSet.RefCount.Load()
			cut, e := database.CapturePhysicalSnapshotCutV1(context.Background())
			if e != nil {
				t.Fatal(e)
			}
			defer cut.Close()
			// Reentrant terminal callback must refuse instead of joining itself.
			database.terminalSnapshotHookV5 = func(string) error {
				if e := database.Close(); !errors.Is(e, errTerminalRetirementBlockedV5) {
					t.Fatalf("reentrant Close %v", e)
				}
				return nil
			}
			if failure {
				injected := false
				fault := errors.New("injected after primary before vlog")
				database.terminalSnapshotHookV5 = func(phase string) error {
					if phase == "vlog" && !injected {
						injected = true
						return fault
					}
					return nil
				}
				if e = database.Close(); !errors.Is(e, fault) {
					t.Fatalf("fault %v", e)
				}
				if !injected || database.closeTeardownComplete || database.rootPublication != rootRuntime || database.valueLogManager != vm || snapshot.idx != oldGen || snapshot.registryID == 0 || !snapshot.vlogPinned || custody.done {
					t.Fatal("failure discarded physical custody")
				}
				if got, e := vm.Read(ptr); e != nil || !bytes.Equal(got, value) {
					t.Fatalf("failure closed actual file %d %v", len(got), e)
				}
				if ownedSet.RefCount.Load() != beforeRefs {
					t.Fatal("failed phase consumed vlog owner")
				}
			}
			if e = database.Close(); e != nil {
				t.Fatal(e)
			}
			if database.primaryCustodyHead != nil || !custody.done || snapshot.db != nil || snapshot.idx != nil || snapshot.state != nil || snapshot.primaryRoot != nil || snapshot.vlogPinned || snapshot.registryID != 0 || snapshot.rootTrees != nil || custody.primaryQueue != nil || custody.vlogCursor != nil || custody.ownerDB != nil || lease.snapshot != nil || lease.root != nil || lease.generation != nil || !database.closeTeardownComplete {
				t.Fatal("typed nonleaf debt leaked")
			}
			if ownedSet.RefCount.Load() != beforeRefs-1 {
				t.Fatal("exact Set ref was not released once")
			}
			if ready, e := custody.ReleaseReadPinV5(&iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}); ready || !errors.Is(e, primaryarena.ErrStale) {
				t.Fatalf("former private owner %v %v", ready, e)
			}
			records, steps := database.terminalRetirementRecords, database.terminalRetirementSteps
			if records == 0 || steps < 2 {
				t.Fatal("terminal work not recorded")
			}
			if e = database.Close(); e != nil || database.terminalRetirementRecords != records {
				t.Fatal("Close repeated typed retirement")
			}
			destination := t.TempDir()
			for name, write := range map[string]func(*os.File) error{indexFileName: func(f *os.File) error { return cut.WriteToContext(context.Background(), f) }, primaryIndexFileName: func(f *os.File) error { return cut.WritePrimaryToContext(context.Background(), f) }} {
				f, e := os.Create(filepath.Join(destination, name))
				if e != nil {
					t.Fatal(e)
				}
				if e = write(f); e != nil {
					t.Fatal(e)
				}
				if e = f.Close(); e != nil {
					t.Fatal(e)
				}
			}
			// Cut holds actual namespace/file closure independently after owning DB.Close.
			for _, name := range []string{"format.json", "value_vlog"} {
				e = filepath.WalkDir(filepath.Join(dir, name), func(path string, d os.DirEntry, err error) error {
					if err != nil {
						return err
					}
					relative, err := filepath.Rel(dir, path)
					if err != nil {
						return err
					}
					target := filepath.Join(destination, relative)
					if d.IsDir() {
						return os.MkdirAll(target, 0700)
					}
					raw, err := os.ReadFile(path)
					if err != nil {
						return err
					}
					return os.WriteFile(target, raw, 0600)
				})
				if e != nil {
					t.Fatal(e)
				}
			}
			if e = RebindDurableRootSnapshotV1(destination); e != nil {
				t.Fatal(e)
			}
			readOpts := opts
			readOpts.Dir = destination
			readOpts.ReadOnly = true
			recovered, e := Open(readOpts)
			if e != nil {
				t.Fatal(e)
			}
			defer recovered.Close()
			if got, e := recovered.Get([]byte("pointer")); e != nil || !bytes.Equal(got, value) {
				t.Fatalf("cut after Close pointer %d %v", len(got), e)
			}
			t.Logf("typed terminal retirement %d records/%d bytes/%d calls; ordinary physical DB teardown separately follows", records, database.terminalRetirementBytes, steps)
		})
	}
}

func TestPrimaryTerminalCloseActiveReadRetainsCustodyV5(t *testing.T) {
	database, e := Open(Options{Dir: t.TempDir(), ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if e != nil {
		t.Fatal(e)
	}
	defer database.Close()
	if e = database.SetSync([]byte("held"), []byte("value")); e != nil {
		t.Fatal(e)
	}
	snapshot := database.AcquireSnapshot()
	if e = snapshot.beginRead(); e != nil {
		t.Fatal(e)
	} // exact private proof pin
	if e = snapshot.beginRead(); e != nil {
		t.Fatal(e)
	} // actual in-flight borrower
	snapshot.privatePrimaryOwnerV5 = true
	lease := &primaryBaseReadLeaseV5{snapshot: snapshot, pinned: true, wrapperGeneration: snapshot.generation.Load(), root: snapshot.primaryRoot}
	custody, ready, e := claimPrimarySnapshotCustodyV5(lease, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
	if e != nil || !ready {
		t.Fatalf("claim %v %v", ready, e)
	}
	generation, manager, runtime := snapshot.idx, snapshot.vlogManager, database.rootPublication
	if e = database.Close(); !errors.Is(e, errTerminalRetirementBlockedV5) {
		t.Fatalf("active borrower Close %v", e)
	}
	if custody.done || database.closeTeardownComplete || snapshot.idx != generation || snapshot.vlogManager != manager || database.rootPublication != runtime || database.primaryCustodyHead != custody {
		t.Fatal("active read lost physical cursor custody")
	}
	if ready, e = custody.ReleaseReadPinV5(&iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}); ready || !errors.Is(e, primaryarena.ErrStale) {
		t.Fatalf("former owner %v %v", ready, e)
	}
	snapshot.endRead() // finishes the already running read; does not finalize custody
	if custody.done || snapshot.idx != generation {
		t.Fatal("public endRead stole terminal retirement")
	}
	if e = database.Close(); e != nil {
		t.Fatal(e)
	}
	if !custody.done || lease.snapshot != nil || lease.root != nil || lease.generation != nil || snapshot.idx != nil || snapshot.db != nil {
		t.Fatal("completed cursor retained reader fields")
	}
}
