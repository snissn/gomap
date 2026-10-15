package db

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/snissn/gomap/TreeDB/page"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// Refuse precisely the late admission, while allowing original abort cleanup.
type primaryReviewRefusalBudget struct{ refuseNext bool }
type primaryReviewRefusalLease struct{ budget *primaryReviewRefusalBudget }

func (b *primaryReviewRefusalBudget) AcquireRetention(uint64) (retainedalloc.Lease, error) {
	return &primaryReviewRefusalLease{b}, nil
}
func (l *primaryReviewRefusalLease) Resize(uint64) error {
	if l.budget.refuseNext {
		l.budget.refuseNext = false
		return retainedalloc.ErrCapacity
	}
	return nil
}
func (*primaryReviewRefusalLease) Close() {}

func TestPrimaryDataCertificateV6RefusalKeepsAcceptedCache(t *testing.T) {
	for _, seeded := range []bool{false, true} {
		for _, phase := range []string{"prepare", "activate"} {
			t.Run(fmt.Sprintf("seeded=%t/%s", seeded, phase), func(t *testing.T) {
				d, err := Open(Options{Dir: t.TempDir(), ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
				if err != nil {
					t.Fatal(err)
				}
				defer d.Close()
				if seeded {
					if err = d.SetSync([]byte("old"), []byte("value")); err != nil {
						t.Fatal(err)
					}
				}
				runtime := d.rootPublication
				old := runtime.primaryDataCertificateV6
				snapshot := d.AcquireSnapshot()
				defer snapshot.Close()
				arena := d.idx.Load().primary
				budget := &primaryReviewRefusalBudget{}
				enrollment, err := arena.MetadataOwner().Enroll(budget)
				if err != nil {
					t.Fatal(err)
				}
				defer enrollment.Close()
				var failedRef primaryarena.Ref
				observed := false
				testHookPrimaryDataCertificateV6 = func(at string, r *rootPublicationRuntimeV1, p *rootpublication.PreparedPrimaryRootV5) {
					if at != "prepared" || r != runtime {
						return
					}
					observed = true
					if p.DataCertificateV6() == nil || p.DataCertificateV6() == old {
						t.Fatal("fixture did not construct a fresh actual DATA proof")
					}
					if r.primaryDataCertificateV6 != old {
						t.Fatal("private proof replaced accepted cache before activation")
					}
					failedRef = p.ReadCertificateV6().Reference()
					if arena.MetadataOwner().Bytes() == 0 {
						t.Fatal("prepared proof lacks original root allowance")
					}
					if phase == "prepare" {
						budget.refuseNext = true
					}
				}
				defer func() {
					testHookPrimaryDataCertificateV6 = nil
					d.testFailDurableRootVisibleInstall.Store(false)
					budget.refuseNext = false
				}()
				if phase == "activate" {
					d.testFailDurableRootVisibleInstall.Store(true)
				}
				err = d.SetSync(bytes.Repeat([]byte("x"), 3500), []byte("new DATA"))
				testHookPrimaryDataCertificateV6 = nil
				d.testFailDurableRootVisibleInstall.Store(false)
				budget.refuseNext = false
				want := retainedalloc.ErrCapacity
				if phase == "activate" {
					want = errTestDurableRootVisibleInstallFailpoint
				}
				if !observed || !errors.Is(err, want) {
					t.Fatalf("late refusal not reached: observed=%v err=%v", observed, err)
				}
				if runtime.primaryDataCertificateV6 != old {
					t.Fatal("refusal replaced accepted DATA cache")
				}
				for {
					ready, progress, e := arena.ReleaseStep(nil)
					if e != nil {
						t.Fatal(e)
					}
					if ready && !progress {
						break
					}
				}
				work := &iterator.OrdinalScanWork{RecordLimit: 1000, ByteLimit: 1 << 24}
				if _, ready, e := arena.BorrowReadRootCertificateV6(failedRef, work); ready || e == nil {
					t.Fatal("aborted private root survived its original disposal")
				}
				if seeded {
					if got, e := snapshot.Get([]byte("old")); e != nil || string(got) != "value" {
						t.Fatal("refusal invalidated independent old cut", e)
					}
				}
			})
		}
	}
}

func TestPrimaryDataCertificateV6TerminalDetachesBeforeWithdrawal(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	if err = d.SetSync([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	runtime := d.rootPublication
	if runtime.primaryDataCertificateV6 == nil {
		t.Fatal("missing accepted proof")
	}
	observed := false
	testHookPrimaryDataCertificateV6 = func(phase string, r *rootPublicationRuntimeV1, _ *rootpublication.PreparedPrimaryRootV5) {
		if phase != "terminal-detached" {
			return
		}
		observed = true
		if r != runtime || r.primaryDataCertificateV6 != nil {
			t.Fatal("terminal cache alias remains")
		}
		state := d.state.Load()
		if state == nil || state.primaryRoot == nil || state.primaryRoot.arena.MetadataOwner().Bytes() == 0 {
			t.Fatal("detachment happened after supplying root charge withdrew")
		}
	}
	defer func() { testHookPrimaryDataCertificateV6 = nil }()
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	if !observed || runtime.primaryDataCertificateV6 != nil {
		t.Fatal("terminal detachment missing")
	}
}

func TestPrimaryJointRecoveryMarkerV6FixedAdmittedScratch(t *testing.T) {
	for _, size := range []int{4096, 4140, 4141, 4142, 8192} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, indexReadyFileName), bytes.Repeat([]byte("x"), size), 0600); err != nil {
				t.Fatal(err)
			}
			registry := rootpublication.NewIdentityPinRegistry()
			defer registry.Close()
			before := registry.MetadataOwner().Bytes()
			var captured *primaryJointRecoveryProofV6
			testHookPrimaryJointRecoveryProofV6 = func(phase string, p *primaryJointRecoveryProofV6) {
				if phase == "before-marker-close" {
					captured = p
					if cap(p.image) != primaryJointMarkerReadLimitV6 || len(p.image) != min(size, primaryJointMarkerReadLimitV6) {
						t.Fatalf("unbounded marker backing len=%d cap=%d", len(p.image), cap(p.image))
					}
					if p.metadata.Bytes() < before+p.charge {
						t.Fatal("marker backing lacks original admission")
					}
				}
			}
			defer func() { testHookPrimaryJointRecoveryProofV6 = nil }()
			handled, err := recoverPrimaryJointSwapWithRegistryV6(dir, false, registry, nil)
			testHookPrimaryJointRecoveryProofV6 = nil
			if !handled || !errors.Is(err, ErrRecoveryRequired) || captured == nil {
				t.Fatalf("malformed marker accepted: %v %v", handled, err)
			}
			if captured.image != nil || captured.marker != nil || captured.parent != nil || captured.metadata != nil || captured.charge != 0 || registry.MetadataOwner().Bytes() != before {
				t.Fatal("decode failure retained/refunded live proof backing incorrectly")
			}
			if image, e := os.ReadFile(filepath.Join(dir, indexReadyFileName)); e != nil || len(image) != size {
				t.Fatal("decode refusal altered decision", e)
			}
		})
	}
}

func TestPrimaryJointRecoveryMarkerV6AdmissionRefusesBeforeProof(t *testing.T) {
	registry := rootpublication.NewIdentityPinRegistry()
	defer registry.Close()
	owner := registry.MetadataOwner()
	budget := &primaryScratchBudget{capacity: ^uint64(0)}
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	before := owner.Bytes()
	budget.capacity = before
	proof, err := newPrimaryJointRecoveryProofV6(registry, nil)
	if proof != nil || !errors.Is(err, retainedalloc.ErrCapacity) || owner.Bytes() != before {
		t.Fatalf("proof allocated/mutated before admission: proof=%v err=%v bytes=%d->%d", proof != nil, err, before, owner.Bytes())
	}
	budget.capacity = ^uint64(0)
}

func TestPrimaryClosedIteratorDetachesOriginalRootV6(t *testing.T) {
	for _, owned := range []bool{false, true} {
		t.Run(fmt.Sprintf("owned=%t", owned), func(t *testing.T) {
			d, e := Open(Options{Dir: t.TempDir(), ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
			if e != nil {
				t.Fatal(e)
			}
			defer d.Close()
			if e = d.SetSync([]byte("key"), []byte("value")); e != nil {
				t.Fatal(e)
			}
			s := d.AcquireSnapshot()
			root := s.primaryRoot
			arena := root.arena
			var raw iterator.UnsafeIterator
			var bound *snapshotBoundIterator
			if owned {
				raw, e = s.OwnedPointerProjectionIterator([]byte("a"), []byte("z"), func(page.LeafLogPtr, []byte) ([]byte, error) { return nil, errors.New("unexpected leaf callback") })
			} else {
				var it iterator.UnsafeIterator
				it, e = s.IteratorWithOptions([]byte("a"), []byte("z"), IteratorOptions{})
				if e == nil {
					bound = it.(*snapshotBoundIterator)
					raw = bound.inner
				}
			}
			if e != nil {
				t.Fatal(e)
			}
			before := reflect.ValueOf(raw).Elem()
			if before.Type().Name() != "primaryIterator" || before.FieldByName("directory").IsZero() || before.FieldByName("base").IsNil() {
				t.Fatal("fixture did not borrow actual PRIMARY directory and base")
			}
			if owned && (before.FieldByName("readLeaf").IsNil() || before.FieldByName("leafScratch").Len() == 0) {
				t.Fatal("owned fixture lacks callback/scratch")
			}
			if !owned {
				if e = s.Close(); e != nil {
					t.Fatal(e)
				}
				if s.finalized.Load() {
					t.Fatal("live iterator did not retain original Snapshot")
				}
			}
			if bound != nil {
				e = bound.Close()
			} else {
				e = raw.Close()
			}
			if e != nil {
				t.Fatal(e)
			}
			if e = raw.Close(); e != nil {
				t.Fatal(e)
			}
			if bound != nil && (bound.inner != nil || bound.owner != nil) {
				t.Fatal("closed exported handle retains backend")
			}
			after := reflect.ValueOf(raw).Elem()
			for _, field := range []string{"baseTree", "base", "delta", "slot", "directory", "opts", "readLeaf", "leafScratch"} {
				if !after.FieldByName(field).IsZero() {
					t.Fatalf("closed iterator retains %s", field)
				}
			}
			a, b := raw.Domain()
			if string(a) != "a" || string(b) != "z" || raw.Valid() {
				t.Fatal("closed observations changed")
			}
			if e = s.Close(); e != nil {
				t.Fatal(e)
			}
			if s.state != nil || s.primaryRoot != nil || s.idx != nil {
				t.Fatal("Snapshot did not finish after iterator close")
			}
			// Advance both fixed slots and their parents; a closed retained handle must
			// not keep the superseded logical root or its original metadata charge alive.
			for i := 0; i < 6; i++ {
				if e = d.SetSync([]byte(fmt.Sprintf("advance/%d", i)), []byte("v")); e != nil {
					t.Fatal(e)
				}
			}
			for {
				ready, progress, err := arena.ReleaseStep(nil)
				if err != nil {
					t.Fatal(err)
				}
				if ready && !progress {
					break
				}
			}
			w := &iterator.OrdinalScanWork{RecordLimit: 1000, ByteLimit: 1 << 24}
			if _, ready, err := arena.BorrowReadRootCertificateV6(root.ref, w); ready || err == nil {
				t.Fatal("superseded original root/charge remains after all live cuts drain")
			}
			if bound != nil {
				if e = bound.Close(); e != nil {
					t.Fatal(e)
				}
			}
		})
	}
}
