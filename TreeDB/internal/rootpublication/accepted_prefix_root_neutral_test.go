package rootpublication

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func neutralBoundaryCoordinator(t *testing.T, initial LogicalFrontiers) *Coordinator {
	t.Helper()
	c, err := New(Options{StartupSynchronous: true, Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
		return PublishResult{Outcome: PublishSucceeded}
	})})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.ConfigureAcceptedPrefix(8, initial); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.CloseAccepted(); stopClean(t, c) })
	return c
}

func neutralBoundaryAppend(t *testing.T, c *Coordinator, lsn uint64, maintenance bool, descriptor AcceptedDescriptor) AcceptedTicket {
	t.Helper()
	ticket, err := c.ReserveAccepted(maintenance)
	if err != nil {
		t.Fatal(err)
	}
	if err := ticket.RecordAssigned(lsn, nil, false); err != nil {
		t.Fatal(err)
	}
	if err := ticket.InstallAccepted(descriptor); err != nil {
		t.Fatal(err)
	}
	return ticket
}

func TestAcceptedRootNeutralBoundaryExactFiniteSuffix(t *testing.T) {
	c := neutralBoundaryCoordinator(t, LogicalFrontiers{})
	neutralBoundaryAppend(t, c, 1, false, AcceptedDescriptor{Sequence: 1})
	neutralBoundaryAppend(t, c, 2, false, AcceptedDescriptor{Sequence: 1, RootNeutral: true})
	opening := SealedPrefix{LogicalSequence: 1, CanonicalLSN: 1}
	if err := c.MarkAcceptedBoundary(opening); !errors.Is(err, ErrAcceptedReceipt) {
		t.Fatalf("strict mark accepted stale canonical endpoint: %v", err)
	}
	through, err := c.MarkAcceptedRootNeutralBoundary(opening)
	if err != nil || through != (SealedPrefix{LogicalSequence: 1, CanonicalLSN: 2}) {
		t.Fatalf("actual neutral closure: %+v %v", through, err)
	}
	// A later assigned barrier is outside the already marked finite cut even
	// before it installs. Capture must retain the exact earlier endpoint.
	later, err := c.ReserveAccepted(false)
	if err != nil {
		t.Fatal(err)
	}
	if err := later.RecordAssigned(3, nil, false); err != nil {
		t.Fatal(err)
	}
	cut, err := c.CaptureAcceptedCut(through, LogicalRootSet{Raw: new(int)})
	if err != nil {
		t.Fatal(err)
	}
	var visits int
	if err := cut.Range(func(_ AcceptedDescriptor, r AcceptedReceipt) error {
		visits++
		if !r.Installed || r.CanonicalLSN > 2 {
			t.Fatalf("finite cut imported later barrier: %+v", r)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if visits != 2 || cut.Through() != through {
		t.Fatalf("finite cut changed: visits=%d through=%+v", visits, cut.Through())
	}
	if err := cut.Abandon(); err != nil {
		t.Fatal(err)
	}
	if err := later.InstallAccepted(AcceptedDescriptor{Sequence: 1, RootNeutral: true}); err != nil {
		t.Fatal(err)
	}
	if next, err := c.MarkAcceptedRootNeutralBoundary(through); err != nil || next.CanonicalLSN != 3 {
		t.Fatalf("next cut lost later barrier: %+v %v", next, err)
	}
	if got := c.LogicalFrontiers(); got.V != 1 || got.C != 3 || got.W != 0 || got.S != (SealedPrefix{}) {
		t.Fatalf("neutral proof advanced another frontier: %+v", got)
	}
}

func TestAcceptedRootNeutralBoundaryStartsAtSealedEndpoint(t *testing.T) {
	sealed := SealedPrefix{LogicalSequence: 4, CanonicalLSN: 9}
	c := neutralBoundaryCoordinator(t, LogicalFrontiers{V: 4, C: 9, W: 9, S: sealed})
	if got, err := c.MarkAcceptedRootNeutralBoundary(sealed); err != nil || got != sealed {
		t.Fatalf("already sealed no-op: %+v %v", got, err)
	}
	neutralBoundaryAppend(t, c, 10, false, AcceptedDescriptor{Sequence: 4, RootNeutral: true})
	if got, err := c.MarkAcceptedRootNeutralBoundary(sealed); err != nil || got != (SealedPrefix{LogicalSequence: 4, CanonicalLSN: 10}) {
		t.Fatalf("barrier-only closure from original S: %+v %v", got, err)
	}
}

type neutralBoundarySealTarget struct{}

func (*neutralBoundarySealTarget) BindAcceptedPhysicalSeal(PhysicalSealReceipt) {}

func TestAcceptedRootNeutralBoundaryRejectsUnprovedSuffix(t *testing.T) {
	for _, name := range []string{"foreign_anchor", "changed_V", "non_neutral", "projection", "custody", "seal_target", "admitted_charge", "cleanup_charge", "gap", "uncertain"} {
		t.Run(name, func(t *testing.T) {
			c := neutralBoundaryCoordinator(t, LogicalFrontiers{})
			neutralBoundaryAppend(t, c, 1, false, AcceptedDescriptor{Sequence: 1})
			opening := SealedPrefix{LogicalSequence: 1, CanonicalLSN: 1}
			d := AcceptedDescriptor{Sequence: 1, RootNeutral: true}
			maintenance := false
			switch name {
			case "foreign_anchor":
				opening.CanonicalLSN = 0
			case "changed_V":
				d.Sequence, d.RootNeutral = 2, false
			case "non_neutral":
				d.RootNeutral, maintenance = false, true
			case "projection":
				d.Projections = []ParticipantProjection{{Domain: 1, Incarnation: 1, Plan: new(int)}}
				d.Custody = &prefixOriginalCustody{release: func() error { return nil }}
			case "custody":
				d.Custody = &prefixOriginalCustody{release: func() error { return nil }}
			case "seal_target":
				d.SealTarget = &neutralBoundarySealTarget{}
			case "admitted_charge":
				d.AdmittedBytes = 1
			case "cleanup_charge":
				d.CleanupBytes = 1
			}
			if name == "uncertain" {
				ticket, err := c.ReserveAccepted(false)
				if err != nil {
					t.Fatal(err)
				}
				injected := errors.New("assigned barrier append refused")
				if err := ticket.RecordAssigned(2, injected, false); !errors.Is(err, injected) {
					t.Fatal(err)
				}
				if _, err := c.MarkAcceptedRootNeutralBoundary(opening); !errors.Is(err, injected) {
					t.Fatalf("uncertain assignment lost original poison: %v", err)
				}
				return
			}
			ticket := neutralBoundaryAppend(t, c, 2, maintenance, d)
			if name == "gap" {
				// The proof must independently reject an invalid original range,
				// rather than trusting a sampled canonical maximum.
				c.accepted.mu.Lock()
				c.accepted.entries[ticket.slot].firstCanonicalLSN = 3
				c.accepted.mu.Unlock()
			}
			if _, err := c.MarkAcceptedRootNeutralBoundary(opening); !errors.Is(err, ErrAcceptedReceipt) {
				t.Fatalf("unproved %s suffix accepted: %v", name, err)
			}
		})
	}
}

// Cond.Wait invokes this Unlock only when the proof actually joins the assigned
// tail. Installation/poison/Close starts after that event, without timing sleeps
// or exported production hooks; all locking still uses the original mutex.
type neutralBoundaryWaitLock struct {
	mu      *sync.Mutex
	entered chan struct{}
	once    sync.Once
}

func (l *neutralBoundaryWaitLock) Lock() { l.mu.Lock() }
func (l *neutralBoundaryWaitLock) Unlock() {
	l.once.Do(func() { close(l.entered) })
	l.mu.Unlock()
}

func TestAcceptedRootNeutralBoundaryJoinsAssignedTail(t *testing.T) {
	for _, outcome := range []string{"install", "poison", "close"} {
		t.Run(outcome, func(t *testing.T) {
			c := neutralBoundaryCoordinator(t, LogicalFrontiers{})
			neutralBoundaryAppend(t, c, 1, false, AcceptedDescriptor{Sequence: 1})
			ticket, err := c.ReserveAccepted(false)
			if err != nil {
				t.Fatal(err)
			}
			if err := ticket.RecordAssigned(2, nil, false); err != nil {
				t.Fatal(err)
			}
			entered := make(chan struct{})
			c.accepted.changed.L = &neutralBoundaryWaitLock{mu: &c.accepted.mu, entered: entered}
			type result struct {
				through SealedPrefix
				err     error
			}
			done := make(chan result, 1)
			go func() {
				through, err := c.MarkAcceptedRootNeutralBoundary(SealedPrefix{LogicalSequence: 1, CanonicalLSN: 1})
				done <- result{through, err}
			}()
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("proof did not join assigned but uninstalled tail")
			}
			injected := errors.New("original barrier failed")
			switch outcome {
			case "install":
				if err := ticket.InstallAccepted(AcceptedDescriptor{Sequence: 1, RootNeutral: true}); err != nil {
					t.Fatal(err)
				}
			case "poison":
				c.PoisonAccepted(injected)
			case "close":
				if err := c.CloseAccepted(); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case got := <-done:
				switch outcome {
				case "install":
					if got.err != nil || got.through != (SealedPrefix{LogicalSequence: 1, CanonicalLSN: 2}) {
						t.Fatalf("assigned tail not included: %+v %v", got.through, got.err)
					}
				case "poison":
					if !errors.Is(got.err, injected) {
						t.Fatal("lost original poison", got.err)
					}
				case "close":
					if !errors.Is(got.err, ErrClosed) {
						t.Fatal("lost original Close", got.err)
					}
				}
			case <-time.After(3 * time.Second):
				t.Fatal("assigned-tail proof did not terminate")
			}
		})
	}
}
