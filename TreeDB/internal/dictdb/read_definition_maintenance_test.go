package dictdb

import (
	"bytes"
	"context"
	"errors"
	"runtime"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func TestCOWMaintenanceDictionaryEarlyCustodyAndUnwind(t *testing.T) {
	for _, mode := range []string{"refusal", "panic", "goexit", "success"} {
		for _, stop := range []int{1, 2, 3} {
			t.Run(mode+string(rune('0'+stop)), func(t *testing.T) {
				s, err := Open(t.TempDir(), db.Options{ChunkSize: 65536})
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := s.Close(); err != nil {
						t.Error(err)
					}
				}()
				want := bytes.Repeat([]byte("maintenance-definition|"), 128)
				id, err := s.PutDictBytes(context.Background(), want)
				if err != nil {
					t.Fatal(err)
				}
				limits := valuelog.COWReadLimits{MaxRecordBytes: 1 << 20, MaxRawBytes: 1 << 20, MaxValueBytes: 1 << 20}
				refusal := errors.New("selected admission refusal")
				var owned *DictionaryReadDefinition
				var result error
				stage, live, releases := 0, 0, 0
				unwound, premature := false, false
				returned, matched := false, false
				done := make(chan struct{})
				go func() {
					defer close(done)
					defer func() {
						unwound = recover() != nil
						premature = releases != 0
						if err := owned.ClosePreparedRead(); err != nil {
							result = errors.Join(result, err)
						}
					}()
					result = s.PrepareDictionaryReadDefinitionInto(id, limits, 256, func(DictionaryReadAllocationSizes) (func(), error) {
						stage++
						if stage > 1 && owned == nil {
							panic("owner not transferred before next effect")
						}
						if stage == stop {
							switch mode {
							case "refusal":
								return nil, refusal
							case "panic":
								panic(refusal)
							case "goexit":
								runtime.Goexit()
							}
						}
						live++
						return func() { live--; releases++ }, nil
					}, &owned)
					if result == nil {
						matched = owned != nil && bytes.Equal(owned.Bytes, want)
					}
					returned = true
				}()
				<-done // Publishes all scalar/owner observations, including Goexit.
				if premature || live != 0 {
					t.Fatalf("cleanup timing/refund: premature=%t live=%d", premature, live)
				}
				if mode == "refusal" && !errors.Is(result, refusal) {
					t.Fatalf("lost original refusal: %v", result)
				}
				if mode == "panic" && (!unwound || returned) {
					t.Fatal("panic was not propagated to original unwind")
				}
				if mode == "goexit" && (unwound || returned) {
					t.Fatal("Goexit was not propagated to original unwind")
				}
				if mode == "success" && (result != nil || unwound || !returned || !matched || owned == nil) {
					t.Fatalf("success must return authenticated owned bytes: err=%v unwound=%t returned=%t matched=%t owner=%t", result, unwound, returned, matched, owned != nil)
				}
				if owned != nil {
					if err := owned.ClosePreparedRead(); err != nil {
						t.Fatal(err)
					}
					if owned.Bytes != nil || owned.snapshot != nil || owned.metadata != nil || owned.pin != nil || owned.releaseOwner != nil || owned.releaseSnapshot != nil || owned.releaseBytes != nil {
						t.Fatal("refunded before owner aliases were detached")
					}
				}
			})
		}
	}
}

func TestCOWMaintenanceDictionaryRetainsIncompleteCleanup(t *testing.T) {
	// This negative control supplies a real private snapshot without installing
	// its required completion receipt. Close acceptance cannot be used as a join.
	s, err := Open(t.TempDir(), db.Options{ChunkSize: 65536})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	}()
	refunded := false
	d := &DictionaryReadDefinition{snapshot: s.backend.AcquireStableSnapshot(), releaseOwner: func() { refunded = true }}
	if d.snapshot == nil {
		t.Fatal("missing actual snapshot")
	}
	first := d.ClosePreparedRead()
	if first == nil || d.ClosePreparedRead() != first || refunded || d.snapshot == nil || d.releaseOwner == nil {
		t.Fatal("incomplete original cleanup discarded custody/refunded/retried")
	}
}

func TestCOWMaintenanceDictionaryPreparedCleanupUnwind(t *testing.T) {
	// These are the three existing fallible admission-refund callbacks, not
	// replacement snapshot destructors. No new production fault hook is needed.
	for _, mode := range []string{"panic", "goexit", "success"} {
		for _, stop := range []int{0, 1, 2} {
			t.Run(mode+string(rune('0'+stop)), func(t *testing.T) {
				var calls, refunds [3]int
				d := &DictionaryReadDefinition{Bytes: []byte("owned")}
				callback := func(stage int) func() {
					return func() {
						calls[stage]++
						if stage == stop {
							switch mode {
							case "panic":
								panic("selected prepared cleanup unwind")
							case "goexit":
								runtime.Goexit()
							}
						}
						refunds[stage]++
					}
				}
				d.releaseSnapshot, d.releaseBytes, d.releaseOwner = callback(0), callback(1), callback(2)
				returned, panicked := false, false
				var first error
				done := make(chan struct{})
				go func() {
					defer close(done)
					defer func() { panicked = recover() != nil }()
					first = d.ClosePreparedRead()
					returned = true
				}()
				<-done // Publishes both Once completion and actual callback effects.
				beforeCalls, beforeRefunds := calls, refunds
				again := d.ClosePreparedRead()
				if mode == "success" {
					if !returned || panicked || first != nil || again != nil || calls != [3]int{1, 1, 1} || refunds != calls || d.Bytes != nil || d.releaseSnapshot != nil || d.releaseBytes != nil || d.releaseOwner != nil {
						t.Fatalf("ordinary complete cleanup/idempotence: returned=%t panic=%t first=%v again=%v calls=%v refunds=%v", returned, panicked, first, again, calls, refunds)
					}
				} else {
					if returned || (mode == "panic") != panicked || !errors.Is(again, rootpublication.ErrResourceOwnership) || d.ClosePreparedRead() != again {
						t.Fatalf("unfinished Once reported success: returned=%t panic=%t error=%v", returned, panicked, again)
					}
					owners := [3]bool{d.releaseSnapshot != nil, d.releaseBytes != nil, d.releaseOwner != nil}
					for stage := range calls {
						wantCalls, wantRefunds := 0, 0
						if stage <= stop {
							wantCalls = 1
						}
						if stage < stop {
							wantRefunds = 1
						}
						if calls[stage] != wantCalls || refunds[stage] != wantRefunds || owners[stage] != (stage >= stop) {
							t.Fatalf("lost failed callback custody or false refund at %d: calls=%v refunds=%v owners=%v", stage, calls, refunds, owners)
						}
					}
					if stop == 0 && !bytes.Equal(d.Bytes, []byte("owned")) {
						t.Fatal("capture unwind discarded independent owned bytes")
					}
				}
				if calls != beforeCalls || refunds != beforeRefunds {
					t.Fatal("terminal cleanup retried a consumed destructor")
				}
			})
		}
	}
}

func TestCOWMaintenanceDictionaryPreparedCleanupRetainsRecordedError(t *testing.T) {
	s, err := Open(t.TempDir(), db.Options{ChunkSize: 65536})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	}()
	first := errors.New("original snapshot completion error")
	d := &DictionaryReadDefinition{snapshot: s.backend.AcquireStableSnapshot(), Bytes: []byte("retained")}
	if d.snapshot == nil {
		t.Fatal("missing real snapshot")
	}
	// A controlled recorded consumer error must not be replaced by the terminal
	// incomplete marker. The real snapshot closes normally here: this is an
	// error-retention control, not an injected storage-failure reproduction.
	d.SnapshotCleanupCompleted(first)
	refunded := false
	d.releaseOwner = func() { refunded = true }
	got := d.ClosePreparedRead()
	if !errors.Is(got, first) || d.ClosePreparedRead() != got || refunded || d.snapshot == nil || d.releaseOwner == nil || !bytes.Equal(d.Bytes, []byte("retained")) {
		t.Fatalf("original error/custody was replaced by an incomplete marker: %v", got)
	}
}
