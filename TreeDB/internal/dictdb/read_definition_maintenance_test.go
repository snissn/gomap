package dictdb

import (
	"bytes"
	"context"
	"errors"
	"runtime"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
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
					if result == nil && !bytes.Equal(owned.Bytes, want) {
						panic("authenticated definition mismatch")
					}
				}()
				<-done // Publishes all scalar/owner observations, including Goexit.
				if premature || live != 0 {
					t.Fatalf("cleanup timing/refund: premature=%t live=%d", premature, live)
				}
				if mode == "refusal" && !errors.Is(result, refusal) {
					t.Fatalf("lost original refusal: %v", result)
				}
				if mode == "panic" && !unwound {
					t.Fatal("panic was not propagated to original unwind")
				}
				if mode == "success" && result != nil {
					t.Fatal(result)
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
