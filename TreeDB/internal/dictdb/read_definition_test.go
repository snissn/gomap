package dictdb

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func TestCOWDictionaryReadDefinitionOwnsBytesAndPhysicalPin(t *testing.T) {
	for _, pointer := range []bool{false, true} {
		t.Run(map[bool]string{false: "inline", true: "pointer"}[pointer], func(t *testing.T) {
			s, err := Open(t.TempDir(), db.Options{ChunkSize: 65536, Durability: db.DurabilityWALOffRelaxed})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			want := []byte("dictionary")
			if pointer {
				want = bytes.Repeat([]byte("dictionary-payload|"), 100)
			}
			id, err := s.PutDictBytes(context.Background(), want)
			if err != nil {
				t.Fatal(err)
			}
			registry := s.backend.ValueLogIdentityPinRegistry()
			beforePins, beforeLinks := registry.ActivePins(), registry.ActiveStableNamespaceLinks()
			var live int
			var admitted DictionaryReadAllocationSizes
			owner, err := s.AcquireDictionaryReadDefinition(id, valuelog.COWReadLimits{MaxRecordBytes: 1 << 20, MaxRawBytes: 1 << 20, MaxValueBytes: 1 << 20}, 256, func(sizes DictionaryReadAllocationSizes) (func(), error) {
				live++
				admitted.Definition += sizes.Definition
				admitted.Payload += sizes.Payload
				admitted.Pin += sizes.Pin
				return func() { live-- }, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(owner.Bytes, want) || admitted.Definition != uint64(len(want)) {
				t.Fatal("definition ownership")
			}
			if owner.snapshot != nil {
				t.Fatal("copied definition retained namespace snapshot")
			}
			if registry.ActiveStableNamespaceLinks() != beforeLinks {
				t.Fatal("read capture made a durability proof")
			}
			if pointer {
				if registry.ActivePins() != beforePins+1 || admitted.Payload == 0 || admitted.Pin == 0 {
					t.Fatal("physical source pin missing")
				}
				lease, err := registry.BeginDelete(dictionarySourceIdentityForTest(t, s, id))
				if lease != nil || !errors.Is(err, rootpublication.ErrResourcePinned) {
					t.Fatalf("delete retained definition: %v %v", lease, err)
				}
			}
			if err := s.SetCurrent(context.Background(), id); err != nil {
				t.Fatal(err)
			}
			if err := s.SetCurrent(context.Background(), 0); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(owner.Bytes, want) {
				t.Fatal("marker changed old definition")
			}
			var identity rootpublication.StableIdentity
			if pointer {
				identity = dictionarySourceIdentityForTest(t, s, id)
			}
			pinsBeforeDrain := registry.ActivePins()
			owner.Close()
			owner.Close()
			wantPins := pinsBeforeDrain
			if pointer {
				wantPins--
			}
			if live != 0 || registry.ActivePins() != wantPins {
				t.Fatalf("owner leaked: charges=%d pins=%d", live, registry.ActivePins())
			}
			if pointer {
				lease, err := registry.BeginDelete(identity)
				if err == nil {
					lease.Abort()
				} else if !errors.Is(err, rootpublication.ErrResourcePinned) {
					t.Fatalf("delete after read drain: %v", err)
				}
				// Producer/publication custody may independently pin a live
				// dictionary source. This owner must release exactly its one pin.
				if registry.PinCount(identity) >= pinsBeforeDrain {
					t.Fatal("physical read pin survived drain")
				}
			}
		})
	}
}

func dictionarySourceIdentityForTest(t *testing.T, s *Store, id uint64) rootpublication.StableIdentity {
	t.Helper()
	snapshot := s.backend.AcquireSnapshot()
	defer snapshot.Close()
	entry, err := snapshot.GetEntryExact(bytesKey(id))
	if err != nil {
		t.Fatal(err)
	}
	_, identity, err := snapshot.PinnedValueLogFile(entry.ValuePtr.FileID)
	if err != nil {
		t.Fatal(err)
	}
	return identity
}

func TestCOWDictionaryReadDefinitionRefusesBeforeEffectAndUnwinds(t *testing.T) {
	s, err := Open(t.TempDir(), db.Options{ChunkSize: 65536})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	id, err := s.PutDictBytes(context.Background(), bytes.Repeat([]byte("payload|"), 200))
	if err != nil {
		t.Fatal(err)
	}
	refusal := errors.New("capacity")
	limits := valuelog.COWReadLimits{MaxRecordBytes: 1 << 20, MaxRawBytes: 1 << 20, MaxValueBytes: 1 << 20}
	if got := testing.AllocsPerRun(100, func() {
		owner, err := s.AcquireDictionaryReadDefinition(id, limits, 256, func(DictionaryReadAllocationSizes) (func(), error) { return nil, refusal })
		if owner != nil || err != refusal {
			t.Fatal("owner refusal")
		}
	}); got != 0 {
		t.Fatalf("refused definition allocated %g", got)
	}
	before := s.backend.ValueLogIdentityPinRegistry().ActivePins()
	for refuseStage := 2; refuseStage <= 3; refuseStage++ {
		stage, live := 0, 0
		owner, err := s.AcquireDictionaryReadDefinition(id, limits, 256, func(DictionaryReadAllocationSizes) (func(), error) {
			stage++
			if stage == refuseStage {
				return nil, refusal
			}
			live++
			return func() { live-- }, nil
		})
		if owner != nil || err != refusal || live != 0 || s.backend.ValueLogIdentityPinRegistry().ActivePins() != before {
			t.Fatalf("stage %d failed unwind: %v live=%d", refuseStage, err, live)
		}
	}
}

func TestCOWDictionaryPrepareTransfersTemporaryCleanup(t *testing.T) {
	s, err := Open(t.TempDir(), db.Options{ChunkSize: 65536})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	id, err := s.PutDictBytes(context.Background(), []byte("dictionary"))
	if err != nil {
		t.Fatal(err)
	}
	limits := valuelog.COWReadLimits{MaxRecordBytes: 1 << 20, MaxRawBytes: 1 << 20, MaxValueBytes: 1 << 20}
	refusal := errors.New("third stage refused")
	stage, live := 0, 0
	owner, err := s.PrepareDictionaryReadDefinition(id, limits, 256, func(DictionaryReadAllocationSizes) (func(), error) {
		stage++
		if stage == 3 {
			return nil, refusal
		}
		live++
		return func() { live-- }, nil
	})
	if owner == nil || err != refusal || owner.snapshot == nil || live != 2 {
		t.Fatalf("partial owner lost: %v live=%d", err, live)
	}
	owner.Close()
	if live != 0 {
		t.Fatal("partial owner leaked")
	}
	owner, err = s.PrepareDictionaryReadDefinition(id, limits, 256, func(DictionaryReadAllocationSizes) (func(), error) { live++; return func() { live-- }, nil })
	if err != nil || owner.snapshot == nil || live != 3 {
		t.Fatalf("prepared owner: %v live=%d", err, live)
	}
	owner.ReleaseCapture()
	if owner.snapshot != nil || live != 2 || string(owner.Bytes) != "dictionary" {
		t.Fatal("temporary capture retirement changed definition")
	}
	owner.Close()
	if live != 0 {
		t.Fatal("prepared owner leaked")
	}
}
