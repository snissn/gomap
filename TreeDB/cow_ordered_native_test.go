package treedb

import (
	"bytes"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/caching"
	"github.com/snissn/gomap/TreeDB/internal/mvccadmission"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
)

func TestCOWOrderedLogicalFacadeRetainedPhysicalRecords(t *testing.T) {
	for _, profile := range cowPublicContractProfiles {
		t.Run(string(profile), func(t *testing.T) {
			database, _, _ := cowPublicContractOpen(t, profile, func(options *Options) {
				options.MemtableMode = "cow_ordered"
			})
			if !database.SupportsLogicalMVCC() {
				t.Fatal("cow_ordered did not activate logical MVCC")
			}
			capability := database.NewMVCCAdmission()
			if capability == nil {
				t.Fatal("Store admission unavailable")
			}
			defer capability.Close()
			key, value := []byte{'a', 0, 'b'}, []byte("old-value")
			mutations := []mvccadmission.LogicalMutation{
				{Key: key, Value: value, Timestamp: 11},
				{Key: key, Delete: true, Timestamp: 17},
				{Key: key, Timestamp: 23},
				{Timestamp: 29, Value: []byte("empty-key")},
			}
			if err := database.CommitLogicalMVCC(mutations, capability.LogicalInput(mutations, 0), false); err != nil {
				t.Fatal(err)
			}
			key[0], value[0] = 'X', 'X'
			var commonRevision EntryRevision
			for _, expected := range []struct {
				key []byte
				ts  uint64
				v   []byte
			}{
				{[]byte{'a', 0, 'b'}, 11, []byte("\x01old-value")},
				{[]byte{'a', 0, 'b'}, 17, []byte{0x02}},
				{[]byte{'a', 0, 'b'}, 23, []byte{0x01}},
				{nil, 29, []byte("\x01empty-key")},
			} {
				physical, err := mvcckey.Encode(expected.key, expected.ts)
				if err != nil {
					t.Fatal(err)
				}
				got, revision, err := database.GetVersioned(physical)
				if err != nil || !bytes.Equal(got, expected.v) || revision == LegacyEntryRevision {
					t.Fatalf("retained TS %d: got %x revision=%d error=%v", expected.ts, got, revision, err)
				}
				if commonRevision == LegacyEntryRevision {
					commonRevision = revision
				} else if revision != commonRevision {
					t.Fatalf("one logical group received different native revisions: %d and %d", commonRevision, revision)
				}
			}
			// The singleton uses the same native batch producer and explicit
			// sync ACK while retaining its typed input contract.
			single := []mvccadmission.LogicalMutation{{Key: []byte("single"), Value: []byte("point"), Timestamp: 31}}
			if err := database.CommitLogicalMVCC(single, capability.LogicalInput(single, 0), true); err != nil {
				t.Fatal(err)
			}
			physical, _ := mvcckey.Encode(single[0].Key, single[0].Timestamp)
			got, err := database.Get(physical)
			if err != nil || !bytes.Equal(got, []byte("\x01point")) {
				t.Fatalf("logical point: %x %v", got, err)
			}
		})
	}
}

func TestCOWOrderedLogicalFacadeRejectsOperandSubstitution(t *testing.T) {
	database, _, _ := cowPublicContractOpen(t, ProfileNoWALFast, func(options *Options) {
		options.MemtableMode = "cow_ordered"
	})
	capability := database.NewMVCCAdmission()
	if capability == nil || !database.SupportsLogicalMVCC() {
		t.Fatal("logical engine/admission unavailable")
	}
	defer capability.Close()
	original := []mvccadmission.LogicalMutation{{Key: []byte("key"), Value: []byte("value"), Timestamp: 13}}
	input := capability.LogicalInput(original, 0)
	for _, test := range []struct {
		name string
		m    mvccadmission.LogicalMutation
	}{
		{"key", mvccadmission.LogicalMutation{Key: []byte("other"), Value: []byte("value"), Timestamp: 13}},
		{"value", mvccadmission.LogicalMutation{Key: []byte("key"), Value: []byte("other"), Timestamp: 13}},
		{"timestamp", mvccadmission.LogicalMutation{Key: []byte("key"), Value: []byte("value"), Timestamp: 14}},
		{"delete", mvccadmission.LogicalMutation{Key: []byte("key"), Value: []byte("value"), Timestamp: 13, Delete: true}},
	} {
		t.Run(test.name, func(t *testing.T) {
			before := database.cached.COWMemoryStats()
			if err := database.CommitLogicalMVCC([]mvccadmission.LogicalMutation{test.m}, input, false); !errors.Is(err, errLogicalMVCCInput) {
				t.Fatalf("substituted operand: %v", err)
			}
			after := database.cached.COWMemoryStats()
			if after != before {
				t.Fatalf("rejected operand changed COW accounting: before=%+v after=%+v", before, after)
			}
			physical, _ := mvcckey.Encode(test.m.Key, test.m.Timestamp)
			if got, err := database.Get(physical); err != nil || got != nil {
				t.Fatalf("rejected operand became visible: %x %v", got, err)
			}
		})
	}
	if err := database.CommitLogicalMVCC(original, mvccadmission.LogicalInput{}, false); !errors.Is(err, errLogicalMVCCInput) {
		t.Fatalf("missing input: %v", err)
	}
	if err := database.CommitLogicalMVCC(original, capability.LogicalInput(original, 13), false); !errors.Is(err, errLogicalMVCCInput) {
		t.Fatalf("input at discard floor: %v", err)
	}
}

func TestCOWOrderedLogicalFacadeModeAndClosedAdmission(t *testing.T) {
	legacy, _, _ := cowPublicContractOpen(t, ProfileNoWALFast)
	if legacy.SupportsLogicalMVCC() {
		t.Fatal("cow_btree advertised logical history")
	}
	if err := legacy.CommitLogicalMVCC(nil, mvccadmission.LogicalInput{}, false); !errors.Is(err, caching.ErrCOWUnsupported) {
		t.Fatalf("legacy logical commit: %v", err)
	}
	ordered, _, _ := cowPublicContractOpen(t, ProfileNoWALFast, func(options *Options) {
		options.MemtableMode = "cow_ordered"
	})
	if !ordered.SupportsLogicalMVCC() {
		t.Fatal("cow_ordered did not activate")
	}
	if err := ordered.Close(); err != nil {
		t.Fatal(err)
	}
	if ordered.SupportsLogicalMVCC() {
		t.Fatal("closed DB advertised logical history")
	}
	if err := ordered.CommitLogicalMVCC(nil, mvccadmission.LogicalInput{}, true); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed logical commit: %v", err)
	}
}

// The qualification tag is shared by both COW representations. Advertising no
// logical facade must leave the original qualified point and batch paths intact.
func TestCOWOrderedCompatibilityQualifiedWrites(t *testing.T) {
	for _, profile := range cowPublicContractProfiles {
		t.Run(string(profile), func(t *testing.T) {
			database, _, _ := cowPublicContractOpen(t, profile)
			if database.SupportsLogicalMVCC() {
				t.Fatal("cow_btree advertised logical history")
			}
			capability := database.NewMVCCAdmission()
			if capability == nil {
				t.Fatal("Store admission unavailable")
			}
			defer capability.Close()
			point, err := mvcckey.Encode([]byte("point"), 11)
			if err != nil {
				t.Fatal(err)
			}
			if err := database.SetPointWithMVCCInput(point, []byte("\x01point"), false, capability.Input(point, 11, 0)); err != nil {
				t.Fatal(err)
			}
			b := database.NewBatchWithSize(2)
			defer b.Close()
			qualified, ok := b.(interface {
				SetWithMVCCInput([]byte, []byte, mvccadmission.Input) error
			})
			if !ok {
				t.Fatal("ordinary native batch lost Store qualification")
			}
			var batchKeys [][]byte
			for _, ts := range []uint64{17, 23} {
				key, err := mvcckey.Encode([]byte("batch"), ts)
				if err != nil {
					t.Fatal(err)
				}
				batchKeys = append(batchKeys, key)
				if err := qualified.SetWithMVCCInput(key, []byte("\x01batch"), capability.Input(key, ts, 0)); err != nil {
					t.Fatal(err)
				}
			}
			if err := b.WriteSync(); err != nil {
				t.Fatal(err)
			}
			for _, expected := range []struct{ key, value []byte }{
				{point, []byte("\x01point")},
				{batchKeys[0], []byte("\x01batch")},
				{batchKeys[1], []byte("\x01batch")},
			} {
				got, revision, err := database.GetVersioned(expected.key)
				if err != nil || !bytes.Equal(got, expected.value) || revision == LegacyEntryRevision {
					t.Fatalf("qualified compatibility record %x: got %x revision=%d error=%v", expected.key, got, revision, err)
				}
			}
		})
	}
}
