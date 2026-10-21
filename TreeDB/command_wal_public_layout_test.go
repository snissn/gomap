package treedb

import (
	"errors"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func TestPublicCommandWALPayloadTailOrdinaryReuse(t *testing.T) {
	inner := &commandWALHookBatch{}
	wrapped := newCommandWALPublicBatch(nil, inner, 2)
	defer wrapped.Close()
	state := wrapped.commandWALPublicOrdinaryState
	if state == nil || wrapped.isCOW() {
		t.Fatal("ordinary wrapper has no payload owner")
	}
	key, value := []byte("key"), []byte("value")
	keyView, valueView, err := wrapped.SetViewWithReplayBytes(key, value)
	if err != nil {
		t.Fatal(err)
	}
	key[0], value[0] = 'x', 'x'
	if err := wrapped.Write(); err != nil {
		t.Fatal(err)
	}
	if string(keyView) != "key" || string(valueView) != "value" {
		t.Fatalf("payload views changed across Write: %q=%q", keyView, valueView)
	}
	wrapped.Reset()
	if wrapped.commandWALPublicOrdinaryState != state || wrapped.payload.Count() != 0 {
		t.Fatal("Reset replaced the payload owner or retained operations")
	}
	if err := wrapped.Delete([]byte("key")); err != nil {
		t.Fatal(err)
	}
	if err := wrapped.Close(); err != nil {
		t.Fatal(err)
	}
	if wrapped.payload.Count() != 0 || len(wrapped.payload.Payload()) != 0 {
		t.Fatal("Close retained payload state")
	}
	if err := wrapped.Set([]byte("closed"), nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("Set after Close: %v", err)
	}
}

func TestPublicCommandWALPayloadTailHandBuiltOrdinary(t *testing.T) {
	for _, resetFirst := range []bool{false, true} {
		inner := &commandWALNoResetBatch{}
		wrapped := &commandWALPublicBatch{inner: inner}
		if resetFirst {
			wrapped.Reset()
		}
		if err := wrapped.Set([]byte("key"), []byte("value")); err != nil {
			t.Fatal(err)
		}
		if wrapped.commandWALPublicOrdinaryState == nil || wrapped.payload.Count() != 1 {
			t.Fatal("ordinary method did not initialize its payload")
		}
		if err := wrapped.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPublicCommandWALPayloadTailCOWAdmissionAndClose(t *testing.T) {
	database, _, _ := cowPublicContractOpen(t, ProfileCommandWALDurable, func(o *Options) {
		o.ValueLog.Generational.Policy = ValueLogGenerationOff
		o.DisableBackgroundPrune = true
		o.BackgroundCheckpointInterval = -1
		o.BackgroundCheckpointIdleDuration = -1
		o.BackgroundIndexVacuumInterval = -1
		o.MaxWALBytes = -1
	})
	before := database.cached.COWMemoryStats()
	wrapped := newCommandWALPublicBatch(database, &commandWALNoResetBatch{}, 0)
	defer wrapped.Close()
	if !wrapped.isCOW() || wrapped.commandWALPublicOrdinaryState != nil {
		t.Fatal("COW wrapper allocated ordinary payload state")
	}
	charge := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(commandWALPublicCOWOwner{}))) +
		memtable.COWAllocationCharge(2*uint64(unsafe.Sizeof(uintptr(0)))) +
		memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	admitted := database.cached.COWMemoryStats()
	if admitted.TotalBytes != before.TotalBytes+charge || admitted.ReservedBytes != before.ReservedBytes+charge ||
		admitted.ExternalBytes != before.ExternalBytes+charge || admitted.ExternalLeases != before.ExternalLeases+1 {
		t.Fatalf("wrapper admission does not match its concrete owner: before=%+v admitted=%+v charge=%d", before, admitted, charge)
	}
	wrapped.Reset()
	if wrapped.commandWALPublicOrdinaryState != nil || !wrapped.payloadBypass {
		t.Fatal("COW Reset allocated an ordinary payload")
	}
	if err := wrapped.Close(); err != nil {
		t.Fatal(err)
	}
	if wrapped.commandWALPublicOrdinaryState != nil {
		t.Fatal("COW Close allocated an ordinary payload")
	}
	after := database.cached.COWMemoryStats()
	if after.TotalBytes != before.TotalBytes || after.ReservedBytes != before.ReservedBytes ||
		after.ExternalBytes != before.ExternalBytes || after.ExternalLeases != before.ExternalLeases {
		t.Fatalf("wrapper Close retained admission: before=%+v after=%+v", before, after)
	}
	t.Logf("wrapper bytes: cow=%d ordinary=%d payload=%d", unsafe.Sizeof(commandWALPublicCOWOwner{}),
		unsafe.Sizeof(commandWALPublicOrdinaryOwner{}), unsafe.Sizeof(commandWALPublicOrdinaryState{}))
}

func TestPublicCommandWALPayloadTailCOWWriteReuse(t *testing.T) {
	for _, profile := range []Profile{ProfileCommandWALDurable, ProfileCommandWALRelaxed} {
		t.Run(string(profile), func(t *testing.T) {
			database, _, _ := cowPublicContractOpen(t, profile)
			wrapped, ok := database.NewBatch().(*commandWALPublicBatch)
			if !ok {
				t.Fatal("COW command-WAL batch did not use its public wrapper")
			}
			defer wrapped.Close()
			for _, value := range []string{"first", "second"} {
				if err := wrapped.Set([]byte("key"), []byte(value)); err != nil {
					t.Fatal(err)
				}
				if err := wrapped.Write(); err != nil {
					t.Fatal(err)
				}
				wrapped.Reset()
				if wrapped.commandWALPublicOrdinaryState != nil {
					t.Fatal("COW mutation, Write or Reset allocated ordinary payload state")
				}
				got, err := database.Get([]byte("key"))
				if err != nil || string(got) != value {
					t.Fatalf("Get after Write: %q, %v", got, err)
				}
			}
			if err := wrapped.Close(); err != nil {
				t.Fatal(err)
			}
			if wrapped.commandWALPublicOrdinaryState != nil {
				t.Fatal("COW Close allocated ordinary payload state")
			}
		})
	}
}
