package collections

import (
	"errors"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// These tests use the actual saved record families, original capture allocator
// and shared codec. They never install the private comparison runtime.
func TestNativeHNSWGraphPhaseCustody(t *testing.T) {
	for _, refuse := range []bool{false, true} {
		name := "success"
		if refuse {
			name = "capacity"
		}
		t.Run(name, func(t *testing.T) {
			c, closeAll, _, def, _ := openHNSWPublicFixture(t, "native", 32, 2)
			var budget nativeHNSWAdmissionBudget
			t.Cleanup(func() {
				closeAll()
				if budget != nil {
					if s := budget.Stats(); s.TotalBytes != 0 || s.ExternalLeases != 0 {
						t.Errorf("full Close: %+v", s)
					}
				}
			})
			original, err := c.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
			if err != nil {
				t.Fatal(err)
			}
			if _, err = original.SaveSnapshot(); err != nil {
				t.Fatal(err)
			}
			read, err := c.nativeJSON.Capture(c.name)
			if err != nil {
				t.Fatal(err)
			}
			defer read.Close()
			budget = read.capture.NativeAllocator().(nativeHNSWAdmissionBudget)
			def, _ = findVectorIndex(read.meta.VectorIndexes, def.Name)
			def, err = normalizeVectorIndexDefinition(def)
			if err != nil {
				t.Fatal(err)
			}
			lease, err := read.capture.NativeAllocator().AdmitNativeBytes(64 << 10)
			if err != nil {
				t.Fatal(err)
			}
			defer lease.Close()
			charge := uint64(64 << 10)
			admit := func(n uint64) error {
				if n > ^uint64(0)-charge {
					return memtable.ErrCOWCapacity
				}
				if err := lease.Resize(charge + n); err != nil {
					return err
				}
				charge += n
				return nil
			}
			meta, _, reason, err := readNativeHNSWMeta(read, def, admit)
			if err != nil || reason != "" {
				t.Fatalf("meta %q: %v", reason, err)
			}
			floor := charge
			if floor <= 64<<10 {
				t.Fatal("metadata did not reserve its complete promise")
			}
			beforeSnapshot, _ := original.persistSnapshot()
			beforeRuntimeCharge := original.nativeHNSWCharge
			if refuse {
				// Exhaust the real remaining in-flight tranche after metadata.
				// The phase may not credit metadata to admit even its first step.
				blocker, err := read.capture.NativeAllocator().AdmitNativeBytes(0)
				if err != nil {
					t.Fatal(err)
				}
				defer blocker.Close()
				available := budget.Limits().MaxInFlightBytes - budget.Stats().ReservedBytes
				if err = blocker.Resize(available); err != nil {
					t.Fatal(err)
				}
				before := budget.Stats()
				loaded, _, reason, err := c.loadNativeHNSWGraphPhase(read, def, meta, &charge, admit)
				if !errors.Is(err, memtable.ErrCOWCapacity) || reason != "" || loaded != nil {
					t.Fatalf("real refusal loaded=%p reason=%q err=%v", loaded, reason, err)
				}
				if charge != floor {
					t.Fatalf("refusal changed ledger %d -> %d", floor, charge)
				}
				after := budget.Stats()
				if after.ReservedBytes > before.ReservedBytes || after.ExternalLeases > before.ExternalLeases {
					t.Fatalf("refusal retained added resources: before=%+v after=%+v", before, after)
				}
				if c.registeredVectorIndex(def.Name) != original || original.nativeHNSWCharge != beforeRuntimeCharge {
					t.Fatal("refusal changed the registered runtime")
				}
				blocker.Close()
			}
			loaded, disk, reason, err := c.loadNativeHNSWGraphPhase(read, def, meta, &charge, admit)
			if err != nil || reason != "" {
				t.Fatalf("graph %q: %v", reason, err)
			}
			defer loaded.releaseNativeHNSW()
			if disk <= 0 || charge <= floor {
				t.Fatalf("graph disk=%d charge=%d floor=%d", disk, charge, floor)
			}
			loadedSnapshot, _ := loaded.persistSnapshot()
			if !reflect.DeepEqual(beforeSnapshot.Nodes, loadedSnapshot.Nodes) || !reflect.DeepEqual(beforeSnapshot.DocMap, loadedSnapshot.DocMap) {
				t.Fatal("shared codec changed owned nodes or document map")
			}
			before := budget.Stats()
			all := charge
			runtime := loaded.nativeHNSWCharge
			if err = completeNativeHNSWGraphPhase(lease, &charge, floor); err != nil {
				t.Fatal(err)
			}
			after := budget.Stats()
			if charge != floor || before.ReservedBytes-after.ReservedBytes != all-floor || before.ExternalBytes-after.ExternalBytes != all-floor || before.ExternalLeases != after.ExternalLeases {
				t.Fatalf("inexact original-receipt credit: all=%d floor=%d before=%+v after=%+v ledger=%d", all, floor, before, after, charge)
			}
			if loaded.name != meta.Name || loaded.partitionPreparation != meta.Preparation || loaded.nativeHNSWCharge != runtime {
				t.Fatal("metadata/runtime custody changed at scratch boundary")
			}
			if err = admit(memtable.COWAllocationCharge(513)); err != nil {
				t.Fatal(err)
			}
			if charge != floor+memtable.COWAllocationCharge(513) {
				t.Fatal("later growth restored graph credit")
			}
			again, _ := loaded.persistSnapshot()
			if !reflect.DeepEqual(loadedSnapshot.Nodes, again.Nodes) || !reflect.DeepEqual(loadedSnapshot.DocMap, again.DocMap) {
				t.Fatal("later scratch growth changed final runtime backing")
			}
			// An actual cold public Load infers caller-omitted dimensions from
			// the canonical declaration/metadata and records its schema identity.
			// Canonical zero dimensions themselves are rejected by normalization.
			c.UnregisterVectorIndex(def.Name)
			reloaded, status, err := c.LoadVectorIndexSnapshot(VectorIndexOptions{Name: def.Name})
			if err != nil || !status.Loaded {
				t.Fatalf("inferred public Load: %+v %v", status, err)
			}
			if reloaded.dimensions != meta.Dimensions || reloaded.schemaGeneration != def.SchemaGeneration {
				t.Fatalf("inferred definition dimensions=%d schema=%d want=%d/%d", reloaded.dimensions, reloaded.schemaGeneration, meta.Dimensions, def.SchemaGeneration)
			}
			t.Logf("post-meta floor=%d graph=%d retained runtime=%d", floor, all-floor, runtime)
		})
	}
}

func TestNativeHNSWGraphPhaseFailureKeepsLedger(t *testing.T) {
	// Pin every setup cut: unrelated retirement must not mask the private
	// runtime receipt or change the exact post-failure accounting baseline.
	// The fixture retains the full-Close zero-bytes/zero-leases assertion.
	c, def, budget := openPinnedNativeHNSWAdmissionFixture(t, []byte("row-00000"))
	idx, err := c.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = idx.SaveSnapshot(); err != nil {
		t.Fatal(err)
	}
	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	def, _ = findVectorIndex(read.meta.VectorIndexes, def.Name)
	def, err = normalizeVectorIndexDefinition(def)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := read.capture.NativeAllocator().AdmitNativeBytes(64 << 10)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	charge := uint64(64 << 10)
	admit := func(n uint64) error {
		if n > ^uint64(0)-charge {
			return memtable.ErrCOWCapacity
		}
		if err := lease.Resize(charge + n); err != nil {
			return err
		}
		charge += n
		return nil
	}
	meta, _, reason, err := readNativeHNSWMeta(read, def, admit)
	if err != nil || reason != "" {
		t.Fatalf("meta %q: %v", reason, err)
	}
	floor := charge
	baseline := budget.Stats()
	stop := errors.New("graph read admission stopped")
	failing := func(n uint64) error {
		// Census cursor backing was already admitted; stop only once the
		// independent private runtime receipt also exists, during record work.
		if budget.Stats().ExternalLeases > baseline.ExternalLeases {
			return stop
		}
		return admit(n)
	}
	loaded, _, _, err := c.loadNativeHNSWGraphPhase(read, def, meta, &charge, failing)
	if loaded != nil {
		defer loaded.releaseNativeHNSW()
	}
	if !errors.Is(err, stop) || loaded != nil || charge <= floor {
		t.Fatalf("failed phase: %v loaded=%p charge=%d floor=%d", err, loaded, charge, floor)
	}
	after := budget.Stats()
	if after.ExternalLeases != baseline.ExternalLeases || after.ReservedBytes-baseline.ReservedBytes != charge-floor {
		t.Fatalf("partial runtime cleanup or scratch refund: before=%+v after=%+v ledger=%d floor=%d", baseline, after, charge, floor)
	}
	// No completion is allowed on error: earlier successful increments stay
	// charged until the original receipt closes. Shrink failure also leaves the
	// caller's ledger untouched rather than allowing later Resize to restore it.
	all := charge
	lease.Close()
	if err = completeNativeHNSWGraphPhase(lease, &charge, floor); !errors.Is(err, memtable.ErrCOWClosed) || charge != all {
		t.Fatalf("closed original receipt: charge=%d want=%d err=%v", charge, all, err)
	}
	if err = completeNativeHNSWGraphPhase(nil, &charge, all+1); !errors.Is(err, memtable.ErrCOWCapacity) || charge != all {
		t.Fatalf("invalid floor changed ledger: charge=%d err=%v", charge, err)
	}
}
