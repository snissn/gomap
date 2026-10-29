package collections

import (
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// Only independently cloned domain output escapes this private construction
// scope. The catalog, callback, analyzers, prepared states, reducer maps and
// unpooled tables end when buildNativeTextPhase returns (including table reset
// and iterator Close). This is allocation custody, not an immediate-GC promise.
// The opening/current row bytes and staged template resolver belong to the
// preexisting binding. Neither is closed, reset, nor credited here.
type nativeTextPhase struct {
	domains                    []nativeJSONDomainChange
	before, admitted, retained uint64
}

func (t *NativeJSONTxn) buildNativeTextPhase(g *nativeJSONTxnBinding) (nativeTextPhase, error) {
	phase := nativeTextPhase{before: g.stageCharge}
	// Admit the callback environment and its captured counter before constructing
	// the bound catalog. They do not escape with the independently owned output.
	control := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(struct {
		entry   uintptr
		txn     *NativeJSONTxn
		binding *nativeJSONTxnBinding
		counter *uint64
	}{}))) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(uint64(0))))
	if err := t.admitStage(g, control); err != nil {
		return nativeTextPhase{}, err
	}
	admitted := control
	admit := func(n uint64) error {
		if n > ^uint64(0)-admitted {
			return memtable.ErrCOWCapacity
		}
		if err := t.admitStage(g, n); err != nil {
			return err
		}
		admitted += n
		return nil
	}
	catalog, err := newNativeTextCatalog(&g.probe, admit)
	if err != nil {
		return nativeTextPhase{}, err
	}
	// The exact mutation state/stat readers replace only their own record envelope.
	// All other records, iterators, inspections and query catalogs are unchanged.
	catalog.nativeText.mutationCodecs = true
	phase.domains, err = nativeTextMutationDomains(g.probe.capture.Snapshot(), catalog, g.rows.rows, g.templates, admit)
	if err != nil {
		return nativeTextPhase{}, err
	}
	phase.admitted = admitted
	phase.retained, err = nativeTextPhaseRetained(phase.domains, uint64(len(g.name)))
	if err != nil {
		return nativeTextPhase{}, err
	}
	return phase, nil
}

// Names have already been made collection-relative with TrimPrefix; the full
// qualified string backing still escapes. Keys/values are independent clones,
// not aliases into reset tables. Count actual backing capacities, independently
// rounded for each allocation, rather than logical entry or payload lengths.
func nativeTextPhaseRetained(domains []nativeJSONDomainChange, collectionBytes uint64) (uint64, error) {
	var retained nativeTextAdmissionSum
	retained.array(uint64(cap(domains)), uint64(unsafe.Sizeof(nativeJSONDomainChange{})))
	for _, domain := range domains {
		nameBytes := uint64(len(domain.name))
		if collectionBytes == ^uint64(0) || nameBytes > ^uint64(0)-collectionBytes-1 {
			return 0, memtable.ErrCOWCapacity
		}
		retained.array(collectionBytes+1+nameBytes, 1)
		retained.array(uint64(cap(domain.entries)), uint64(unsafe.Sizeof(memtable.COWMutation{})))
		for _, entry := range domain.entries {
			retained.array(uint64(cap(entry.Key)), 1)
			retained.array(uint64(cap(entry.Value)), 1)
		}
	}
	if retained.bad {
		return 0, memtable.ErrCOWCapacity
	}
	return retained.total, nil
}

func (t *NativeJSONTxn) completeNativeTextPhase(g *nativeJSONTxnBinding, phase nativeTextPhase) error {
	if phase.admitted > ^uint64(0)-phase.before || g.stageCharge < phase.before+phase.admitted || phase.retained > phase.admitted {
		return memtable.ErrCOWCapacity
	}
	// A borrowed staged resolver can admit directly on the original stage while
	// the reducer runs. Preserve ALL such increments, whether temporary or
	// retained, rather than assuming its callback/cache lifetime ended here.
	credit := phase.admitted - phase.retained
	remaining := g.stageCharge - credit
	index := 0
	for index < len(t.bindings) && &t.bindings[index] != g {
		index++
	}
	if index == len(t.bindings) || index >= len(t.stageLeases) || t.stageLeases[index] == nil {
		return memtable.ErrCOWClosed
	}
	if err := t.stageLeases[index].ShrinkNativeBytes(credit, remaining); err != nil {
		return err
	}
	g.stageCharge = remaining
	return nil
}
