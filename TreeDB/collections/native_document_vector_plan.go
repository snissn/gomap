package collections

import (
	"fmt"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/txnhistory"
	"github.com/snissn/gomap/TreeDB/node"
)

// An identity-free checked private delta carries only this fixed source tuple.
// It grants no storage access and is never inherited by generic domain writers.
type nativeDocumentGraphAuthority struct {
	incarnation uint64
	generation  uint64
	schema      [32]byte
	checked     bool
}

func (t *NativeJSONTxn) finalizeNativeDocumentVectors(g *nativeJSONTxnBinding) error {
	if g.probe.meta.nativeDocumentCoverageVersion != nativeDocumentVectorCoverageVersion {
		return nil
	}
	if g.plan == nil || g.probe.collection == nil {
		return fmt.Errorf("collections: native graph requires final document plan")
	}
	base, _ := t.core.State().(*nativeEngineVersion)
	if base == nil {
		base = t.imported
	}
	old, err := nativeDocumentGenerationAtRead(&g.probe, base)
	if err != nil {
		return err
	}
	next, err := nativeDocumentGenerationAfter(old, &g.rows)
	if err != nil {
		return err
	}
	if next == old {
		return nil
	}
	admit := func(n uint64) error { return t.admitStage(g, n) }
	if err = admitNativeInsertArray(len(g.plan.additional)+len(g.probe.meta.VectorIndexes), uint64(unsafe.Sizeof(nativeJSONDomainChange{})), admit); err != nil {
		return err
	}
	additional := make([]nativeJSONDomainChange, len(g.plan.additional), len(g.plan.additional)+len(g.probe.meta.VectorIndexes))
	copy(additional, g.plan.additional)
	for _, def := range g.probe.meta.VectorIndexes {
		if err = admit(uint64(len(def.Name)+len(def.Field))*8 + 512); err != nil {
			return err
		}
		fingerprint, err := nativeDocumentGraphSchema(def)
		if err != nil {
			return err
		}
		name := "vector-index/" + def.Name
		domain, _ := g.probe.collection.domains.Get(name)
		read, err := newNativeDocumentGraphRead(&g.probe, domain)
		if err != nil {
			return err
		}
		// Private graph scratch dies before this borrowed domain/read scope closes.
		mutations, err := func() ([]memtable.COWMutation, error) {
			graph, err := newNativeDocumentGraph(def, g.incarnation, old, read.get, admit)
			if err != nil {
				return nil, err
			}
			graph.scanNode = read.scanNode
			if domain != nil && !domain.documentGraph.valid(g.incarnation, old, fingerprint) {
				if err = graph.validateCold(read.scanNode, read.walk); err != nil {
					return nil, err
				}
			}
			if err = nativeDocumentGraphApplyRows(graph, g.probe.meta, &g.rows, admit); err != nil {
				return nil, err
			}
			return graph.mutations(next)
		}()
		read.Close()
		read = nil
		if err != nil {
			return err
		}
		additional = append(additional, nativeJSONDomainChange{name: name, entries: mutations, documentGraph: nativeDocumentGraphAuthority{incarnation: g.incarnation, generation: next, schema: fingerprint, checked: true}})
	}
	key, err := nativeDocumentGenerationKey(g.incarnation)
	if err != nil {
		return err
	}
	if err = admit(memtable.COWAllocationCharge(uint64(len(key))) + memtable.COWAllocationCharge(8) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWMutation{})))); err != nil {
		return err
	}
	g.plan.additional = additional
	g.plan.expectedCollection = g.probe.collection
	g.plan.catalogEffects = []memtable.COWMutation{{Key: []byte(key), Value: encodeRootID(next), Flags: node.FlagInline}}
	// Original catalog conflict authority binds all graph/counter effects to
	// the same source incarnation/schema, in addition to point document OCC.
	if err = admit(memtable.COWAllocationCharge(uint64(len(g.effects)+1)*uint64(unsafe.Sizeof(txnhistory.Effect{}))) + memtable.COWAllocationCharge(uint64(len(g.name)))); err != nil {
		return err
	}
	effects := make([]txnhistory.Effect, len(g.effects), len(g.effects)+1)
	copy(effects, g.effects)
	g.effects = append(effects, txnhistory.Effect{Namespace: txnhistory.Namespace{Kind: txnhistory.CatalogNamespace}, Kind: txnhistory.PointEffect, Key: []byte(g.name)})
	return nil
}
