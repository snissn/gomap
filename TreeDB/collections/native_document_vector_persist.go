package collections

import (
	"errors"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commandwalapply"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/txnhistory"
)

// Explicit Save/Rebuild owns one complete rebuild under the original captured
// transaction. It preserves the v5 source counter and uses the original103
// names-only frame. Ordinary CRUD instead stages only touched graph records.
func (idx *VectorIndex) saveNativeDocumentVector() (status VectorIndexLoadStatus, resultErr error) {
	c := idx.collection
	tx, err := c.nativeJSON.Begin(c.name)
	if err != nil {
		return status, err
	}
	defer func() { resultErr = errors.Join(resultErr, tx.Close()) }()
	g := &tx.bindings[0]
	def, ok := findVectorIndex(g.probe.meta.VectorIndexes, idx.name)
	if !ok {
		return status, ErrIndexNotFound
	}
	if g.probe.meta.nativeDocumentCoverageVersion != nativeDocumentVectorCoverageVersion || idx.validateNativeSnapshotDefinition(def) != "" {
		return status, backenddb.ErrNativeSemanticUnsupported
	}
	base, _ := tx.core.State().(*nativeEngineVersion)
	if base == nil {
		base = tx.imported
	}
	generation, err := nativeDocumentGenerationAtRead(&g.probe, base)
	if err != nil {
		return status, err
	}
	primary, _ := g.probe.collection.domains.Get("primary")
	read, err := newNativeDocumentGraphRead(&g.probe, primary)
	if err != nil {
		return status, err
	}
	admit := func(n uint64) error { return tx.admitStage(g, n) }
	entries, err := buildNativeDocumentGraph(g.probe.meta, g.incarnation, generation, def, read.walk, admit)
	read.Close()
	read = nil
	if err != nil {
		return status, err
	}
	if err = admit(memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeJSONLogicalInsert{}))) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeJSONDomainChange{}))) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(txnhistory.Effect{}))) + uint64(len(g.name)+len(def.Name)+len(def.Field))*16 + 512); err != nil {
		return status, err
	}
	schema, err := nativeDocumentGraphSchema(def)
	if err != nil {
		return status, err
	}
	domainName := "vector-index/" + idx.name
	policy, err := collectionRootStoragePolicyForDB(tx.db, g.probe.meta, collectionVectorIndexRootName(g.name, idx.name))
	if err != nil {
		return status, err
	}
	g.plan = &nativeJSONLogicalInsert{name: g.name, incarnation: g.incarnation, custody: tx.custody, expectedCollection: g.probe.collection, documentGraph: true, additional: []nativeJSONDomainChange{{name: domainName, policy: policy, entries: entries, fullReplacement: true, documentGraph: nativeDocumentGraphAuthority{incarnation: g.incarnation, generation: generation, schema: schema, checked: true}}}}
	g.effects = []txnhistory.Effect{{Namespace: txnhistory.Namespace{Kind: txnhistory.CatalogNamespace}, Kind: txnhistory.PointEffect, Key: []byte(g.name)}}
	payload, err := commitlog.EncodeCollectionRebuildVectorIndexPayload(g.name, idx.name)
	if err != nil {
		return status, err
	}
	collectionName := g.name
	apply := nativeCommandWALApplyCommit{frame: commandwalapply.LoweredFrame{Kind: commitlog.CommandKindCollectionRebuildVectorIndex, Scope: commitlog.CommandScopeCollection, PayloadFormat: commitlog.PayloadFormatCollectionRebuildVectorIndexV1, Payload: payload}, sync: true}
	if err = tx.commitNativeApply(false, &apply); err != nil {
		if apply.result.LogicalInstalled {
			err = commitAmbiguousError("document vector durable completion", err)
		}
		return status, err
	}
	status.Loaded = true
	status.RootName = collectionVectorIndexRootName(collectionName, idx.name)
	return status, nil
}
func (c *Collection) rebuildNativeDocumentVector(name string) (out VectorIndexStatus, resultErr error) {
	scope, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		return out, err
	}
	def, ok := findVectorIndex(scope.meta.VectorIndexes, name)
	err = scope.Close()
	if err != nil {
		return out, err
	}
	if !ok {
		return out, ErrIndexNotFound
	}
	idx, err := newVectorIndex(c, vectorIndexOptionsFromDefinition(def))
	if err != nil {
		return out, err
	}
	_, err = idx.saveNativeDocumentVector()
	if err != nil {
		return out, err
	}
	return c.nativeDocumentVectorStatus(name)
}
func (c *Collection) nativeDocumentVectorStatus(name string) (out VectorIndexStatus, resultErr error) {
	scope, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		return out, err
	}
	defer func() { resultErr = errors.Join(resultErr, scope.Close()) }()
	session, err := openNativeDocumentGraphSession(scope, name)
	if err != nil {
		return out, err
	}
	defer session.close()
	def, _ := findVectorIndex(scope.meta.VectorIndexes, name)
	stats, err := session.graph.stats()
	if err != nil {
		return out, err
	}
	return VectorIndexStatus{Name: name, Definition: def, Strategy: VectorIndexStrategyNativeRuntime, Loaded: true, NativeRootLoaded: true, RootName: collectionVectorIndexRootName(c.name, name), Stats: stats}, nil
}
