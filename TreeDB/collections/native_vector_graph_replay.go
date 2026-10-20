package collections

import (
	"encoding/binary"
	"errors"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

// Startup executes the original assigned103 handler at its current physical
// predecessor. No foreground logical recipe, new LSN, or native engine exists.
func replayNativeVectorGraph(db *backenddb.DB, env commitlog.CommandEnvelope, intent *backenddb.CommandWALIntent, meta CollectionMeta, name string) (resultErr error) {
	snap := db.AcquireSnapshot()
	if snap == nil {
		return backenddb.ErrClosed
	}
	defer func() { resultErr = errors.Join(resultErr, snap.Close()) }()
	catalog, err := loadCollectionCatalog(snap, meta.Name)
	if err != nil {
		return err
	}
	if catalog == nil {
		return errCollectionNotFound
	}
	meta = catalog.meta
	def, ok := findVectorIndex(meta.VectorIndexes, name)
	if !ok {
		return ErrIndexNotFound
	}
	if vectorIndexUsesCosineNormalizedF32V1(def) {
		return backenddb.ErrNativeSemanticUnsupported
	}
	root := catalog.rootID(collectionColumnManifestRootName(meta.Name))
	h, err := nativeVectorGraphHeader(meta, snap, root)
	if err != nil {
		return err
	}
	capacity, err := nativeVectorGraphCapacity(meta, snap, root, db.ColumnAssetRootDir(), def, h, func(uint64) error { return nil })
	if err != nil {
		return err
	}
	recipe := &nativeVectorGraphRecipe{def: def, identity: *meta.Options.ColumnStore.ActiveManifest, digest: h.Digest, rows: h.Rows}
	d := &nativeTypedDelta{data: &nativeTypedRows{graph: recipe}, appliedLSN: env.LSN, buildCapacity: capacity}
	capture, err := db.AcquireStableResourceCaptureLease()
	if err != nil {
		return err
	}
	defer capture.Release()
	group, err := db.BeginRootPublicationBuildGroupBorrowingLease(snap, capture)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, group.Close()) }()
	work, err := prepareNativeTypedReplayMaterialization(nativeTypedMaterializeInput{meta: meta, baseManifestRoot: root, deltas: []*nativeTypedDelta{d}, snapshot: snap, capture: capture, group: group})
	if work != nil {
		defer func() { done, e := work.release(); done.Complete(); resultErr = errors.Join(resultErr, e) }()
	}
	if err != nil {
		return err
	}
	if err = work.materialize(); err != nil {
		return err
	}
	if err = group.SetNativeDurableResources(work.resources, work.requirements); err != nil {
		return err
	}
	work.resources = nil
	system := batch.New(nil, int(^uint(0)>>1))
	defer system.Close()
	for _, r := range work.roots {
		var raw [8]byte
		binary.BigEndian.PutUint64(raw[:], r.root)
		if err = system.Set([]byte(systemCollectionRootKey(meta.Name+"/"+r.name)), raw[:]); err != nil {
			return err
		}
	}
	encoded, err := encodeCollectionMeta(work.meta)
	if err != nil {
		return err
	}
	if err = system.Set([]byte(systemCollectionMetaKey(meta.Name)), encoded); err != nil {
		return err
	}
	if err = group.BuildNativeSystemDelta(system); err != nil {
		return err
	}
	physical := db.NewPhysicalBatch().(*backenddb.Batch)
	defer func() { resultErr = errors.Join(resultErr, physical.Close()) }()
	if err = physical.AttachVectorGraphReplayGroup(intent, group); err != nil {
		return err
	}
	return physical.WriteSync()
}
