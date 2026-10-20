package collections

import (
	"bytes"
	"encoding/binary"
	"errors"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// Admission for this bounded delta uses actual leaf bytes/proof counts. The
// original sixteen-copy manifest envelope includes record headers, maps and
// coexisting encoder/mutation/requirements backing; no prior source leaf is kept.
func nativeSourceDirectoryDeltaCapacityV2(p *sourceImportPublicationV2) (uint64, error) {
	if p == nil {
		return 0, backenddb.ErrNativeSemanticUnsupported
	}
	var total uint64
	add := func(key, value uint64) error {
		n := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(columnManifestRecord{}))) + memtable.COWAllocationCharge(key) + memtable.COWAllocationCharge(value) + preparedSemanticStreamMapEntryReserveBytes
		n, e := nativeTypedCheckedMul(n, 16)
		if e != nil {
			return e
		}
		return nativeTypedCheckedAdd(&total, n)
	}
	prefix := uint64(len(sourceImportDirectoryPrefixV2(p.progressKey)))
	if e := add(prefix+lenSourceChunkKeyV2, 8+32); e != nil {
		return 0, e
	}
	for off := 0; off < len(p.leafBytes); off += columnSourceDirectoryPageBytesV2 {
		if e := add(prefix+lenSourceChunkKeyV2, uint64(min(columnSourceDirectoryPageBytesV2, len(p.leafBytes)-off))); e != nil {
			return 0, e
		}
	}
	for range p.proofNodes {
		if e := add(prefix+uint64(len("node/"))+16, 32); e != nil {
			return 0, e
		}
	}
	// Directory frontier emits at most 64 carry nodes and two fixed headers.
	for range 64 {
		if e := add(uint64(len(columnSourceDirectoryNodePrefixV2))+16, 32); e != nil {
			return 0, e
		}
	}
	for range 2 {
		if e := add(uint64(len(columnSourceDirectoryLeafPrefixV2))+16, columnSourceDirectoryHeaderBytesV2); e != nil {
			return 0, e
		}
	}
	for _, kv := range [][2]uint64{{uint64(len(p.progressKey)), uint64(len(p.next))}, {uint64(len(p.receiptKey)), 32}, {uint64(len(p.bindingKey)), uint64(len(p.binding))}, {uint64(len(p.progressKey) + len(":seal")), sourceImportSealBytesV2}} {
		if e := add(kv[0], kv[1]); e != nil {
			return 0, e
		}
	}
	return total, nil
}

const lenSourceChunkKeyV2 = uint64(len("chunk/")) + 16

func (w *nativeTypedMaterialization) buildSourceImportSystemDelta(p *sourceImportPublicationV2) error {
	delta := batch.NewBoundedRetainingLargeEntries(nil, int(^uint(0)>>1), nativeBuildBatchEntries)
	defer delta.CloseWithoutPooling()
	if p.completed.Digest != ([32]byte{}) && len(p.seal) != sourceImportSealBytesV2 {
		return errors.New("collections: completed source import lacks directory seal")
	}
	// Four entries fit the original bounded batch, with original canonical keys.
	entries := []struct {
		key   string
		value []byte
	}{{p.progressKey, p.next}, {p.receiptKey, p.receipt[:]}, {p.bindingKey, p.binding}}
	if p.completed.Digest != ([32]byte{}) {
		entries = append(entries, struct {
			key   string
			value []byte
		}{p.progressKey + ":seal", p.seal})
	}
	// Original Set owns sorted root mutation handling; no current-state recapture.
	for _, entry := range entries {
		if e := delta.Set([]byte(entry.key), entry.value); e != nil {
			return e
		}
	}
	return w.input.group.BuildNativeSystemDelta(delta)
}

func (w *nativeTypedMaterialization) finishSourceDirectoryDelta(plan ColumnPublishPlan, d *columnSourceDirectoryAppendV2) error {
	mutation, e := rootpublication.MergeStableLogicalObligationMutations(w.appendMutation, plan.durableResourceMutation)
	if e != nil {
		return e
	}
	w.appendMutation = mutation
	w.requirementWork.Add(plan.durableResourceRequirementWork)
	// Keep only fresh original asset records as the fallback suffix. The original
	// cdir2 metadata/leaf/proof pages are not closure requirements.
	for _, r := range plan.RootDelta.Records {
		if stableColumnManifestDurableRequirementRecord(r.key) {
			w.records = append(w.records, r)
		}
		if bytes.Equal(r.key, []byte(columnSourceDirectoryHeaderKeyV2)) {
			d.previous, e = decodeColumnSourceDirectoryHeaderV2(r.value)
			if e != nil {
				return e
			}
		}
		if bytes.Equal(r.key, []byte(columnSourceDirectoryActiveSegmentKeyV2)) {
			if len(r.value) < 4 {
				return backenddb.ErrNativeSemanticUnsupported
			}
			file := binary.BigEndian.Uint32(r.value[:4])
			marker, x := decodeColumnManifestSegmentOwnershipForScan(columnManifestSegmentOwnershipRecordKey(file), r.value[4:], w.meta.Options.ColumnStore.AssetManager.Namespace, plan.UpdatedActiveManifest.Generation)
			if x != nil {
				return x
			}
			d.activeSegment = &marker
		}
	}
	requirements, e := nativeSourceManifestRequirementsV2(w.input.snapshot, w.input.baseManifestRoot, plan.UpdatedActiveManifest.Generation, w.meta.Options.ColumnStore.AssetManager.Namespace, w.records, w.meta.VectorIndexes...)
	if e != nil {
		return e
	}
	w.requirements = requirements
	// Same source object, not another historical array or engine ancestor. Group
	// cleanup owns the borrowed scope through its synchronous exact verification.
	w.fallback = func() (rootpublication.StableLogicalObligationRequirements, rootpublication.StableResourceClosureWork, error) {
		return requirements, rootpublication.StableResourceClosureWork{}, nil
	}
	return nil
}
