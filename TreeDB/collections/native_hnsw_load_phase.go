package collections

import (
	"fmt"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// loadNativeHNSWGraphPhase ends the census/record callbacks, encoded copies,
// seeded decode arrays and validation temporaries before its caller returns
// their credit. Only the final independently admitted runtime escapes. Metadata
// was decoded before this scope: its complete original promise stays live,
// including Name and Preparation backing retained by the maintained loader.
func (c *Collection) loadNativeHNSWGraphPhase(read *NativeJSONRead, def VectorIndexDefinition, meta vectorIndexPersistMeta, scratchCharge *uint64, admit func(uint64) error) (index *VectorIndex, disk int64, reason string, resultErr error) {
	counts, reason, err := inspectNativeHNSWRecordKeys(read, "vector-index/"+def.Name, admit)
	if err != nil || reason != "" {
		return nil, 0, reason, err
	}
	if reason = validateVectorIndexPersistShape(meta, counts.nodes, counts.edges, counts.tombs, counts.docs); reason != "" {
		return nil, 0, reason, nil
	}
	// Exact final node descriptors are allocated before the first record. Six
	// descriptor cohorts per row preserve the existing retained-growth contract;
	// each row later adds the remainder of the shared nativeHNSWRowCharge.
	rowDescriptors := uint64(6) * uint64(unsafe.Sizeof(vectorIndexNode{}))
	runtimeCharge := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(VectorIndex{}))) + 64<<10
	if uint64(counts.nodes) > (^uint64(0)-runtimeCharge)/rowDescriptors {
		return nil, 0, "", memtable.ErrCOWCapacity
	}
	runtimeCharge += uint64(counts.nodes) * rowDescriptors
	runtimeLease, err := read.capture.NativeAllocator().AdmitNativeBytes(runtimeCharge)
	if err != nil {
		return nil, 0, "", err
	}
	retained := false
	var owned *VectorIndex
	defer func() {
		if !retained {
			if owned != nil && owned.nativeHNSWLease != nil {
				owned.releaseNativeHNSW()
			} else {
				runtimeLease.Close()
			}
		}
	}()
	index, err = newVectorIndexWithMeta(c, vectorIndexOptionsFromDefinition(def), read.meta)
	if err != nil {
		return nil, 0, "", err
	}
	owned = index
	index.nativeHNSWLease = runtimeLease
	index.nativeHNSWCharge = runtimeCharge
	// Cold conversion performs no insertion traversal and creates no insert
	// scratch. Native mutation/fold admission grows this high-water term before
	// insertion workers allocate anything; no reserved graph storage is refunded.
	index.nativeHNSWConstructionPeak = 0
	def.Dimensions = meta.Dimensions
	graphDisk, reason, err := loadNativeHNSWRecords(index, read, def, meta, counts, admit)
	if err != nil {
		return nil, 0, "", fmt.Errorf("collections: native vector direct load (%d decode bytes, %d runtime bytes): %w", *scratchCharge, index.nativeHNSWCharge, err)
	}
	if reason != "" {
		return nil, 0, reason, nil
	}
	retained = true
	return index, graphDisk, "", nil
}

// completeNativeHNSWGraphPhase returns only the completed graph phase's exact
// increments on the SAME original receipt. The full post-meta floor remains;
// runtime, read/capture, schema and physical decoder receipts are independent.
// Call only after successful graph construction and all graph helper aliases
// have ended. Error paths keep their complete original promise until cleanup.
func completeNativeHNSWGraphPhase(lease *memtable.COWExternalLease, charge *uint64, floor uint64) error {
	if *charge < floor {
		return memtable.ErrCOWCapacity
	}
	credit := *charge - floor
	if err := lease.ShrinkNativeBytes(credit, floor); err != nil {
		return err
	}
	*charge = floor
	return nil
}
