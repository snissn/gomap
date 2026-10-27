package collections

import (
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// nativeColumnGraphConstructionPlan prices only the maintained, untraced,
// c=nil parallel builder with exactly pre-sized source rows and node storage.
// CallerRowsBytes survives its return. PrivateBytes ends at that return: workers
// are joined, and neither source vectors nor fresh uint32 adjacency alias the
// private index. Source mappings, captured records/schema, Config/options,
// dictionaries, assets and stable authority are EXCLUDED and stay with their
// original caller owners. TotalBytes is not the complete graph recipe promise.
type nativeColumnGraphConstructionPlan struct {
	Workers                                                              int // zero keeps the original uncapped reciprocal bound
	Rows, IDBytes                                                        uint64
	LevelRows                                                            [33]uint64
	SourceRowsBytes, SourceIDBytes, OutputAdjacencyBytes                 uint64
	CallerRowsBytes                                                      uint64
	NativeRowsBytes, SearchScratchBytes, SerialScratchBytes, MatrixBytes uint64
	PlanningBytes, CommitBytes, LocalityBytes, ControlBytes              uint64
	InsertionBytes, RemapBytes, PrivateBytes, TotalBytes                 uint64
}

type nativeColumnGraphCapacitySum struct {
	bytes uint64
	err   error
}

func (s *nativeColumnGraphCapacitySum) add(n uint64) {
	if s.err == nil {
		s.err = nativeTypedCheckedAdd(&s.bytes, n)
	}
}

func (s *nativeColumnGraphCapacitySum) allocation(count, size uint64) {
	if s.err != nil {
		return
	}
	n, err := nativeTypedCheckedMul(count, size)
	if err != nil {
		s.err = err
		return
	}
	charge := memtable.COWAllocationCharge(n)
	if charge == ^uint64(0) {
		s.err = memtable.ErrCOWCapacity
		return
	}
	s.add(charge)
}

func (s *nativeColumnGraphCapacitySum) copies(copies, count, size uint64) {
	var one nativeColumnGraphCapacitySum
	one.allocation(count, size)
	if one.err != nil {
		s.err = one.err
		return
	}
	n, err := nativeTypedCheckedMul(copies, one.bytes)
	if err != nil {
		s.err = err
		return
	}
	s.add(n)
}

// walkIDs visits the original borrowed IDs once, in insertion order, without
// retaining them. It must report exactly rows IDs. No fake values or N-element
// planning carrier is needed. The caller must bind the same immutable source
// and use the ordinary parallel wrapper with nil trace/policy/observer. This
// deliberately rejects other shapes so they can keep their existing promise.
func nativeColumnGraphConstructionCapacity(def VectorIndexDefinition, rows uint64, walkIDs func(func([]byte) error) error) (nativeColumnGraphConstructionPlan, error) {
	return nativeColumnGraphConstructionCapacityForWorkers(def, rows, 0, walkIDs)
}

// A positive worker ceiling must be captured before admission and bound to the
// actual offline index for every planning and reciprocal batch. Zero preserves
// the conservative unbound caller contract. Batch width and topology stay fixed.
func nativeColumnGraphConstructionCapacityForWorkers(def VectorIndexDefinition, rows uint64, workers int, walkIDs func(func([]byte) error) error) (nativeColumnGraphConstructionPlan, error) {
	var p nativeColumnGraphConstructionPlan
	if workers < 0 || workers > nativeVectorFrozenPrefixBatchWidth {
		return p, backenddb.ErrNativeSemanticUnsupported
	}
	p.Workers = workers
	if def.Name == "" || def.Field == "" || def.Strategy != VectorIndexStrategyColumnGraph || def.Representation != "" || def.M != 16 || def.EfConstruction != 128 || def.Metric != VectorMetricCosine || def.Encoding != VectorIndexEncodingFloat32 || def.Dimensions <= 0 || len(def.QuantizedIndexes) != 0 || walkIDs == nil {
		return p, backenddb.ErrNativeSemanticUnsupported
	}
	if rows > maxColumnVectorGraphAdjacencyOrdinal || rows > uint64(int(^uint(0)>>1)) || !vectorIndexNodeOrdinalsFitUint32(0, rows) {
		return p, memtable.ErrCOWCapacity
	}
	probe := columnGraphPartitionLiveCarrier{columnGraphHNSWBuilder: &columnGraphHNSWBuilder{name: def.Name, m: def.M}}
	var ids, nativeRows, adjacency nativeColumnGraphCapacitySum
	maxLevel := 0
	err := walkIDs(func(id []byte) error {
		if len(id) == 0 || p.Rows >= rows {
			return backenddb.ErrNativeSemanticUnsupported
		}
		level := probe.levelForDocumentID(id)
		if level < 0 || level >= len(p.LevelRows) {
			return backenddb.ErrNativeSemanticUnsupported
		}
		if err := nativeTypedCheckedAdd(&p.IDBytes, uint64(len(id))); err != nil {
			return err
		}
		ids.allocation(uint64(len(id)), 1)
		n, err := columnGraphHNSWRowCharge(def, id)
		if err != nil {
			return err
		}
		nativeRows.add(n)
		// Final row encoding: level zero has a plain list; upper layers add
		// magic/max-layer plus one length word per layer. Actual empty layers
		// can only reduce this bound.
		words := uint64(def.M*2 + level*def.M)
		if level != 0 {
			words += uint64(level + 3)
		}
		adjacency.allocation(words, uint64(unsafe.Sizeof(uint32(0))))
		if ids.err != nil {
			return ids.err
		}
		if nativeRows.err != nil {
			return nativeRows.err
		}
		if adjacency.err != nil {
			return adjacency.err
		}
		p.Rows++
		p.LevelRows[level]++
		maxLevel = max(maxLevel, level)
		return nil
	})
	if err != nil {
		return nativeColumnGraphConstructionPlan{}, err
	}
	if p.Rows != rows {
		return nativeColumnGraphConstructionPlan{}, backenddb.ErrNativeSemanticUnsupported
	}
	p.SourceIDBytes, p.NativeRowsBytes, p.OutputAdjacencyBytes = ids.bytes, nativeRows.bytes, adjacency.bytes
	var source nativeColumnGraphCapacitySum
	source.allocation(rows, uint64(unsafe.Sizeof(columnVectorGraphAssetRow{})))
	p.SourceRowsBytes = source.bytes
	source.add(p.SourceIDBytes)
	source.add(p.OutputAdjacencyBytes)
	if source.err != nil {
		return nativeColumnGraphConstructionPlan{}, source.err
	}
	p.CallerRowsBytes = source.bytes

	// M16 starts neighbor backing at exact limits32/16. In serial fallback,
	// one overflow grows then prune rewrites in place; the shared native row
	// envelope's mutable and two view backings cover this transition. Parallel
	// reciprocal candidates use the separately priced dot workers below.
	// Native descriptor/map/ID view reserves are retained conservatively; this
	// helper never lowers existing native HNSW admission.
	planWidth := uint64(0)
	if rows >= nativeVectorFrozenPrefixBatchMinimum {
		planWidth = min(rows, uint64(nativeVectorFrozenPrefixBatchWidth))
	}
	planningWorkers := planWidth
	if workers > 0 {
		planningWorkers = min(planningWorkers, uint64(workers))
	}
	// insertScratch remains owned even after first/serial fallback rows. The
	// bounded planning array is a distinct owner throughout all batch loops.
	search, err := columnGraphHNSWConstructionChargeForWorkers(def, rows, planningWorkers+1)
	if err != nil {
		return nativeColumnGraphConstructionPlan{}, err
	}
	p.SearchScratchBytes = search
	p.SerialScratchBytes, err = columnGraphHNSWConstructionCharge(def, rows)
	if err != nil {
		return nativeColumnGraphConstructionPlan{}, err
	}
	var planning nativeColumnGraphCapacitySum
	// The insertion wrapper's ID/vector slice-header carriers die with it.
	planning.copies(2, rows, uint64(unsafe.Sizeof([]byte{})))
	if planWidth != 0 {
		planning.allocation(nativeVectorFrozenPrefixBatchWidth, uint64(unsafe.Sizeof(vectorIndexSearchScratch{})))
	}
	planning.allocation(planWidth, uint64(unsafe.Sizeof(vectorIndexFrozenPrefixInsert{})))
	planning.allocation(planWidth, uint64(unsafe.Sizeof(error(nil))))
	planning.copies(planWidth, uint64(maxLevel+1), uint64(unsafe.Sizeof([]int{})))
	// Every plan retains one selected ordinal slice per populated layer.
	for layer := 0; layer <= maxLevel; layer++ {
		degree := uint64(def.M)
		if layer == 0 {
			degree *= 2
		}
		planning.copies(planWidth, degree, uint64(unsafe.Sizeof(int(0))))
	}
	if planning.err != nil {
		return nativeColumnGraphConstructionPlan{}, planning.err
	}
	p.PlanningBytes = planning.bytes

	var matrix nativeColumnGraphCapacitySum
	matrixElements, err := nativeTypedCheckedMul(rows, uint64(def.Dimensions))
	if err != nil {
		return nativeColumnGraphConstructionPlan{}, err
	}
	matrix.allocation(matrixElements, 4)
	if matrix.err != nil {
		return nativeColumnGraphConstructionPlan{}, matrix.err
	}
	p.MatrixBytes = matrix.bytes // in addition to all pre-transition row vectors

	// One frozen batch contributes at most width * sum(layer degree) links;
	// every group contains at least one link. Bound reciprocal workers by
	// groups, independent of a mutable GOMAXPROCS or sync.Pool implementation.
	degreeSum := uint64(def.M*2 + maxLevel*def.M)
	links, err := nativeTypedCheckedMul(planWidth, degreeSum)
	if err != nil {
		return nativeColumnGraphConstructionPlan{}, err
	}
	var commit nativeColumnGraphCapacitySum
	// Existing eight-cohort append policy (as in the shared search envelope).
	commit.copies(8, links, uint64(unsafe.Sizeof(vectorIndexFrozenPrefixReciprocalLink{})))
	commit.copies(8, links, uint64(unsafe.Sizeof(vectorIndexFrozenPrefixReciprocalGroup{})))
	commit.copies(2, links, uint64(unsafe.Sizeof(bool(false))))
	commit.copies(2, links, uint64(unsafe.Sizeof(vectorIndexFrozenPrefixDiversityScratch{})))
	var dot nativeColumnGraphCapacitySum
	// A group starts with <=2M neighbors and can add <=width new endpoints.
	dot.copies(8, uint64(def.M*2)+planWidth, uint64(unsafe.Sizeof(vectorIndexNeighbor{})))
	// Indexed row IDs/dots grow by replacement, retaining at most old+new.
	dot.copies(2, uint64(def.M*2), uint64(unsafe.Sizeof(uint32(0))))
	dot.copies(2, uint64(def.M*2), uint64(unsafe.Sizeof(float32(0))))
	if dot.err != nil {
		return nativeColumnGraphConstructionPlan{}, dot.err
	}
	reciprocalWorkers := links
	if workers > 0 {
		reciprocalWorkers = min(links, uint64(workers))
	}
	dotTotal, err := nativeTypedCheckedMul(reciprocalWorkers, dot.bytes)
	if err != nil {
		return nativeColumnGraphConstructionPlan{}, err
	}
	commit.add(dotTotal)
	if commit.err != nil {
		return nativeColumnGraphConstructionPlan{}, commit.err
	}
	p.CommitBytes = commit.bytes

	var locality nativeColumnGraphCapacitySum
	locality.allocation(rows, uint64(unsafe.Sizeof(columnVectorGraphAssetRow{})))
	// inputOrdinalByNode, nodeOrdinal, order, queue, visited. Charge queue and
	// visited even though their helper-return lifetime ends before conversion.
	locality.copies(4, rows, uint64(unsafe.Sizeof(int(0))))
	locality.allocation(rows, uint64(unsafe.Sizeof(bool(false))))
	// The native six-descriptor row term already dominates the one exact
	// node allocation including its aggregate rounding, even for a single row.
	// One row's conversion layer headers and temporary uint32 layer payloads.
	locality.allocation(uint64(maxLevel+1), uint64(unsafe.Sizeof([]uint32{})))
	for layer := 0; layer <= maxLevel; layer++ {
		degree := uint64(def.M)
		if layer == 0 {
			degree *= 2
		}
		locality.allocation(degree, 4)
	}
	if locality.err != nil {
		return nativeColumnGraphConstructionPlan{}, locality.err
	}
	p.LocalityBytes = locality.bytes

	var control nativeColumnGraphCapacitySum
	control.allocation(1, uint64(unsafe.Sizeof(columnGraphPartitionLiveCarrier{})))
	control.allocation(1, uint64(unsafe.Sizeof(columnGraphHNSWBuilder{})))
	control.allocation(uint64(len(def.Field))+1, uint64(unsafe.Sizeof(string(""))))
	control.allocation(1, uint64(unsafe.Sizeof(vectorIndexFrozenPrefixCommitScratch{})))
	control.allocation(1, uint64(unsafe.Sizeof(nativeColumnGraphConstructionPlan{})))
	// Existing1KiB control policy covers each bounded worker closure/capture
	// and operation control. These contain borrowed pointers/scalars only;
	// corpus backings are enumerated above, and observer/trace are excluded.
	controls := reciprocalWorkers + planningWorkers + 1
	control.copies(controls, 1, preparedSemanticStreamMapEntryReserveBytes)
	if control.err != nil {
		return nativeColumnGraphConstructionPlan{}, control.err
	}
	p.ControlBytes = control.bytes
	var insertion, remap nativeColumnGraphCapacitySum
	for _, n := range []uint64{p.SearchScratchBytes, p.PlanningBytes, p.CommitBytes} {
		insertion.add(n)
	}
	remap.add(p.SerialScratchBytes)
	remap.add(p.LocalityBytes)
	if insertion.err != nil {
		return nativeColumnGraphConstructionPlan{}, insertion.err
	}
	if remap.err != nil {
		return nativeColumnGraphConstructionPlan{}, remap.err
	}
	p.InsertionBytes, p.RemapBytes = insertion.bytes, remap.bytes
	// All planner workers are joined and their operation-local array and
	// commit buffers end before locality starts. The index's serial scratch,
	// node/vector/matrix/map backing remains live through locality.
	var private nativeColumnGraphCapacitySum
	for _, n := range []uint64{p.NativeRowsBytes, p.MatrixBytes, p.ControlBytes, max(p.InsertionBytes, p.RemapBytes)} {
		private.add(n)
	}
	if private.err != nil {
		return nativeColumnGraphConstructionPlan{}, private.err
	}
	p.PrivateBytes = private.bytes
	private.add(p.CallerRowsBytes)
	if private.err != nil {
		return nativeColumnGraphConstructionPlan{}, private.err
	}
	p.TotalBytes = private.bytes
	return p, nil
}
