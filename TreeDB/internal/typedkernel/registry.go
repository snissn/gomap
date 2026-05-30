package typedkernel

import (
	"fmt"
	"slices"

	"github.com/snissn/gomap/TreeDB/internal/columnlayout"
	"github.com/snissn/gomap/TreeDB/internal/columnsemantics"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

// AggregateOp names the aggregate semantics accepted by this package. The
// values intentionally alias columnsemantics.Operation so callers can carry the
// #1843 decision through prepare without an additional translation layer.
type AggregateOp = columnsemantics.Operation

const (
	OpCountRows               AggregateOp = columnsemantics.OpCountRows
	OpCountNonNull            AggregateOp = columnsemantics.OpCountNonNull
	OpSum                     AggregateOp = columnsemantics.OpSum
	OpAvg                     AggregateOp = columnsemantics.OpAvg
	OpMin                     AggregateOp = columnsemantics.OpMin
	OpMax                     AggregateOp = columnsemantics.OpMax
	OpBoolCounts              AggregateOp = columnsemantics.OpBoolCounts
	OpDictionaryGroupBy       AggregateOp = columnsemantics.OpDictionaryGroupBy
	OpDictionaryCount         AggregateOp = columnsemantics.OpDictionaryCount
	OpDictionaryCountDistinct AggregateOp = columnsemantics.OpDictionaryCountDistinct
)

// AggregateResult keeps row-count and value-count semantics distinct. For
// CountRows, Rows is the row count. For CountNonNull and value aggregates,
// NonNulls is the number of values included in the aggregate. For OpBoolCounts,
// Rows and NonNulls are the selected non-null bool rows while TrueCount and
// FalseCount partition those rows. HasValue is false for empty min/max/avg
// inputs.
type AggregateResult struct {
	Op         AggregateOp
	Rows       int64
	NonNulls   int64
	TrueCount  int64
	FalseCount int64
	Sum        int64
	Avg        float64
	Min        int64
	Max        int64
	HasValue   bool
}

// ReduceRequest is the concrete hot-loop input for one selected block. Rows is
// the block-local row domain. Int64Values is used by materialized/direct-view
// int64 value kernels and may be nil for count-row/count-non-null reducers and
// empty selections. Int64Cursor is used by streaming int64 kernels; callers
// provide either Int64Values or Int64Cursor for value aggregates, not both.
type ReduceRequest struct {
	Rows           int
	Selection      typedcolumn.RowSelection
	Int64Values    []int64
	Int64Cursor    *typedcolumn.Int64Cursor
	BoolGranule    typedcolumn.EncodedGranule
	HasBoolGranule bool
	BoolReader     *typedcolumn.GranuleReader
}

// Scratch is caller/session-owned reusable storage for future kernels. It is
// deliberately explicit even though the initial int64 reducers do not need it.
type Scratch struct {
	Selection  typedcolumn.RowSelectionScratch
	Int64      []int64
	Bool       typedcolumn.BoolSelectionScratch
	Dictionary typedcolumn.Uint32CodeSelectionScratch
}

// ReduceFunc runs a concrete reducer after registry dispatch. Implementations
// must switch on RowSelection.Kind and must not use reflection or per-row
// interface/callback dispatch.
type ReduceFunc func(op AggregateOp, req ReduceRequest, scratch *Scratch) (AggregateResult, error)

// KernelSpec is an immutable registry entry. Empty Logical/Physical fields are
// wildcards, intended for generic kernels such as count rows. Value kernels
// should bind logical and physical types and rely on Dispatch to enforce the
// semantic and layout gates before matching.
type KernelSpec struct {
	Name          string
	Logical       columnsemantics.LogicalType
	Physical      typedcolumn.ColumnType
	Ops           []AggregateOp
	AllowNullable bool
	Reduce        ReduceFunc
}

// Registry is an immutable/caller-owned dispatch table. Methods that add a
// kernel return a new Registry and never mutate the receiver.
type Registry struct {
	entries []KernelSpec
}

var defaultEntries = [...]KernelSpec{
	{
		Name:   "generic.count_rows.v1",
		Ops:    []AggregateOp{OpCountRows},
		Reduce: reduceCountRows,
		// Counting rows does not consume carrier values and is safe for nullable
		// wrappers once semantics/layout have accepted the operation.
		AllowNullable: true,
	},
	{
		Name:     "int64.aggregate.v1",
		Logical:  columnsemantics.LogicalInt64,
		Physical: typedcolumn.ColumnTypeInt64,
		Ops:      []AggregateOp{OpCountNonNull, OpSum, OpAvg, OpMin, OpMax},
		Reduce:   reduceInt64Aggregate,
	},
	{
		Name:     "bool.counts.v1",
		Logical:  columnsemantics.LogicalBool,
		Physical: typedcolumn.ColumnTypeBool,
		Ops:      []AggregateOp{OpCountNonNull, OpBoolCounts},
		Reduce:   reduceBoolCounts,
	},
}

// DefaultRegistry returns the package's static immutable kernel table.
func DefaultRegistry() Registry {
	entries := make([]KernelSpec, len(defaultEntries))
	for i, entry := range defaultEntries {
		entries[i] = cloneKernelSpec(entry)
	}
	return Registry{entries: entries}
}

// NewRegistry returns a caller-owned registry containing the provided entries.
func NewRegistry(entries []KernelSpec) (Registry, error) {
	out := Registry{entries: make([]KernelSpec, len(entries))}
	for i, entry := range entries {
		if err := validateKernelSpec(entry); err != nil {
			return Registry{}, fmt.Errorf("typedkernel: kernel %d: %w", i, err)
		}
		out.entries[i] = cloneKernelSpec(entry)
	}
	return out, nil
}

// WithKernel returns a new caller-owned registry with entry appended.
func (r Registry) WithKernel(entry KernelSpec) (Registry, error) {
	if err := validateKernelSpec(entry); err != nil {
		return Registry{}, err
	}
	out := Registry{entries: make([]KernelSpec, 0, len(r.entries)+1)}
	for _, existing := range r.entries {
		out.entries = append(out.entries, cloneKernelSpec(existing))
	}
	out.entries = append(out.entries, cloneKernelSpec(entry))
	return out, nil
}

// DispatchRequest is resolved at prepare/block-planning boundaries. Semantic
// and layout descriptors must describe the same logical/physical column.
type DispatchRequest struct {
	Operation AggregateOp
	Semantic  columnsemantics.Descriptor
	Layout    columnlayout.Capabilities
}

// PreparedReducer is the concrete reducer selected by Dispatch. It is safe to
// reuse for blocks with the same prepared semantic/layout decision.
type PreparedReducer struct {
	op          AggregateOp
	kernel      KernelSpec
	semanticCap columnsemantics.Capability
	layoutCap   columnlayout.Capability
}

func (p PreparedReducer) Operation() AggregateOp { return p.op }
func (p PreparedReducer) KernelName() string     { return p.kernel.Name }
func (p PreparedReducer) SemanticCapability() columnsemantics.Capability {
	return p.semanticCap
}
func (p PreparedReducer) LayoutCapability() columnlayout.Capability { return p.layoutCap }

// Reduce executes the selected concrete reducer. The function-table dispatch is
// outside row loops; reducers own their RowSelection.Kind switch.
func (p PreparedReducer) Reduce(req ReduceRequest, scratch *Scratch) (AggregateResult, error) {
	if p.kernel.Reduce == nil {
		return AggregateResult{}, fmt.Errorf("typedkernel: uninitialized reducer for %s", p.op)
	}
	return p.kernel.Reduce(p.op, req, scratch)
}

// Dispatch validates #1843 semantics and #1838 layout capabilities, then picks
// the first matching registry entry. Unsupported/fallback capabilities fail
// closed before any physical kernel is selected.
func (r Registry) Dispatch(req DispatchRequest) (PreparedReducer, error) {
	if !isAggregateOp(req.Operation) {
		return PreparedReducer{}, fmt.Errorf("typedkernel: unsupported aggregate operation %q", req.Operation)
	}
	if err := validateDescriptorPair(req.Semantic, req.Layout.Descriptor); err != nil {
		return PreparedReducer{}, err
	}
	semCap := columnsemantics.CapabilityFor(req.Semantic, req.Operation)
	if !semCap.Supported() {
		return PreparedReducer{}, fmt.Errorf("typedkernel: semantic capability %s status=%s reason=%s", req.Operation, semCap.Status, semCap.Error())
	}
	layoutCap := req.Layout.SupportsSemanticOperation(req.Operation)
	if !layoutCap.Supported() {
		return PreparedReducer{}, fmt.Errorf("typedkernel: layout capability %s status=%s reason=%s", req.Operation, layoutCap.Status, layoutCap.Error())
	}
	for _, entry := range r.entries {
		if entry.matches(req.Operation, req.Semantic, req.Layout) {
			return PreparedReducer{op: req.Operation, kernel: cloneKernelSpec(entry), semanticCap: semCap, layoutCap: layoutCap}, nil
		}
	}
	return PreparedReducer{}, fmt.Errorf("typedkernel: no kernel registered for op=%s logical=%s physical=%s encoding=%s nullable=%v", req.Operation, req.Semantic.Logical, req.Semantic.Physical, req.Semantic.Encoding, nullableDescriptor(req.Semantic, req.Layout))
}

func validateKernelSpec(entry KernelSpec) error {
	if entry.Name == "" {
		return fmt.Errorf("missing kernel name")
	}
	if entry.Reduce == nil {
		return fmt.Errorf("kernel %q missing reducer", entry.Name)
	}
	if len(entry.Ops) == 0 {
		return fmt.Errorf("kernel %q missing operations", entry.Name)
	}
	for _, op := range entry.Ops {
		if !isAggregateOp(op) {
			return fmt.Errorf("kernel %q unsupported aggregate operation %q", entry.Name, op)
		}
	}
	return nil
}

func cloneKernelSpec(entry KernelSpec) KernelSpec {
	entry.Ops = slices.Clone(entry.Ops)
	return entry
}

func (entry KernelSpec) matches(op AggregateOp, desc columnsemantics.Descriptor, layout columnlayout.Capabilities) bool {
	if !slices.Contains(entry.Ops, op) {
		return false
	}
	if entry.Logical != "" && entry.Logical != desc.Logical {
		return false
	}
	if entry.Physical != "" && entry.Physical != desc.Physical {
		return false
	}
	if nullableDescriptor(desc, layout) && !entry.AllowNullable {
		return false
	}
	return true
}

func isAggregateOp(op AggregateOp) bool {
	switch op {
	case OpCountRows, OpCountNonNull, OpSum, OpAvg, OpMin, OpMax, OpBoolCounts, OpDictionaryGroupBy, OpDictionaryCount, OpDictionaryCountDistinct:
		return true
	default:
		return false
	}
}

func validateDescriptorPair(sem columnsemantics.Descriptor, layout columnlayout.Descriptor) error {
	if sem.Logical != layout.Logical || sem.Physical != layout.Physical || sem.Encoding != layout.Encoding || sem.Nullable != layout.Nullable || sem.DictionaryOrder != layout.DictionaryOrder || sem.DictionaryCollation != layout.DictionaryCollation {
		return fmt.Errorf("typedkernel: semantic/layout descriptor mismatch semantic=(logical=%s physical=%s encoding=%s nullable=%v dictionary_order=%v collation=%q) layout=(logical=%s physical=%s encoding=%s nullable=%v dictionary_order=%v collation=%q)", sem.Logical, sem.Physical, sem.Encoding, sem.Nullable, sem.DictionaryOrder, sem.DictionaryCollation, layout.Logical, layout.Physical, layout.Encoding, layout.Nullable, layout.DictionaryOrder, layout.DictionaryCollation)
	}
	return nil
}

func nullableDescriptor(desc columnsemantics.Descriptor, layout columnlayout.Capabilities) bool {
	return desc.Nullable || desc.Encoding == typedcolumn.EncodingNullableInt64 || layout.Wrappers.Nullable
}

func validateSelectionRows(req ReduceRequest) (int, error) {
	rows := req.Rows
	if rows == 0 && req.Selection.Rows() != 0 {
		rows = req.Selection.Rows()
	}
	if rows < 0 {
		return 0, fmt.Errorf("typedkernel: negative row domain %d", rows)
	}
	if req.Selection.Rows() != rows {
		return 0, fmt.Errorf("typedkernel: selection rows=%d want %d", req.Selection.Rows(), rows)
	}
	return rows, nil
}

func reduceCountRows(op AggregateOp, req ReduceRequest, _ *Scratch) (AggregateResult, error) {
	if op != OpCountRows {
		return AggregateResult{}, fmt.Errorf("typedkernel: count rows reducer got op=%s", op)
	}
	if _, err := validateSelectionRows(req); err != nil {
		return AggregateResult{}, err
	}
	return AggregateResult{Op: op, Rows: int64(req.Selection.Count()), HasValue: true}, nil
}
