package zipper

import (
	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/page"
	"unsafe"
)

// Fixed scalars only. The charge already contains all original linked frames;
// FrameRaw is diagnostic layout, never another allocation to add to Charge.
type NativeMaterializedGeometryForTestV6 struct {
	Charge, HeaderRaw, FrameRaw, Phase, Visited, OpsCapacity             uint64
	InputCharge, InputRaw, InputEntriesCapacity                          uint64
	End, Written, PageResult, OldPage, Adopted, AppendTarget, Diagnostic bool
	ValidationRecords, ValidationBytes                                   uint64
	Overflow                                                             bool
}

// An ordinal restarts at the immutable frame head. Every traversed link is
// preadmitted, including links revisited to reach a later ordinal. A frame
// pointer is never returned or retained as an observer continuation.
func (s *MaterializedWriteWork) MaintenanceNativeFrameGeometryForTestV6(ordinal uint64, a freelist.NativeCOWGeometryForTestV6, input *MaterializedInputWorkV6, w *iterator.OrdinalScanWork) (g NativeMaterializedGeometryForTestV6, ready bool) {
	if w == nil || !w.Reserve(2, 2*uint64(unsafe.Sizeof(MaterializedWriteWork{}))+2*uint64(unsafe.Sizeof(MaterializedInputWorkV6{}))+2*uint64(unsafe.Sizeof(g))) {
		return g, false
	}
	if s == nil {
		g.End = true
		return g, true
	}
	g.Charge, g.HeaderRaw, g.FrameRaw, g.Phase = s.charge, uint64(unsafe.Sizeof(*s)), uint64(unsafe.Sizeof(materializedWriteFrame{})), uint64(s.phase)
	g.Adopted, g.AppendTarget, g.Diagnostic = s.adoptionBegun, s.appendTarget != nil, s.unboundedLeafPersistenceDiagnostic
	if input != nil {
		g.InputCharge, g.InputRaw, g.InputEntriesCapacity = input.charge, uint64(unsafe.Sizeof(*input)), uint64(cap(input.entries))
	}
	for f := s.head; f != nil; {
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(materializedWriteFrame{}))) {
			return g, false
		}
		g.Visited++
		if g.Visited-1 == ordinal {
			g.Written, g.PageResult, g.OldPage = f.written, f.result.IsPage(), f.oldID != 0
			g.OpsCapacity = uint64(cap(f.ops)) // borrowed input, NOT a new per-frame backing
			p := nativeFramePriceForTestV6{records: 3, bytes: 6*uint64(unsafe.Sizeof(freelist.AppendWork{})) + 2*uint64(unsafe.Sizeof(page.ChildRef{})) + 96}
			if !p.add(a.LiveAllocated, a.AllocatedVisitBytes) || !p.add(a.PrivateAllocated, a.AllocatedVisitBytes) {
				g.Overflow = true
				return g, true
			}
			if g.OldPage {
				if !p.add(a.RetirementRecords, a.RetirementVisitBytes) || !p.add(a.RetirementIDs, 16) {
					g.Overflow = true
					return g, true
				}
			}
			g.ValidationRecords, g.ValidationBytes = p.records, p.bytes
			return g, true
		}
		f = f.next
	}
	g.End = true
	return g, true
}

type nativeFramePriceForTestV6 struct{ records, bytes uint64 }

func (p *nativeFramePriceForTestV6) add(n, b uint64) bool {
	if n > ^uint64(0)-p.records || n != 0 && b > ^uint64(0)/n || n*b > ^uint64(0)-p.bytes {
		return false
	}
	p.records += n
	p.bytes += n * b
	return true
}

// The original output owner can be inspected without exporting its pointer.
func (s *MaterializedWriteWork) MaintenanceNativeOwnerGeometryForTestV6(b retainedalloc.Budget, expected *retainedalloc.Owner, w *iterator.OrdinalScanWork) (retainedalloc.NativeOwnerGeometryForTestV6, bool, bool) {
	if w == nil || !w.Reserve(1, 2*uint64(unsafe.Sizeof(MaterializedWriteWork{}))) {
		return retainedalloc.NativeOwnerGeometryForTestV6{}, false, false
	}
	if s == nil {
		g, r := (*retainedalloc.Owner)(nil).MaintenanceNativeOwnerGeometryForTestV6(b, w)
		return g, false, r
	}
	g, r := s.owner.MaintenanceNativeOwnerGeometryForTestV6(b, w)
	return g, s.owner == expected, r
}
