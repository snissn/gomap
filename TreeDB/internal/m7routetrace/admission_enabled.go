//go:build treedb_test && m7_route_trace

package m7routetrace

import (
	"runtime"
	"strings"
	"sync/atomic"
)

type admissionCell struct{ fields [8]atomic.Uint64 }

func (c *admissionCell) store(t AdmissionTuple) {
	for i, v := range [8]uint64{t.Call, t.Site, t.A, t.B, t.C, t.D, t.E, t.F} {
		c.fields[i].Store(v)
	}
}
func (c *admissionCell) load() AdmissionTuple {
	var v [8]uint64
	for i := range v {
		v[i] = c.fields[i].Load()
	}
	return AdmissionTuple{v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7]}
}
func admission(o *Observer, site, a, b, c, d, e, f uint64) {
	if site >= 8 {
		return
	}
	o.admissionCount[site].Add(1)
	t := AdmissionTuple{o.calls.Load(), site, a, b, c, d, e, f}
	n := o.admissionTuples.Add(1) - 1
	if n < 24 {
		o.admissionFirst[n].store(t)
	}
	o.admissionLast[n%8].store(t)
	if site == 0 && o.reserveSeen.CompareAndSwap(0, 1) {
		o.firstReserve.store(t)
	}
}
func Admission(owner uintptr, site, a, b, c, d, e, f uint64) {
	o := active.Load()
	if o == nil || owner == 0 || (owner != o.owners[0] && owner != o.owners[1] && owner != o.owners[2]) {
		return
	}
	admission(o, site, a, b, c, d, e, f)
}
func diagnosticHash(s string) uint64 {
	h := uint64(14695981039346656037)
	for i := 0; i < len(s); i++ {
		h = (h ^ uint64(s[i])) * 1099511628211
	}
	return h
}

// ReserveFailure observes only the existing single active public fixture's
// synchronous floor-sync stack. It retains no Work/source/key/frame pointers.
// Stack lookup and observer work are diagnostic overhead, never qualification.
func ReserveFailure(records, bytes, usedR, usedB, limitR, limitB uint64) {
	o := active.Load()
	if o == nil {
		return
	}
	var pcs [32]uintptr
	n := runtime.Callers(2, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	var line, site uint64
	inSync := false
	for {
		fr, more := frames.Next()
		if site == 0 && !strings.Contains(fr.Function, "m7routetrace.") && !strings.HasSuffix(fr.Function, "OrdinalScanWork).Reserve") {
			line = uint64(fr.Line)
			site = diagnosticHash(fr.Function)
		}
		if strings.HasSuffix(fr.Function, ".syncMaintenanceNoWAL") {
			inSync = true
		}
		if !more {
			break
		}
	}
	if !inSync {
		return
	}
	var remR, remB uint64
	if usedR <= limitR {
		remR = limitR - usedR
	}
	if usedB <= limitB {
		remB = limitB - usedB
	}
	admission(o, 0, records, bytes, remR, remB, line, site)
}
