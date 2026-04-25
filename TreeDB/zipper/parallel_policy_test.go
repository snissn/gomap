package zipper

import "testing"

func TestInternalMergeParallelThresholds_Default(t *testing.T) {
	minChildren, minOps := internalMergeParallelThresholds(false, ParallelMergePressureNormal)
	if minChildren != mergeInternalMinParallelChildren || minOps != mergeInternalMinParallelOps {
		t.Fatalf("thresholds=(children=%d ops=%d) want (%d,%d)", minChildren, minOps, mergeInternalMinParallelChildren, mergeInternalMinParallelOps)
	}
}

func TestInternalMergeParallelThresholds_MaintenanceIgnoresPressure(t *testing.T) {
	minChildren, minOps := internalMergeParallelThresholds(true, ParallelMergePressureCritical)
	if minChildren != mergeInternalMinParallelChildren || minOps != mergeInternalMinParallelOps {
		t.Fatalf("maintenance thresholds=(children=%d ops=%d) want (%d,%d)", minChildren, minOps, mergeInternalMinParallelChildren, mergeInternalMinParallelOps)
	}
}

func TestShouldUseParallelInternalMerge_HighPressureTightensGate(t *testing.T) {
	if !shouldUseParallelInternalMerge(mergeInternalMinParallelChildren, mergeInternalMinParallelOps, 4, false, ParallelMergePressureNormal) {
		t.Fatalf("baseline gate unexpectedly disabled")
	}
	if shouldUseParallelInternalMerge(mergeInternalMinParallelChildren, mergeInternalMinParallelOps, 4, false, ParallelMergePressureHigh) {
		t.Fatalf("high-pressure gate should reject baseline-sized work")
	}
	if !shouldUseParallelInternalMerge(mergeInternalHighPressureMinChildren, mergeInternalHighPressureMinOps, 4, false, ParallelMergePressureHigh) {
		t.Fatalf("high-pressure gate should accept the raised threshold")
	}
}

func TestShouldUseParallelInternalMerge_CriticalPressureTightensFurther(t *testing.T) {
	if shouldUseParallelInternalMerge(mergeInternalHighPressureMinChildren, mergeInternalHighPressureMinOps, 4, false, ParallelMergePressureCritical) {
		t.Fatalf("critical-pressure gate should reject high-pressure-sized work")
	}
	if !shouldUseParallelInternalMerge(mergeInternalCriticalPressureMinChildren, mergeInternalCriticalPressureMinOps, 4, false, ParallelMergePressureCritical) {
		t.Fatalf("critical-pressure gate should accept the raised threshold")
	}
}

func TestZipperParallelMergePressureLevel_NormalizesUnknown(t *testing.T) {
	z := &Zipper{
		parallelMergePressure: func() ParallelMergePressureLevel {
			return ParallelMergePressureLevel(255)
		},
	}
	if got := z.parallelMergePressureLevel(); got != ParallelMergePressureNormal {
		t.Fatalf("pressure=%v want %v", got, ParallelMergePressureNormal)
	}
}
