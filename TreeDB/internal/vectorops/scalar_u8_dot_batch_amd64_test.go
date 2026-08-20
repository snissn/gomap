//go:build amd64 && !purego

package vectorops

import (
	"testing"

	"golang.org/x/sys/cpu"
)

func TestScalarU8DotBatchAMD64Dispatch2702(t *testing.T) {
	want := "indexed_amd64_sse2"
	switch {
	case cpu.X86.HasAVX512F && cpu.X86.HasAVX512BW && cpu.X86.HasAVX512DQ && cpu.X86.HasAVX512VL && cpu.X86.HasAVX512VNNI:
		want = "indexed_amd64_avx512_vnni"
	case cpu.X86.HasAVX2:
		want = "indexed_amd64_avx2"
	}
	if got := ScalarU8DotBatchImplementation(); got != want {
		t.Fatalf("ScalarU8DotBatchImplementation()=%q want %q", got, want)
	}
}
