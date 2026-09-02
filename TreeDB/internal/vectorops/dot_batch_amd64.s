//go:build amd64 && !purego

#include "textflag.h"

// The per-row arithmetic below mirrors github.com/tphakala/simd/f32 v1.0.22.
// Only row lookup and dispatch move outside the row loop; accumulator and
// reduction order stay identical to DotFloat32 on the selected ISA.

// func dotFloat32IndexedAMD64AVX512Kernel(dst []float32, base []float32, query []float32, rowIDs []uint32, dims, rows int)
TEXT ·dotFloat32IndexedAMD64AVX512Kernel(SB), NOSPLIT, $0-112
	MOVQ dst_base+0(FP), R8
	MOVQ base_base+24(FP), R9
	MOVQ query_base+48(FP), R10
	MOVQ rowIDs_base+72(FP), R11
	MOVQ dims+96(FP), R12
	MOVQ rows+104(FP), R13

indexed_f32_avx512_row:
	TESTQ R13, R13
	JZ indexed_f32_avx512_done
	MOVL (R11), AX
	ADDQ $4, R11
	IMULQ R12, AX
	LEAQ (R9)(AX*4), SI
	MOVQ R10, DI
	MOVQ R12, CX

	VXORPS Z0, Z0, Z0
	VXORPS Z3, Z3, Z3
	VXORPS Z4, Z4, Z4
	VXORPS Z5, Z5, Z5

	MOVQ CX, AX
	SHRQ $6, AX
	JZ indexed_f32_avx512_loop16_check

indexed_f32_avx512_loop64:
	VMOVUPS (SI), Z1
	VMOVUPS (DI), Z2
	VFMADD231PS Z1, Z2, Z0
	VMOVUPS 64(SI), Z1
	VMOVUPS 64(DI), Z2
	VFMADD231PS Z1, Z2, Z3
	VMOVUPS 128(SI), Z1
	VMOVUPS 128(DI), Z2
	VFMADD231PS Z1, Z2, Z4
	VMOVUPS 192(SI), Z1
	VMOVUPS 192(DI), Z2
	VFMADD231PS Z1, Z2, Z5
	ADDQ $256, SI
	ADDQ $256, DI
	DECQ AX
	JNZ indexed_f32_avx512_loop64

	VADDPS Z3, Z0, Z0
	VADDPS Z4, Z0, Z0
	VADDPS Z5, Z0, Z0

indexed_f32_avx512_loop16_check:
	ANDQ $63, CX
	MOVQ CX, AX
	SHRQ $4, AX
	JZ indexed_f32_avx512_reduce

indexed_f32_avx512_loop16:
	VMOVUPS (SI), Z1
	VMOVUPS (DI), Z2
	VFMADD231PS Z1, Z2, Z0
	ADDQ $64, SI
	ADDQ $64, DI
	DECQ AX
	JNZ indexed_f32_avx512_loop16

indexed_f32_avx512_reduce:
	VEXTRACTF32X8 $1, Z0, Y1
	VADDPS Y1, Y0, Y0
	VEXTRACTF128 $1, Y0, X1
	VADDPS X1, X0, X0
	VHADDPS X0, X0, X0
	VHADDPS X0, X0, X0

	ANDQ $15, CX
	JZ indexed_f32_avx512_store

indexed_f32_avx512_scalar:
	VMOVSS (SI), X1
	VMOVSS (DI), X2
	VFMADD231SS X1, X2, X0
	ADDQ $4, SI
	ADDQ $4, DI
	DECQ CX
	JNZ indexed_f32_avx512_scalar

indexed_f32_avx512_store:
	VMOVSS X0, (R8)
	ADDQ $4, R8
	DECQ R13
	JMP indexed_f32_avx512_row

indexed_f32_avx512_done:
	VZEROUPPER
	RET

// func dotFloat32IndexedAMD64AVXFMAKernel(dst []float32, base []float32, query []float32, rowIDs []uint32, dims, rows int)
TEXT ·dotFloat32IndexedAMD64AVXFMAKernel(SB), NOSPLIT, $0-112
	MOVQ dst_base+0(FP), R8
	MOVQ base_base+24(FP), R9
	MOVQ query_base+48(FP), R10
	MOVQ rowIDs_base+72(FP), R11
	MOVQ dims+96(FP), R12
	MOVQ rows+104(FP), R13

indexed_f32_avx_row:
	TESTQ R13, R13
	JZ indexed_f32_avx_done
	MOVL (R11), AX
	ADDQ $4, R11
	IMULQ R12, AX
	LEAQ (R9)(AX*4), SI
	MOVQ R10, DI
	MOVQ R12, CX

	VXORPS Y0, Y0, Y0
	VXORPS Y3, Y3, Y3
	VXORPS Y4, Y4, Y4
	VXORPS Y5, Y5, Y5

	MOVQ CX, AX
	SHRQ $5, AX
	JZ indexed_f32_avx_loop8_check

indexed_f32_avx_loop32:
	VMOVUPS (SI), Y1
	VMOVUPS (DI), Y2
	VFMADD231PS Y1, Y2, Y0
	VMOVUPS 32(SI), Y1
	VMOVUPS 32(DI), Y2
	VFMADD231PS Y1, Y2, Y3
	VMOVUPS 64(SI), Y1
	VMOVUPS 64(DI), Y2
	VFMADD231PS Y1, Y2, Y4
	VMOVUPS 96(SI), Y1
	VMOVUPS 96(DI), Y2
	VFMADD231PS Y1, Y2, Y5
	ADDQ $128, SI
	ADDQ $128, DI
	DECQ AX
	JNZ indexed_f32_avx_loop32

	VADDPS Y3, Y0, Y0
	VADDPS Y4, Y0, Y0
	VADDPS Y5, Y0, Y0

indexed_f32_avx_loop8_check:
	ANDQ $31, CX
	MOVQ CX, AX
	SHRQ $3, AX
	JZ indexed_f32_avx_reduce

indexed_f32_avx_loop8:
	VMOVUPS (SI), Y1
	VMOVUPS (DI), Y2
	VFMADD231PS Y1, Y2, Y0
	ADDQ $32, SI
	ADDQ $32, DI
	DECQ AX
	JNZ indexed_f32_avx_loop8

indexed_f32_avx_reduce:
	VEXTRACTF128 $1, Y0, X1
	VADDPS X1, X0, X0
	VHADDPS X0, X0, X0
	VHADDPS X0, X0, X0

	ANDQ $7, CX
	JZ indexed_f32_avx_store

indexed_f32_avx_scalar:
	VMOVSS (SI), X1
	VMOVSS (DI), X2
	VFMADD231SS X1, X2, X0
	ADDQ $4, SI
	ADDQ $4, DI
	DECQ CX
	JNZ indexed_f32_avx_scalar

indexed_f32_avx_store:
	VMOVSS X0, (R8)
	ADDQ $4, R8
	DECQ R13
	JMP indexed_f32_avx_row

indexed_f32_avx_done:
	VZEROUPPER
	RET

// func dotFloat32IndexedAMD64SSE2Kernel(dst []float32, base []float32, query []float32, rowIDs []uint32, dims, rows int)
TEXT ·dotFloat32IndexedAMD64SSE2Kernel(SB), NOSPLIT, $0-112
	MOVQ dst_base+0(FP), R8
	MOVQ base_base+24(FP), R9
	MOVQ query_base+48(FP), R10
	MOVQ rowIDs_base+72(FP), R11
	MOVQ dims+96(FP), R12
	MOVQ rows+104(FP), R13

indexed_f32_sse2_row:
	TESTQ R13, R13
	JZ indexed_f32_sse2_done
	MOVL (R11), AX
	ADDQ $4, R11
	IMULQ R12, AX
	LEAQ (R9)(AX*4), SI
	MOVQ R10, DI
	MOVQ R12, CX
	XORPS X0, X0

	MOVQ CX, AX
	SHRQ $2, AX
	JZ indexed_f32_sse2_reduce

indexed_f32_sse2_loop4:
	MOVUPS (SI), X1
	MOVUPS (DI), X2
	MULPS X2, X1
	ADDPS X1, X0
	ADDQ $16, SI
	ADDQ $16, DI
	DECQ AX
	JNZ indexed_f32_sse2_loop4

indexed_f32_sse2_reduce:
	MOVAPS X0, X1
	SHUFPS $0x0E, X1, X1
	ADDPS X1, X0
	MOVAPS X0, X1
	SHUFPS $0x01, X1, X1
	ADDSS X1, X0

	ANDQ $3, CX
	JZ indexed_f32_sse2_store

indexed_f32_sse2_scalar:
	MOVSS (SI), X1
	MOVSS (DI), X2
	MULSS X2, X1
	ADDSS X1, X0
	ADDQ $4, SI
	ADDQ $4, DI
	DECQ CX
	JNZ indexed_f32_sse2_scalar

indexed_f32_sse2_store:
	MOVSS X0, (R8)
	ADDQ $4, R8
	DECQ R13
	JMP indexed_f32_sse2_row

indexed_f32_sse2_done:
	RET
