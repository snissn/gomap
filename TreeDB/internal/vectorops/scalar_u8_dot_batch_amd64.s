//go:build amd64 && !purego

#include "textflag.h"

// func dotScalarU8CenteredIndexedAMD64SSE2(dst []int64, codes []byte, query []ScalarU8CenteredCode, rowIDs []uint32, dims int, rows int, querySum int64)
//
// Preconditions are checked by the Go wrapper: rows <= len(dst,rowIDs), dims > 0,
// len(query) >= dims, and every row ID addresses a full row in codes. querySum
// is sum(query[:dims]). The SSE2 kernel is called only for dims <= 32768 so its
// signed 32-bit vector accumulation cannot overflow. It computes centered scores
// as:
//   sum(q * (2*row - 255)) == 2*sum(q*row) - 255*querySum.
TEXT ·dotScalarU8CenteredIndexedAMD64SSE2(SB), NOSPLIT, $0-120
	MOVQ dst_base+0(FP), DI
	MOVQ codes_base+24(FP), SI
	MOVQ query_base+48(FP), R8
	MOVQ rowIDs_base+72(FP), R9
	MOVQ dims+96(FP), R10
	MOVQ rows+104(FP), R11
	MOVQ querySum+112(FP), R12

	PXOR X7, X7                  // zero vector for byte->word widening

scalaru8_amd64_row_loop:
	TESTQ R11, R11
	JZ scalaru8_amd64_done

	MOVL (R9), AX                // row ID; MOVL zero-extends on amd64
	ADDQ $4, R9
	IMULQ R10, AX                // byte offset = rowID*dims
	LEAQ (SI)(AX*1), BX          // row pointer
	MOVQ R8, CX                  // query pointer
	MOVQ R10, DX                 // remaining dims for scalar tail mask
	XORQ R13, R13                // raw scalar accumulator after vector reduction
	PXOR X0, X0                  // packed int32 accumulator

	MOVQ DX, AX
	SHRQ $4, AX                  // 16 dimensions per SSE2 iteration
	JZ scalaru8_amd64_reduce

scalaru8_amd64_loop16:
	MOVOU (BX), X1               // 16 row bytes
	MOVOU X1, X2
	PUNPCKLBW X7, X1             // low 8 bytes -> uint16 words
	PUNPCKHBW X7, X2             // high 8 bytes -> uint16 words
	MOVOU (CX), X3               // low 8 int16 centered query values
	MOVOU 16(CX), X4             // high 8 int16 centered query values
	PMADDWL X3, X1               // adjacent int16 products -> int32 sums
	PMADDWL X4, X2
	PADDL X1, X0
	PADDL X2, X0
	ADDQ $16, BX
	ADDQ $32, CX
	DECQ AX
	JNZ scalaru8_amd64_loop16

scalaru8_amd64_reduce:
	// Horizontal signed int32 reduction of X0 into R13.
	MOVOU X0, X1
	PSRLO $8, X1
	PADDL X1, X0
	MOVOU X0, X1
	PSRLO $4, X1
	PADDL X1, X0
	MOVQ X0, AX
	MOVLQSX AX, AX
	ADDQ AX, R13

	ANDQ $15, DX
	JZ scalaru8_amd64_store

scalaru8_amd64_scalar_tail:
	MOVBQZX (BX), AX
	MOVWQSX (CX), R14
	IMULQ R14, AX
	ADDQ AX, R13
	INCQ BX
	ADDQ $2, CX
	DECQ DX
	JNZ scalaru8_amd64_scalar_tail

scalaru8_amd64_store:
	// centeredScore = 2*rawSum - 255*querySum.
	LEAQ (R13)(R13*1), AX
	MOVQ R12, R14
	SHLQ $8, R14
	SUBQ R12, R14
	SUBQ R14, AX
	MOVQ AX, (DI)
	ADDQ $8, DI
	DECQ R11
	JMP scalaru8_amd64_row_loop

scalaru8_amd64_done:
	RET

// func dotScalarU8CenteredIndexedAMD64AVX2(dst []int64, codes []byte, query []ScalarU8CenteredCode, rowIDs []uint32, dims int, rows int, querySum int64)
//
// Preconditions are checked by the Go wrapper: rows <= len(dst,rowIDs), dims > 0,
// len(query) >= dims, every row ID addresses a full row in codes, the CPU has
// AVX2, and dims <= 32768 so signed int32 vector accumulation cannot overflow.
// It computes centered scores as:
//   sum(q * (2*row - 255)) == 2*sum(q*row) - 255*querySum.
TEXT ·dotScalarU8CenteredIndexedAMD64AVX2(SB), NOSPLIT, $0-120
	MOVQ dst_base+0(FP), DI
	MOVQ codes_base+24(FP), SI
	MOVQ query_base+48(FP), R8
	MOVQ rowIDs_base+72(FP), R9
	MOVQ dims+96(FP), R10
	MOVQ rows+104(FP), R11
	MOVQ querySum+112(FP), R12

scalaru8_amd64_avx2_row_loop:
	TESTQ R11, R11
	JZ scalaru8_amd64_avx2_done

	MOVL (R9), AX                // row ID; MOVL zero-extends on amd64
	ADDQ $4, R9
	IMULQ R10, AX                // byte offset = rowID*dims
	LEAQ (SI)(AX*1), BX          // row pointer
	MOVQ R8, CX                  // query pointer
	MOVQ R10, DX                 // remaining dims for scalar tail mask
	XORQ R13, R13                // raw scalar accumulator after vector reduction
	VPXOR Y0, Y0, Y0             // packed int32 accumulator

	MOVQ DX, AX
	SHRQ $5, AX                  // 32 dimensions per AVX2 iteration
	JZ scalaru8_amd64_avx2_tail16

scalaru8_amd64_avx2_loop32:
	// Widen 32 row bytes to two uint16 YMM vectors and multiply by the matching
	// 32 int16 centered query values. VPMADDWD reduces adjacent products to int32.
	VPMOVZXBW (BX), Y1
	VPMOVZXBW 16(BX), Y2
	VMOVDQU (CX), Y3
	VMOVDQU 32(CX), Y4
	VPMADDWD Y3, Y1, Y1
	VPMADDWD Y4, Y2, Y2
	VPADDD Y1, Y0, Y0
	VPADDD Y2, Y0, Y0
	ADDQ $32, BX
	ADDQ $64, CX
	DECQ AX
	JNZ scalaru8_amd64_avx2_loop32

scalaru8_amd64_avx2_tail16:
	ANDQ $31, DX
	CMPQ DX, $16
	JL scalaru8_amd64_avx2_reduce
	VPMOVZXBW (BX), Y1
	VMOVDQU (CX), Y3
	VPMADDWD Y3, Y1, Y1
	VPADDD Y1, Y0, Y0
	ADDQ $16, BX
	ADDQ $32, CX
	SUBQ $16, DX

scalaru8_amd64_avx2_reduce:
	// Horizontal signed int32 reduction of Y0 into R13.
	VEXTRACTI128 $1, Y0, X1
	VPADDD X1, X0, X0
	VPSRLDQ $8, X0, X1
	VPADDD X1, X0, X0
	VPSRLDQ $4, X0, X1
	VPADDD X1, X0, X0
	VMOVQ X0, AX
	MOVLQSX AX, AX
	ADDQ AX, R13

	TESTQ DX, DX
	JZ scalaru8_amd64_avx2_store

scalaru8_amd64_avx2_scalar_tail:
	MOVBQZX (BX), AX
	MOVWQSX (CX), R14
	IMULQ R14, AX
	ADDQ AX, R13
	INCQ BX
	ADDQ $2, CX
	DECQ DX
	JNZ scalaru8_amd64_avx2_scalar_tail

scalaru8_amd64_avx2_store:
	// centeredScore = 2*rawSum - 255*querySum.
	LEAQ (R13)(R13*1), AX
	MOVQ R12, R14
	SHLQ $8, R14
	SUBQ R12, R14
	SUBQ R14, AX
	MOVQ AX, (DI)
	ADDQ $8, DI
	DECQ R11
	JMP scalaru8_amd64_avx2_row_loop

scalaru8_amd64_avx2_done:
	VZEROUPPER
	RET

// func dotScalarU8CenteredIndexedAMD64AVX512VNNI(dst []int64, codes []byte, query []ScalarU8CenteredCode, rowIDs []uint32, dims int, rows int, querySum int64)
//
// Preconditions match the AVX2 kernel, with AVX-512 F/BW/DQ/VL/VNNI required.
TEXT ·dotScalarU8CenteredIndexedAMD64AVX512VNNI(SB), NOSPLIT, $0-120
	MOVQ dst_base+0(FP), DI
	MOVQ codes_base+24(FP), SI
	MOVQ query_base+48(FP), R8
	MOVQ rowIDs_base+72(FP), R9
	MOVQ dims+96(FP), R10
	MOVQ rows+104(FP), R11
	MOVQ querySum+112(FP), R12

	// The 64D-aligned path reuses each query vector across four rows and keeps
	// two independent accumulators per row to break VPDPWSSD dependencies.
	MOVQ R10, AX
	ANDQ $63, AX
	JNZ scalaru8_amd64_avx512_row_loop

scalaru8_amd64_avx512_four_loop:
	CMPQ R11, $4
	JL scalaru8_amd64_avx512_pair_loop

	MOVL (R9), AX
	MOVL 4(R9), R15
	MOVL 8(R9), R14
	MOVL 12(R9), DX
	ADDQ $16, R9
	IMULQ R10, AX
	IMULQ R10, R15
	IMULQ R10, R14
	IMULQ R10, DX
	LEAQ (SI)(AX*1), BX
	LEAQ (SI)(R15*1), R15
	LEAQ (SI)(R14*1), R14
	LEAQ (SI)(DX*1), DX
	MOVQ R8, CX
	MOVQ R10, AX
	SHRQ $6, AX
	VPXORD Z0, Z0, Z0
	VPXORD Z5, Z5, Z5
	VPXORD Z8, Z8, Z8
	VPXORD Z9, Z9, Z9
	VPXORD Z10, Z10, Z10
	VPXORD Z11, Z11, Z11
	VPXORD Z12, Z12, Z12
	VPXORD Z13, Z13, Z13

scalaru8_amd64_avx512_four_loop64:
	VMOVDQU16 (CX), Z3
	VMOVDQU16 64(CX), Z4
	VPMOVZXBW (BX), Z1
	VPMOVZXBW 32(BX), Z2
	VPDPWSSD Z3, Z1, Z0
	VPDPWSSD Z4, Z2, Z5
	VPMOVZXBW (R15), Z1
	VPMOVZXBW 32(R15), Z2
	VPDPWSSD Z3, Z1, Z8
	VPDPWSSD Z4, Z2, Z9
	VPMOVZXBW (R14), Z1
	VPMOVZXBW 32(R14), Z2
	VPDPWSSD Z3, Z1, Z10
	VPDPWSSD Z4, Z2, Z11
	VPMOVZXBW (DX), Z1
	VPMOVZXBW 32(DX), Z2
	VPDPWSSD Z3, Z1, Z12
	VPDPWSSD Z4, Z2, Z13
	ADDQ $64, BX
	ADDQ $64, R15
	ADDQ $64, R14
	ADDQ $64, DX
	ADDQ $128, CX
	DECQ AX
	JNZ scalaru8_amd64_avx512_four_loop64

	VPADDD Z5, Z0, Z0
	VPADDD Z9, Z8, Z8
	VPADDD Z11, Z10, Z10
	VPADDD Z13, Z12, Z12
	MOVQ R12, R14
	SHLQ $8, R14
	SUBQ R12, R14
	VEXTRACTF32X8 $1, Z0, Y1
	VPADDD Y1, Y0, Y0
	VEXTRACTI128 $1, Y0, X1
	VPADDD X1, X0, X0
	VPSRLDQ $8, X0, X1
	VPADDD X1, X0, X0
	VPSRLDQ $4, X0, X1
	VPADDD X1, X0, X0
	VMOVQ X0, AX
	MOVLQSX AX, AX
	LEAQ (AX)(AX*1), AX
	SUBQ R14, AX
	MOVQ AX, (DI)

	VEXTRACTF32X8 $1, Z8, Y1
	VPADDD Y1, Y8, Y8
	VEXTRACTI128 $1, Y8, X1
	VPADDD X1, X8, X8
	VPSRLDQ $8, X8, X1
	VPADDD X1, X8, X8
	VPSRLDQ $4, X8, X1
	VPADDD X1, X8, X8
	VMOVQ X8, AX
	MOVLQSX AX, AX
	LEAQ (AX)(AX*1), AX
	SUBQ R14, AX
	MOVQ AX, 8(DI)

	VEXTRACTF32X8 $1, Z10, Y1
	VPADDD Y1, Y10, Y10
	VEXTRACTI128 $1, Y10, X1
	VPADDD X1, X10, X10
	VPSRLDQ $8, X10, X1
	VPADDD X1, X10, X10
	VPSRLDQ $4, X10, X1
	VPADDD X1, X10, X10
	VMOVQ X10, AX
	MOVLQSX AX, AX
	LEAQ (AX)(AX*1), AX
	SUBQ R14, AX
	MOVQ AX, 16(DI)

	VEXTRACTF32X8 $1, Z12, Y1
	VPADDD Y1, Y12, Y12
	VEXTRACTI128 $1, Y12, X1
	VPADDD X1, X12, X12
	VPSRLDQ $8, X12, X1
	VPADDD X1, X12, X12
	VPSRLDQ $4, X12, X1
	VPADDD X1, X12, X12
	VMOVQ X12, AX
	MOVLQSX AX, AX
	LEAQ (AX)(AX*1), AX
	SUBQ R14, AX
	MOVQ AX, 24(DI)

	ADDQ $32, DI
	SUBQ $4, R11
	JMP scalaru8_amd64_avx512_four_loop

scalaru8_amd64_avx512_pair_loop:
	CMPQ R11, $2
	JL scalaru8_amd64_avx512_row_loop

	MOVL (R9), AX
	MOVL 4(R9), DX
	ADDQ $8, R9
	IMULQ R10, AX
	IMULQ R10, DX
	LEAQ (SI)(AX*1), BX
	LEAQ (SI)(DX*1), R15
	MOVQ R8, CX
	MOVQ R10, AX
	SHRQ $6, AX
	VPXORD Z0, Z0, Z0
	VPXORD Z5, Z5, Z5
	VPXORD Z8, Z8, Z8
	VPXORD Z9, Z9, Z9

scalaru8_amd64_avx512_pair_loop64:
	VPMOVZXBW (BX), Z1
	VPMOVZXBW 32(BX), Z2
	VPMOVZXBW (R15), Z6
	VPMOVZXBW 32(R15), Z7
	VMOVDQU16 (CX), Z3
	VMOVDQU16 64(CX), Z4
	VPDPWSSD Z3, Z1, Z0
	VPDPWSSD Z4, Z2, Z5
	VPDPWSSD Z3, Z6, Z8
	VPDPWSSD Z4, Z7, Z9
	ADDQ $64, BX
	ADDQ $64, R15
	ADDQ $128, CX
	DECQ AX
	JNZ scalaru8_amd64_avx512_pair_loop64

	VPADDD Z5, Z0, Z0
	VEXTRACTF32X8 $1, Z0, Y1
	VPADDD Y1, Y0, Y0
	VEXTRACTI128 $1, Y0, X1
	VPADDD X1, X0, X0
	VPSRLDQ $8, X0, X1
	VPADDD X1, X0, X0
	VPSRLDQ $4, X0, X1
	VPADDD X1, X0, X0
	VMOVQ X0, AX
	MOVLQSX AX, R13

	VPADDD Z9, Z8, Z8
	VEXTRACTF32X8 $1, Z8, Y6
	VPADDD Y6, Y8, Y8
	VEXTRACTI128 $1, Y8, X6
	VPADDD X6, X8, X8
	VPSRLDQ $8, X8, X6
	VPADDD X6, X8, X8
	VPSRLDQ $4, X8, X6
	VPADDD X6, X8, X8
	VMOVQ X8, AX
	MOVLQSX AX, R14

	LEAQ (R13)(R13*1), AX
	MOVQ R12, DX
	SHLQ $8, DX
	SUBQ R12, DX
	SUBQ DX, AX
	MOVQ AX, (DI)
	LEAQ (R14)(R14*1), AX
	SUBQ DX, AX
	MOVQ AX, 8(DI)
	ADDQ $16, DI
	SUBQ $2, R11
	JMP scalaru8_amd64_avx512_pair_loop

scalaru8_amd64_avx512_row_loop:
	TESTQ R11, R11
	JZ scalaru8_amd64_avx512_done

	MOVL (R9), AX
	ADDQ $4, R9
	IMULQ R10, AX
	LEAQ (SI)(AX*1), BX
	MOVQ R8, CX
	MOVQ R10, DX
	XORQ R13, R13
	VPXORD Z0, Z0, Z0
	VPXORD Z5, Z5, Z5

	MOVQ DX, AX
	SHRQ $6, AX                  // 64 dimensions per AVX-512 iteration
	JZ scalaru8_amd64_avx512_tail32

scalaru8_amd64_avx512_loop64:
	VPMOVZXBW (BX), Z1
	VPMOVZXBW 32(BX), Z2
	VMOVDQU16 (CX), Z3
	VMOVDQU16 64(CX), Z4
	VPDPWSSD Z3, Z1, Z0
	VPDPWSSD Z4, Z2, Z5
	ADDQ $64, BX
	ADDQ $128, CX
	DECQ AX
	JNZ scalaru8_amd64_avx512_loop64

scalaru8_amd64_avx512_tail32:
	ANDQ $63, DX
	CMPQ DX, $32
	JL scalaru8_amd64_avx512_reduce
	VPMOVZXBW (BX), Z1
	VMOVDQU16 (CX), Z3
	VPDPWSSD Z3, Z1, Z0
	ADDQ $32, BX
	ADDQ $64, CX
	SUBQ $32, DX

scalaru8_amd64_avx512_reduce:
	VPADDD Z5, Z0, Z0
	VEXTRACTF32X8 $1, Z0, Y1
	VPADDD Y1, Y0, Y0
	VEXTRACTI128 $1, Y0, X1
	VPADDD X1, X0, X0
	VPSRLDQ $8, X0, X1
	VPADDD X1, X0, X0
	VPSRLDQ $4, X0, X1
	VPADDD X1, X0, X0
	VMOVQ X0, AX
	MOVLQSX AX, AX
	ADDQ AX, R13

	CMPQ DX, $16
	JL scalaru8_amd64_avx512_scalar_tail
	VPMOVZXBW (BX), Y1
	VMOVDQU (CX), Y3
	VPMADDWD Y3, Y1, Y1
	VEXTRACTI128 $1, Y1, X2
	VPADDD X2, X1, X1
	VPSRLDQ $8, X1, X2
	VPADDD X2, X1, X1
	VPSRLDQ $4, X1, X2
	VPADDD X2, X1, X1
	VMOVQ X1, AX
	MOVLQSX AX, AX
	ADDQ AX, R13
	ADDQ $16, BX
	ADDQ $32, CX
	SUBQ $16, DX

scalaru8_amd64_avx512_scalar_tail:
	TESTQ DX, DX
	JZ scalaru8_amd64_avx512_store

scalaru8_amd64_avx512_scalar_loop:
	MOVBQZX (BX), AX
	MOVWQSX (CX), R14
	IMULQ R14, AX
	ADDQ AX, R13
	INCQ BX
	ADDQ $2, CX
	DECQ DX
	JNZ scalaru8_amd64_avx512_scalar_loop

scalaru8_amd64_avx512_store:
	LEAQ (R13)(R13*1), AX
	MOVQ R12, R14
	SHLQ $8, R14
	SUBQ R12, R14
	SUBQ R14, AX
	MOVQ AX, (DI)
	ADDQ $8, DI
	DECQ R11
	JMP scalaru8_amd64_avx512_row_loop

scalaru8_amd64_avx512_done:
	VZEROUPPER
	RET
