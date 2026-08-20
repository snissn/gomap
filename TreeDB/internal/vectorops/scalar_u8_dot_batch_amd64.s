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

	MOVQ DX, AX
	SHRQ $6, AX                  // 64 dimensions per AVX-512 iteration
	JZ scalaru8_amd64_avx512_tail32

scalaru8_amd64_avx512_loop64:
	VPMOVZXBW (BX), Z1
	VPMOVZXBW 32(BX), Z2
	VMOVDQU16 (CX), Z3
	VMOVDQU16 64(CX), Z4
	VPDPWSSD Z3, Z1, Z0
	VPDPWSSD Z4, Z2, Z0
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
