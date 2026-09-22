//go:build amd64 && !purego

#include "textflag.h"

#define SCALARU8_BYTE_VNNI_FOUR(offset) \
	VMOVDQU8 offset(CX), Z3 \
	VMOVDQU8 offset(BX), Z1 \
	VPDPBUSD Z3, Z1, Z0 \
	VMOVDQU8 offset(R15), Z1 \
	VPDPBUSD Z3, Z1, Z8 \
	VMOVDQU8 offset(R14), Z1 \
	VPDPBUSD Z3, Z1, Z10 \
	VMOVDQU8 offset(DX), Z1 \
	VPDPBUSD Z3, Z1, Z12

#define SCALARU8_BYTE_VNNI_PAIR(offset) \
	VMOVDQU8 offset(CX), Z3 \
	VMOVDQU8 offset(BX), Z1 \
	VPDPBUSD Z3, Z1, Z0 \
	VMOVDQU8 offset(R15), Z1 \
	VPDPBUSD Z3, Z1, Z8

// func dotScalarU8CenteredIndexedPreparedByte(dst []int64, codes []byte, queryHalf []int8, rowByteSums []uint32, rowIDs []uint32, dims int, rows int, querySum int64)
//
// Prepared-byte AVX-512 VNNI kernel. queryHalf[i] is the arithmetic
// half of the odd centered query value, and rowSums contains each raw byte-row
// sum. The exact centered score is:
//
//   4*sum(queryHalf*row) + 2*sum(row) - 255*sum(query)
//
// Preconditions are established by the focused tests: rows <= len(dst,rowIDs),
// dims > 0 and divisible by 64, all rows are in range, and side arrays match.
TEXT ·dotScalarU8CenteredIndexedPreparedByte(SB), NOSPLIT, $0-144
	MOVQ dst_base+0(FP), DI
	MOVQ codes_base+24(FP), SI
	MOVQ queryHalf_base+48(FP), R8
	MOVQ rowIDs_base+96(FP), R9
	MOVQ dims+120(FP), R10
	MOVQ rows+128(FP), R11
	MOVQ querySum+136(FP), R12

scalaru8_byte_four_loop:
	CMPQ R11, $4
	JL scalaru8_byte_pair_loop

	MOVL (R9), AX
	MOVL 4(R9), R15
	MOVL 8(R9), R14
	MOVL 12(R9), DX
	ADDQ $16, R9
	// Start the four independent row-sum reads before the long code scan and
	// retain them in otherwise-unused vector registers. This hides side-plane
	// latency instead of serializing it after each horizontal reduction.
	MOVQ rowByteSums_base+72(FP), BX
	MOVL (BX)(AX*4), R13
	VMOVQ R13, X20
	MOVL (BX)(R15*4), R13
	VMOVQ R13, X21
	MOVL (BX)(R14*4), R13
	VMOVQ R13, X22
	MOVL (BX)(DX*4), R13
	VMOVQ R13, X23
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
	VPXORD Z8, Z8, Z8
	VPXORD Z10, Z10, Z10
	VPXORD Z12, Z12, Z12
	CMPQ R10, $768
	JE scalaru8_byte_four_unrolled768

scalaru8_byte_four_loop64:
	VMOVDQU8 (CX), Z3
	VMOVDQU8 (BX), Z1
	VPDPBUSD Z3, Z1, Z0
	VMOVDQU8 (R15), Z1
	VPDPBUSD Z3, Z1, Z8
	VMOVDQU8 (R14), Z1
	VPDPBUSD Z3, Z1, Z10
	VMOVDQU8 (DX), Z1
	VPDPBUSD Z3, Z1, Z12
	ADDQ $64, BX
	ADDQ $64, R15
	ADDQ $64, R14
	ADDQ $64, DX
	ADDQ $64, CX
	DECQ AX
	JNZ scalaru8_byte_four_loop64
	JMP scalaru8_byte_four_reduce

scalaru8_byte_four_unrolled768:
	SCALARU8_BYTE_VNNI_FOUR(0)
	SCALARU8_BYTE_VNNI_FOUR(64)
	SCALARU8_BYTE_VNNI_FOUR(128)
	SCALARU8_BYTE_VNNI_FOUR(192)
	SCALARU8_BYTE_VNNI_FOUR(256)
	SCALARU8_BYTE_VNNI_FOUR(320)
	SCALARU8_BYTE_VNNI_FOUR(384)
	SCALARU8_BYTE_VNNI_FOUR(448)
	SCALARU8_BYTE_VNNI_FOUR(512)
	SCALARU8_BYTE_VNNI_FOUR(576)
	SCALARU8_BYTE_VNNI_FOUR(640)
	SCALARU8_BYTE_VNNI_FOUR(704)

scalaru8_byte_four_reduce:
	// Reduce four rows in parallel to retain independent dependency chains.
	VEXTRACTF32X8 $1, Z0, Y1
	VEXTRACTF32X8 $1, Z8, Y2
	VEXTRACTF32X8 $1, Z10, Y3
	VEXTRACTF32X8 $1, Z12, Y4
	VPADDD Y1, Y0, Y0
	VPADDD Y2, Y8, Y8
	VPADDD Y3, Y10, Y10
	VPADDD Y4, Y12, Y12
	VEXTRACTI128 $1, Y0, X1
	VEXTRACTI128 $1, Y8, X2
	VEXTRACTI128 $1, Y10, X3
	VEXTRACTI128 $1, Y12, X4
	VPADDD X1, X0, X0
	VPADDD X2, X8, X8
	VPADDD X3, X10, X10
	VPADDD X4, X12, X12
	VPSRLDQ $8, X0, X1
	VPSRLDQ $8, X8, X2
	VPSRLDQ $8, X10, X3
	VPSRLDQ $8, X12, X4
	VPADDD X1, X0, X0
	VPADDD X2, X8, X8
	VPADDD X3, X10, X10
	VPADDD X4, X12, X12
	VPSRLDQ $4, X0, X1
	VPSRLDQ $4, X8, X2
	VPSRLDQ $4, X10, X3
	VPSRLDQ $4, X12, X4
	VPADDD X1, X0, X0
	VPADDD X2, X8, X8
	VPADDD X3, X10, X10
	VPADDD X4, X12, X12

	MOVQ R12, R14
	SHLQ $8, R14
	SUBQ R12, R14

	VMOVQ X0, AX
	MOVLQSX AX, AX
	SHLQ $2, AX
	VMOVQ X20, R13
	LEAQ (AX)(R13*2), AX
	SUBQ R14, AX
	MOVQ AX, (DI)

	VMOVQ X8, AX
	MOVLQSX AX, AX
	SHLQ $2, AX
	VMOVQ X21, R13
	LEAQ (AX)(R13*2), AX
	SUBQ R14, AX
	MOVQ AX, 8(DI)

	VMOVQ X10, AX
	MOVLQSX AX, AX
	SHLQ $2, AX
	VMOVQ X22, R13
	LEAQ (AX)(R13*2), AX
	SUBQ R14, AX
	MOVQ AX, 16(DI)

	VMOVQ X12, AX
	MOVLQSX AX, AX
	SHLQ $2, AX
	VMOVQ X23, R13
	LEAQ (AX)(R13*2), AX
	SUBQ R14, AX
	MOVQ AX, 24(DI)

	ADDQ $32, DI
	SUBQ $4, R11
	JMP scalaru8_byte_four_loop

scalaru8_byte_pair_loop:
	CMPQ R11, $2
	JL scalaru8_byte_row_loop

	MOVL (R9), AX
	MOVL 4(R9), DX
	ADDQ $8, R9
	MOVQ rowByteSums_base+72(FP), BX
	MOVL (BX)(AX*4), R13
	VMOVQ R13, X20
	MOVL (BX)(DX*4), R13
	VMOVQ R13, X21
	IMULQ R10, AX
	IMULQ R10, DX
	LEAQ (SI)(AX*1), BX
	LEAQ (SI)(DX*1), R15
	MOVQ R8, CX
	MOVQ R10, AX
	SHRQ $6, AX
	VPXORD Z0, Z0, Z0
	VPXORD Z8, Z8, Z8
	CMPQ R10, $768
	JE scalaru8_byte_pair_unrolled768

scalaru8_byte_pair_loop64:
	VMOVDQU8 (CX), Z3
	VMOVDQU8 (BX), Z1
	VPDPBUSD Z3, Z1, Z0
	VMOVDQU8 (R15), Z1
	VPDPBUSD Z3, Z1, Z8
	ADDQ $64, BX
	ADDQ $64, R15
	ADDQ $64, CX
	DECQ AX
	JNZ scalaru8_byte_pair_loop64
	JMP scalaru8_byte_pair_reduce

scalaru8_byte_pair_unrolled768:
	SCALARU8_BYTE_VNNI_PAIR(0)
	SCALARU8_BYTE_VNNI_PAIR(64)
	SCALARU8_BYTE_VNNI_PAIR(128)
	SCALARU8_BYTE_VNNI_PAIR(192)
	SCALARU8_BYTE_VNNI_PAIR(256)
	SCALARU8_BYTE_VNNI_PAIR(320)
	SCALARU8_BYTE_VNNI_PAIR(384)
	SCALARU8_BYTE_VNNI_PAIR(448)
	SCALARU8_BYTE_VNNI_PAIR(512)
	SCALARU8_BYTE_VNNI_PAIR(576)
	SCALARU8_BYTE_VNNI_PAIR(640)
	SCALARU8_BYTE_VNNI_PAIR(704)

scalaru8_byte_pair_reduce:
	VEXTRACTF32X8 $1, Z0, Y1
	VPADDD Y1, Y0, Y0
	VEXTRACTI128 $1, Y0, X1
	VPADDD X1, X0, X0
	VPSRLDQ $8, X0, X1
	VPADDD X1, X0, X0
	VPSRLDQ $4, X0, X1
	VPADDD X1, X0, X0
	VEXTRACTF32X8 $1, Z8, Y2
	VPADDD Y2, Y8, Y8
	VEXTRACTI128 $1, Y8, X2
	VPADDD X2, X8, X8
	VPSRLDQ $8, X8, X2
	VPADDD X2, X8, X8
	VPSRLDQ $4, X8, X2
	VPADDD X2, X8, X8

	MOVQ R12, R14
	SHLQ $8, R14
	SUBQ R12, R14
	VMOVQ X0, AX
	MOVLQSX AX, AX
	SHLQ $2, AX
	VMOVQ X20, R13
	LEAQ (AX)(R13*2), AX
	SUBQ R14, AX
	MOVQ AX, (DI)
	VMOVQ X8, AX
	MOVLQSX AX, AX
	SHLQ $2, AX
	VMOVQ X21, R13
	LEAQ (AX)(R13*2), AX
	SUBQ R14, AX
	MOVQ AX, 8(DI)
	ADDQ $16, DI
	SUBQ $2, R11
	JMP scalaru8_byte_pair_loop

scalaru8_byte_row_loop:
	TESTQ R11, R11
	JZ scalaru8_byte_done

	MOVL (R9), AX
	ADDQ $4, R9
	MOVQ rowByteSums_base+72(FP), BX
	MOVL (BX)(AX*4), R13
	VMOVQ R13, X20
	IMULQ R10, AX
	LEAQ (SI)(AX*1), BX
	MOVQ R8, CX
	MOVQ R10, AX
	SHRQ $6, AX
	VPXORD Z0, Z0, Z0

scalaru8_byte_row_loop64:
	VMOVDQU8 (CX), Z3
	VMOVDQU8 (BX), Z1
	VPDPBUSD Z3, Z1, Z0
	ADDQ $64, BX
	ADDQ $64, CX
	DECQ AX
	JNZ scalaru8_byte_row_loop64

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
	SHLQ $2, AX
	VMOVQ X20, R13
	LEAQ (AX)(R13*2), AX
	MOVQ R12, R14
	SHLQ $8, R14
	SUBQ R12, R14
	SUBQ R14, AX
	MOVQ AX, (DI)
	ADDQ $8, DI
	DECQ R11
	JMP scalaru8_byte_row_loop

scalaru8_byte_done:
	VZEROUPPER
	RET
