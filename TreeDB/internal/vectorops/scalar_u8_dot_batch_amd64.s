//go:build amd64 && !purego

#include "textflag.h"

// func dotScalarU8CenteredIndexedAMD64(dst []int64, codes []byte, query []ScalarU8CenteredCode, rowIDs []uint32, dims int, rows int, querySum int64)
//
// Preconditions are checked by the Go wrapper: rows <= len(dst,rowIDs), dims > 0,
// len(query) >= dims, and every row ID addresses a full row in codes. querySum
// is sum(query[:dims]). The SSE2 kernel is called only for dims <= 32768 so its
// signed 32-bit vector accumulation cannot overflow. It computes centered scores
// as:
//   sum(q * (2*row - 255)) == 2*sum(q*row) - 255*querySum.
TEXT ·dotScalarU8CenteredIndexedAMD64(SB), NOSPLIT, $0-120
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
