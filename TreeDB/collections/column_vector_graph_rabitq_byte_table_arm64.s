//go:build arm64 && !purego

#include "textflag.h"

// func rabitqByteTableMismatchWeightARM64(querySignBits []byte, code []byte, byteMismatchWeights []float64, n int) float64
//
// Preconditions are checked by the Go caller: n == len(code) == len(querySignBits)
// and byteMismatchWeights contains n contiguous 256-entry float64 tables.
TEXT ·rabitqByteTableMismatchWeightARM64(SB), NOSPLIT, $0-88
	MOVD querySignBits_base+0(FP), R0
	MOVD code_base+24(FP), R1
	MOVD byteMismatchWeights_base+48(FP), R2
	MOVD n+72(FP), R3

	FMOVD ZR, F0
	FMOVD ZR, F1
	FMOVD ZR, F2
	FMOVD ZR, F3
	CBZ R3, rabitq_byte_table_arm64_reduce

	CMP $4, R3
	BLT rabitq_byte_table_arm64_tail

rabitq_byte_table_arm64_loop4:
	MOVBU 0(R1), R4
	MOVBU 0(R0), R5
	EOR R5, R4, R4
	FMOVD (R2)(R4<<3), F4
	FADDD F4, F0, F0

	MOVBU 1(R1), R4
	MOVBU 1(R0), R5
	EOR R5, R4, R4
	ADD $2048, R2, R6
	FMOVD (R6)(R4<<3), F4
	FADDD F4, F1, F1

	MOVBU 2(R1), R4
	MOVBU 2(R0), R5
	EOR R5, R4, R4
	ADD $4096, R2, R6
	FMOVD (R6)(R4<<3), F4
	FADDD F4, F2, F2

	MOVBU 3(R1), R4
	MOVBU 3(R0), R5
	EOR R5, R4, R4
	ADD $6144, R2, R6
	FMOVD (R6)(R4<<3), F4
	FADDD F4, F3, F3

	ADD $4, R0
	ADD $4, R1
	ADD $8192, R2
	SUB $4, R3
	CMP $4, R3
	BGE rabitq_byte_table_arm64_loop4

rabitq_byte_table_arm64_tail:
	CBZ R3, rabitq_byte_table_arm64_reduce
	MOVBU (R1), R4
	MOVBU (R0), R5
	EOR R5, R4, R4
	FMOVD (R2)(R4<<3), F4
	FADDD F4, F0, F0
	ADD $1, R0
	ADD $1, R1
	ADD $2048, R2
	SUB $1, R3
	B rabitq_byte_table_arm64_tail

rabitq_byte_table_arm64_reduce:
	FADDD F1, F0, F0
	FADDD F2, F0, F0
	FADDD F3, F0, F0
	FMOVD F0, ret+80(FP)
	RET
