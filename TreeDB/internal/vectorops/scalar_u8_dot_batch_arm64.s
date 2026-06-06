//go:build arm64 && !purego

#include "textflag.h"

// func dotScalarU8CenteredIndexedARM64(dst []int64, codes []byte, query []ScalarU8CenteredCode, rowIDs []uint32, dims int, rows int, querySum int64)
//
// Preconditions are checked by the Go wrapper: rows <= len(dst,rowIDs), dims > 0,
// len(query) >= dims, and every row ID addresses a full row in codes. querySum
// is sum(query[:dims]). The kernel computes centered scores as:
//   sum(q * (2*row - 255)) == 2*sum(q*row) - 255*querySum.
TEXT ·dotScalarU8CenteredIndexedARM64(SB), NOSPLIT, $0-120
	MOVD dst_base+0(FP), R0
	MOVD codes_base+24(FP), R1
	MOVD query_base+48(FP), R2
	MOVD rowIDs_base+72(FP), R3
	MOVD dims+96(FP), R4
	MOVD rows+104(FP), R5
	MOVD querySum+112(FP), R6

scalaru8_arm64_row_loop:
	CBZ R5, scalaru8_arm64_done
	MOVWU (R3), R7               // row ID
	ADD $4, R3
	MUL R4, R7, R8               // row offset in bytes (dims is byte width)
	ADD R1, R8, R9               // row pointer
	MOVD R2, R10                 // query pointer
	MOVD R4, R11                 // remaining dims

	// Initialize eight int64 vector accumulators. Products are accumulated as
	// int64 via SMULL+SADDLP, so SIMD accumulation is overflow-safe for all
	// practical Go slice-backed dimensions.
	VEOR V0.B16, V0.B16, V0.B16
	VEOR V1.B16, V1.B16, V1.B16
	VEOR V2.B16, V2.B16, V2.B16
	VEOR V3.B16, V3.B16, V3.B16
	VEOR V4.B16, V4.B16, V4.B16
	VEOR V5.B16, V5.B16, V5.B16
	VEOR V6.B16, V6.B16, V6.B16
	VEOR V7.B16, V7.B16, V7.B16

	CMP $32, R11
	BLT scalaru8_arm64_tail16

scalaru8_arm64_loop32:
	// Load 32 row bytes and 32 int16 centered query values.
	VLD1.P 16(R9), [V16.B16]
	VLD1.P 16(R9), [V17.B16]
	VLD1.P 32(R10), [V20.H8, V21.H8]
	VLD1.P 32(R10), [V22.H8, V23.H8]

	// Zero-extend row bytes to int16 lanes: V16 -> V24,V25; V17 -> V26,V27.
	VUXTL V16.B8, V24.H8
	VUXTL2 V16.B16, V25.H8
	VUXTL V17.B8, V26.H8
	VUXTL2 V17.B16, V27.H8

	// q(int16) * row(uint8 widened to positive int16) -> int32 products.
	WORD $0x0E78C29C              // SMULL V28.4S, V20.4H, V24.4H
	WORD $0x4E78C29D              // SMULL2 V29.4S, V20.8H, V24.8H
	WORD $0x0E79C2BE              // SMULL V30.4S, V21.4H, V25.4H
	WORD $0x4E79C2BF              // SMULL2 V31.4S, V21.8H, V25.8H
	WORD $0x4EA02B9C              // SADDLP V28.2D, V28.4S
	WORD $0x4EA02BBD              // SADDLP V29.2D, V29.4S
	WORD $0x4EA02BDE              // SADDLP V30.2D, V30.4S
	WORD $0x4EA02BFF              // SADDLP V31.2D, V31.4S
	WORD $0x4EFC8400              // ADD V0.2D, V0.2D, V28.2D
	WORD $0x4EFD8421              // ADD V1.2D, V1.2D, V29.2D
	WORD $0x4EFE8442              // ADD V2.2D, V2.2D, V30.2D
	WORD $0x4EFF8463              // ADD V3.2D, V3.2D, V31.2D

	WORD $0x0E7AC2DC              // SMULL V28.4S, V22.4H, V26.4H
	WORD $0x4E7AC2DD              // SMULL2 V29.4S, V22.8H, V26.8H
	WORD $0x0E7BC2FE              // SMULL V30.4S, V23.4H, V27.4H
	WORD $0x4E7BC2FF              // SMULL2 V31.4S, V23.8H, V27.8H
	WORD $0x4EA02B9C              // SADDLP V28.2D, V28.4S
	WORD $0x4EA02BBD              // SADDLP V29.2D, V29.4S
	WORD $0x4EA02BDE              // SADDLP V30.2D, V30.4S
	WORD $0x4EA02BFF              // SADDLP V31.2D, V31.4S
	WORD $0x4EFC8484              // ADD V4.2D, V4.2D, V28.2D
	WORD $0x4EFD84A5              // ADD V5.2D, V5.2D, V29.2D
	WORD $0x4EFE84C6              // ADD V6.2D, V6.2D, V30.2D
	WORD $0x4EFF84E7              // ADD V7.2D, V7.2D, V31.2D

	SUB $32, R11
	CMP $32, R11
	BGE scalaru8_arm64_loop32

scalaru8_arm64_tail16:
	CMP $16, R11
	BLT scalaru8_arm64_reduce

	VLD1.P 16(R9), [V16.B16]
	VLD1.P 32(R10), [V20.H8, V21.H8]
	VUXTL V16.B8, V24.H8
	VUXTL2 V16.B16, V25.H8

	WORD $0x0E78C29C              // SMULL V28.4S, V20.4H, V24.4H
	WORD $0x4E78C29D              // SMULL2 V29.4S, V20.8H, V24.8H
	WORD $0x0E79C2BE              // SMULL V30.4S, V21.4H, V25.4H
	WORD $0x4E79C2BF              // SMULL2 V31.4S, V21.8H, V25.8H
	WORD $0x4EA02B9C              // SADDLP V28.2D, V28.4S
	WORD $0x4EA02BBD              // SADDLP V29.2D, V29.4S
	WORD $0x4EA02BDE              // SADDLP V30.2D, V30.4S
	WORD $0x4EA02BFF              // SADDLP V31.2D, V31.4S
	WORD $0x4EFC8400              // ADD V0.2D, V0.2D, V28.2D
	WORD $0x4EFD8421              // ADD V1.2D, V1.2D, V29.2D
	WORD $0x4EFE8442              // ADD V2.2D, V2.2D, V30.2D
	WORD $0x4EFF8463              // ADD V3.2D, V3.2D, V31.2D
	SUB $16, R11

scalaru8_arm64_reduce:
	// Tree reduction of vector int64 accumulators to R12.
	WORD $0x4EE48400              // ADD V0.2D, V0.2D, V4.2D
	WORD $0x4EE58421              // ADD V1.2D, V1.2D, V5.2D
	WORD $0x4EE68442              // ADD V2.2D, V2.2D, V6.2D
	WORD $0x4EE78463              // ADD V3.2D, V3.2D, V7.2D
	WORD $0x4EE28400              // ADD V0.2D, V0.2D, V2.2D
	WORD $0x4EE38421              // ADD V1.2D, V1.2D, V3.2D
	WORD $0x4EE18400              // ADD V0.2D, V0.2D, V1.2D
	VMOV V0.D[0], R12
	VMOV V0.D[1], R13
	ADD R13, R12, R12

scalaru8_arm64_scalar_tail:
	CBZ R11, scalaru8_arm64_store
	MOVBU (R9), R13
	MOVH (R10), R14
	SXTH R14, R14
	MUL R13, R14, R13
	ADD R13, R12, R12
	ADD $1, R9
	ADD $2, R10
	SUB $1, R11
	B scalaru8_arm64_scalar_tail

scalaru8_arm64_store:
	// centeredScore = 2*rawSum - 255*querySum.
	MOVD R12, R13
	LSL $1, R13
	MOVD R6, R14
	LSL $8, R14
	SUB R6, R14, R14
	SUB R14, R13, R13
	MOVD R13, (R0)
	ADD $8, R0
	SUB $1, R5
	B scalaru8_arm64_row_loop

scalaru8_arm64_done:
	RET

// func dotScalarU8CenteredIndexedARM64Int32(dst []int64, codes []byte, query []ScalarU8CenteredCode, rowIDs []uint32, dims int, rows int, querySum int64)
//
// Preconditions are checked by the Go wrapper: rows <= len(dst,rowIDs), dims is
// within dotScalarU8CenteredIndexedARM64Int32MaxDims, len(query) >= dims, and
// every row ID addresses a full row in codes. This bounded-dimension variant
// keeps q*row partial sums in int32 vector lanes and widens once per row at the
// final reduction, avoiding the per-32-dimension SADDLP/int64 accumulation cost
// in the fully overflow-safe kernel above.
TEXT ·dotScalarU8CenteredIndexedARM64Int32(SB), NOSPLIT, $0-120
	MOVD dst_base+0(FP), R0
	MOVD codes_base+24(FP), R1
	MOVD query_base+48(FP), R2
	MOVD rowIDs_base+72(FP), R3
	MOVD dims+96(FP), R4
	MOVD rows+104(FP), R5
	MOVD querySum+112(FP), R6

scalaru8_arm64_i32_row_loop:
	CBZ R5, scalaru8_arm64_i32_done
	MOVWU (R3), R7               // row ID
	ADD $4, R3
	MUL R4, R7, R8               // row offset in bytes (dims is byte width)
	ADD R1, R8, R9               // row pointer
	MOVD R2, R10                 // query pointer
	MOVD R4, R11                 // remaining dims

	// Initialize eight int32 vector accumulators. The Go wrapper only dispatches
	// here for dimensions whose worst-case lane sums fit in signed int32.
	VEOR V0.B16, V0.B16, V0.B16
	VEOR V1.B16, V1.B16, V1.B16
	VEOR V2.B16, V2.B16, V2.B16
	VEOR V3.B16, V3.B16, V3.B16
	VEOR V4.B16, V4.B16, V4.B16
	VEOR V5.B16, V5.B16, V5.B16
	VEOR V6.B16, V6.B16, V6.B16
	VEOR V7.B16, V7.B16, V7.B16

	CMP $32, R11
	BLT scalaru8_arm64_i32_tail16

scalaru8_arm64_i32_loop32:
	// Load 32 row bytes and 32 int16 centered query values.
	VLD1.P 16(R9), [V16.B16]
	VLD1.P 16(R9), [V17.B16]
	VLD1.P 32(R10), [V20.H8, V21.H8]
	VLD1.P 32(R10), [V22.H8, V23.H8]

	// Zero-extend row bytes to int16 lanes: V16 -> V24,V25; V17 -> V26,V27.
	VUXTL V16.B8, V24.H8
	VUXTL2 V16.B16, V25.H8
	VUXTL V17.B8, V26.H8
	VUXTL2 V17.B16, V27.H8

	// q(int16) * row(uint8 widened to positive int16) accumulates into int32.
	WORD $0x0E788280              // SMLAL V0.4S, V20.4H, V24.4H
	WORD $0x4E788281              // SMLAL2 V1.4S, V20.8H, V24.8H
	WORD $0x0E7982A2              // SMLAL V2.4S, V21.4H, V25.4H
	WORD $0x4E7982A3              // SMLAL2 V3.4S, V21.8H, V25.8H
	WORD $0x0E7A82C4              // SMLAL V4.4S, V22.4H, V26.4H
	WORD $0x4E7A82C5              // SMLAL2 V5.4S, V22.8H, V26.8H
	WORD $0x0E7B82E6              // SMLAL V6.4S, V23.4H, V27.4H
	WORD $0x4E7B82E7              // SMLAL2 V7.4S, V23.8H, V27.8H

	SUB $32, R11
	CMP $32, R11
	BGE scalaru8_arm64_i32_loop32

scalaru8_arm64_i32_tail16:
	CMP $16, R11
	BLT scalaru8_arm64_i32_reduce

	VLD1.P 16(R9), [V16.B16]
	VLD1.P 32(R10), [V20.H8, V21.H8]
	VUXTL V16.B8, V24.H8
	VUXTL2 V16.B16, V25.H8

	WORD $0x0E788280              // SMLAL V0.4S, V20.4H, V24.4H
	WORD $0x4E788281              // SMLAL2 V1.4S, V20.8H, V24.8H
	WORD $0x0E7982A2              // SMLAL V2.4S, V21.4H, V25.4H
	WORD $0x4E7982A3              // SMLAL2 V3.4S, V21.8H, V25.8H
	SUB $16, R11

scalaru8_arm64_i32_reduce:
	// Widen each int32 accumulator to int64 once per row, then reuse the same
	// int64 tree reduction shape as the overflow-safe kernel.
	WORD $0x4EA02800              // SADDLP V0.2D, V0.4S
	WORD $0x4EA02821              // SADDLP V1.2D, V1.4S
	WORD $0x4EA02842              // SADDLP V2.2D, V2.4S
	WORD $0x4EA02863              // SADDLP V3.2D, V3.4S
	WORD $0x4EA02884              // SADDLP V4.2D, V4.4S
	WORD $0x4EA028A5              // SADDLP V5.2D, V5.4S
	WORD $0x4EA028C6              // SADDLP V6.2D, V6.4S
	WORD $0x4EA028E7              // SADDLP V7.2D, V7.4S
	WORD $0x4EE48400              // ADD V0.2D, V0.2D, V4.2D
	WORD $0x4EE58421              // ADD V1.2D, V1.2D, V5.2D
	WORD $0x4EE68442              // ADD V2.2D, V2.2D, V6.2D
	WORD $0x4EE78463              // ADD V3.2D, V3.2D, V7.2D
	WORD $0x4EE28400              // ADD V0.2D, V0.2D, V2.2D
	WORD $0x4EE38421              // ADD V1.2D, V1.2D, V3.2D
	WORD $0x4EE18400              // ADD V0.2D, V0.2D, V1.2D
	VMOV V0.D[0], R12
	VMOV V0.D[1], R13
	ADD R13, R12, R12

scalaru8_arm64_i32_scalar_tail:
	CBZ R11, scalaru8_arm64_i32_store
	MOVBU (R9), R13
	MOVH (R10), R14
	SXTH R14, R14
	MUL R13, R14, R13
	ADD R13, R12, R12
	ADD $1, R9
	ADD $2, R10
	SUB $1, R11
	B scalaru8_arm64_i32_scalar_tail

scalaru8_arm64_i32_store:
	// centeredScore = 2*rawSum - 255*querySum.
	MOVD R12, R13
	LSL $1, R13
	MOVD R6, R14
	LSL $8, R14
	SUB R6, R14, R14
	SUB R14, R13, R13
	MOVD R13, (R0)
	ADD $8, R0
	SUB $1, R5
	B scalaru8_arm64_i32_row_loop

scalaru8_arm64_i32_done:
	RET
