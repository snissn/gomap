//go:build arm64 && !purego

#include "textflag.h"

// func dotFloat32IndexedARM64(dst []float32, base []float32, query []float32, rowIDs []uint32, dims int, rows int)
//
// Preconditions are checked by the Go wrapper: rows <= len(dst,rowIDs), dims > 0,
// len(query) >= dims, and every row ID addresses a full row in base.
TEXT ·dotFloat32IndexedARM64(SB), NOSPLIT, $0-112
	MOVD dst_base+0(FP), R0
	MOVD base_base+24(FP), R3
	MOVD query_base+48(FP), R4
	MOVD rowIDs_base+72(FP), R5
	MOVD dims+96(FP), R6
	MOVD rows+104(FP), R7

indexed_arm64_row_loop:
	CBZ R7, indexed_arm64_done
	MOVWU (R5), R8               // row ID
	ADD $4, R5
	MUL R6, R8, R9               // row offset in float32 elements
	LSL $2, R9
	ADD R3, R9, R10              // row pointer
	MOVD R4, R11                 // query pointer
	MOVD R6, R12                 // remaining dims

	// Initialize 16 accumulators to zero. This mirrors the per-row NEON dot
	// kernel but keeps all row dispatch inside one indexed batch function.
	VEOR V0.B16, V0.B16, V0.B16
	VEOR V1.B16, V1.B16, V1.B16
	VEOR V2.B16, V2.B16, V2.B16
	VEOR V3.B16, V3.B16, V3.B16
	VEOR V4.B16, V4.B16, V4.B16
	VEOR V5.B16, V5.B16, V5.B16
	VEOR V6.B16, V6.B16, V6.B16
	VEOR V7.B16, V7.B16, V7.B16
	VEOR V8.B16, V8.B16, V8.B16
	VEOR V9.B16, V9.B16, V9.B16
	VEOR V10.B16, V10.B16, V10.B16
	VEOR V11.B16, V11.B16, V11.B16
	VEOR V12.B16, V12.B16, V12.B16
	VEOR V13.B16, V13.B16, V13.B16
	VEOR V14.B16, V14.B16, V14.B16
	VEOR V15.B16, V15.B16, V15.B16

	CMP $64, R12
	BLT indexed_arm64_tail32

indexed_arm64_loop64:
	// Load 64 row/query elements and FMLA into 16 vector accumulators.
	VLD1.P 64(R10), [V16.S4, V17.S4, V18.S4, V19.S4]
	VLD1.P 64(R11), [V24.S4, V25.S4, V26.S4, V27.S4]
	VLD1.P 64(R10), [V20.S4, V21.S4, V22.S4, V23.S4]
	VLD1.P 64(R11), [V28.S4, V29.S4, V30.S4, V31.S4]

	WORD $0x4E38CE00              // FMLA V0.4S, V16.4S, V24.4S
	WORD $0x4E39CE21              // FMLA V1.4S, V17.4S, V25.4S
	WORD $0x4E3ACE42              // FMLA V2.4S, V18.4S, V26.4S
	WORD $0x4E3BCE63              // FMLA V3.4S, V19.4S, V27.4S
	WORD $0x4E3CCE84              // FMLA V4.4S, V20.4S, V28.4S
	WORD $0x4E3DCEA5              // FMLA V5.4S, V21.4S, V29.4S
	WORD $0x4E3ECEC6              // FMLA V6.4S, V22.4S, V30.4S
	WORD $0x4E3FCEE7              // FMLA V7.4S, V23.4S, V31.4S

	VLD1.P 64(R10), [V16.S4, V17.S4, V18.S4, V19.S4]
	VLD1.P 64(R11), [V24.S4, V25.S4, V26.S4, V27.S4]
	VLD1.P 64(R10), [V20.S4, V21.S4, V22.S4, V23.S4]
	VLD1.P 64(R11), [V28.S4, V29.S4, V30.S4, V31.S4]

	WORD $0x4E38CE08              // FMLA V8.4S, V16.4S, V24.4S
	WORD $0x4E39CE29              // FMLA V9.4S, V17.4S, V25.4S
	WORD $0x4E3ACE4A              // FMLA V10.4S, V18.4S, V26.4S
	WORD $0x4E3BCE6B              // FMLA V11.4S, V19.4S, V27.4S
	WORD $0x4E3CCE8C              // FMLA V12.4S, V20.4S, V28.4S
	WORD $0x4E3DCEAD              // FMLA V13.4S, V21.4S, V29.4S
	WORD $0x4E3ECECE              // FMLA V14.4S, V22.4S, V30.4S
	WORD $0x4E3FCEEF              // FMLA V15.4S, V23.4S, V31.4S

	SUB $64, R12
	CMP $64, R12
	BGE indexed_arm64_loop64

	// Tree reduction of 16 accumulators to V0.
	WORD $0x4E28D400              // FADD V0.4S, V0.4S, V8.4S
	WORD $0x4E29D421              // FADD V1.4S, V1.4S, V9.4S
	WORD $0x4E2AD442              // FADD V2.4S, V2.4S, V10.4S
	WORD $0x4E2BD463              // FADD V3.4S, V3.4S, V11.4S
	WORD $0x4E2CD484              // FADD V4.4S, V4.4S, V12.4S
	WORD $0x4E2DD4A5              // FADD V5.4S, V5.4S, V13.4S
	WORD $0x4E2ED4C6              // FADD V6.4S, V6.4S, V14.4S
	WORD $0x4E2FD4E7              // FADD V7.4S, V7.4S, V15.4S
	WORD $0x4E24D400              // FADD V0.4S, V0.4S, V4.4S
	WORD $0x4E25D421              // FADD V1.4S, V1.4S, V5.4S
	WORD $0x4E26D442              // FADD V2.4S, V2.4S, V6.4S
	WORD $0x4E27D463              // FADD V3.4S, V3.4S, V7.4S
	WORD $0x4E22D400              // FADD V0.4S, V0.4S, V2.4S
	WORD $0x4E23D421              // FADD V1.4S, V1.4S, V3.4S
	WORD $0x4E21D400              // FADD V0.4S, V0.4S, V1.4S

indexed_arm64_tail32:
	CMP $32, R12
	BLT indexed_arm64_tail16
	VLD1.P 64(R10), [V16.S4, V17.S4, V18.S4, V19.S4]
	VLD1.P 64(R11), [V24.S4, V25.S4, V26.S4, V27.S4]
	VLD1.P 64(R10), [V20.S4, V21.S4, V22.S4, V23.S4]
	VLD1.P 64(R11), [V28.S4, V29.S4, V30.S4, V31.S4]
	WORD $0x4E38CE00              // FMLA V0.4S, V16.4S, V24.4S
	WORD $0x4E39CE20              // FMLA V0.4S, V17.4S, V25.4S
	WORD $0x4E3ACE40              // FMLA V0.4S, V18.4S, V26.4S
	WORD $0x4E3BCE60              // FMLA V0.4S, V19.4S, V27.4S
	WORD $0x4E3CCE80              // FMLA V0.4S, V20.4S, V28.4S
	WORD $0x4E3DCEA0              // FMLA V0.4S, V21.4S, V29.4S
	WORD $0x4E3ECEC0              // FMLA V0.4S, V22.4S, V30.4S
	WORD $0x4E3FCEE0              // FMLA V0.4S, V23.4S, V31.4S
	SUB $32, R12

indexed_arm64_tail16:
	CMP $16, R12
	BLT indexed_arm64_tail8
	VLD1.P 64(R10), [V16.S4, V17.S4, V18.S4, V19.S4]
	VLD1.P 64(R11), [V24.S4, V25.S4, V26.S4, V27.S4]
	WORD $0x4E38CE00              // FMLA V0.4S, V16.4S, V24.4S
	WORD $0x4E39CE20              // FMLA V0.4S, V17.4S, V25.4S
	WORD $0x4E3ACE40              // FMLA V0.4S, V18.4S, V26.4S
	WORD $0x4E3BCE60              // FMLA V0.4S, V19.4S, V27.4S
	SUB $16, R12

indexed_arm64_tail8:
	CMP $8, R12
	BLT indexed_arm64_tail4
	VLD1.P 32(R10), [V16.S4, V17.S4]
	VLD1.P 32(R11), [V24.S4, V25.S4]
	WORD $0x4E38CE00              // FMLA V0.4S, V16.4S, V24.4S
	WORD $0x4E39CE20              // FMLA V0.4S, V17.4S, V25.4S
	SUB $8, R12

indexed_arm64_tail4:
	CMP $4, R12
	BLT indexed_arm64_reduce
	VLD1.P 16(R10), [V16.S4]
	VLD1.P 16(R11), [V24.S4]
	WORD $0x4E38CE00              // FMLA V0.4S, V16.4S, V24.4S
	SUB $4, R12

indexed_arm64_reduce:
	WORD $0x6E20D400              // FADDP V0.4S, V0.4S, V0.4S
	WORD $0x7E30D800              // FADDP S0, V0.2S

indexed_arm64_scalar_loop:
	CBZ R12, indexed_arm64_store
	FMOVS (R10), F1
	FMOVS (R11), F2
	FMULS F1, F2, F1
	FADDS F0, F1, F0
	ADD $4, R10
	ADD $4, R11
	SUB $1, R12
	B indexed_arm64_scalar_loop

indexed_arm64_store:
	FMOVS F0, (R0)
	ADD $4, R0
	SUB $1, R7
	B indexed_arm64_row_loop

indexed_arm64_done:
	RET
