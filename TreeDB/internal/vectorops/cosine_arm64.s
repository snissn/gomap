//go:build arm64 && !purego

#include "textflag.h"

// FP32-to-FP64 conversion precedes all products. As in the amd64 and scalar
// paths, the two normalized products are rounded separately before subtraction.
TEXT ·cosineSquaredDifferenceFloat32ARM64(SB), NOSPLIT, $0-48
	MOVD left+0(FP), R0
	MOVD right+8(FP), R1
	MOVD n+16(FP), R2
	MOVD leftInvNorm+24(FP), R3
	VDUP R3, V6.D2
	MOVD rightInvNorm+32(FP), R3
	VDUP R3, V7.D2
	VEOR V0.B16, V0.B16, V0.B16
	VEOR V1.B16, V1.B16, V1.B16

cosine_arm64_loop4:
	CMP $4, R2
	BLT cosine_arm64_reduce
	VLD1.P 16(R0), [V2.S4]
	VLD1.P 16(R1), [V3.S4]
	WORD $0x0E617844 // FCVTL V4.2D, V2.2S
	WORD $0x4E617842 // FCVTL2 V2.2D, V2.4S
	WORD $0x0E617865 // FCVTL V5.2D, V3.2S
	WORD $0x4E617863 // FCVTL2 V3.2D, V3.4S
	WORD $0x6E66DC84 // FMUL V4.2D, V4.2D, V6.2D
	WORD $0x6E67DCA5 // FMUL V5.2D, V5.2D, V7.2D
	WORD $0x4EE5D484 // FSUB V4.2D, V4.2D, V5.2D
	WORD $0x6E64DC84 // FMUL V4.2D, V4.2D, V4.2D
	WORD $0x4E64D400 // FADD V0.2D, V0.2D, V4.2D
	WORD $0x6E66DC42 // FMUL V2.2D, V2.2D, V6.2D
	WORD $0x6E67DC63 // FMUL V3.2D, V3.2D, V7.2D
	WORD $0x4EE3D442 // FSUB V2.2D, V2.2D, V3.2D
	WORD $0x6E62DC42 // FMUL V2.2D, V2.2D, V2.2D
	WORD $0x4E62D421 // FADD V1.2D, V1.2D, V2.2D
	SUB $4, R2
	B cosine_arm64_loop4

cosine_arm64_reduce:
	WORD $0x4E61D400 // FADD V0.2D, V0.2D, V1.2D
	WORD $0x7E70D800 // FADDP D0, V0.2D
	CBZ R2, cosine_arm64_done

cosine_arm64_tail:
	FMOVS (R0), F2
	FMOVS (R1), F3
	FCVTSD F2, F2
	FCVTSD F3, F3
	FMULD F6, F2, F2
	FMULD F7, F3, F3
	FSUBD F3, F2, F2
	FMULD F2, F2, F2
	FADDD F2, F0, F0
	ADD $4, R0
	ADD $4, R1
	SUB $1, R2
	CBNZ R2, cosine_arm64_tail

cosine_arm64_done:
	FMOVD F0, ret+40(FP)
	RET
