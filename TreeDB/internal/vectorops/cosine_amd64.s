//go:build amd64 && !purego

#include "textflag.h"

// Convert FP32 components before arithmetic, then sum squared normalized
// differences in FP64. Separate multiplies preserve symmetry under reversal.
TEXT ·cosineSquaredDifferenceFloat32AVX(SB), NOSPLIT, $0-48
	MOVQ left+0(FP), SI
	MOVQ right+8(FP), DI
	MOVQ n+16(FP), CX
	VBROADCASTSD leftInvNorm+24(FP), Y6
	VBROADCASTSD rightInvNorm+32(FP), Y7
	VXORPD Y0, Y0, Y0
	VXORPD Y1, Y1, Y1

cosine_loop8:
	CMPQ CX, $8
	JL cosine_reduce
	VCVTPS2PD (SI), Y2
	VCVTPS2PD (DI), Y3
	VCVTPS2PD 16(SI), Y4
	VCVTPS2PD 16(DI), Y5
	VMULPD Y6, Y2, Y2
	VMULPD Y7, Y3, Y3
	VMULPD Y6, Y4, Y4
	VMULPD Y7, Y5, Y5
	VSUBPD Y3, Y2, Y2
	VSUBPD Y5, Y4, Y4
	VMULPD Y2, Y2, Y2
	VMULPD Y4, Y4, Y4
	VADDPD Y2, Y0, Y0
	VADDPD Y4, Y1, Y1
	ADDQ $32, SI
	ADDQ $32, DI
	SUBQ $8, CX
	JMP cosine_loop8

cosine_reduce:
	VADDPD Y1, Y0, Y0
	VEXTRACTF128 $1, Y0, X1
	VADDPD X1, X0, X0
	VUNPCKHPD X0, X0, X1
	VADDSD X1, X0, X0
	TESTQ CX, CX
	JZ cosine_done

cosine_tail:
	VCVTSS2SD (SI), X2, X2
	VCVTSS2SD (DI), X3, X3
	VMULSD X6, X2, X2
	VMULSD X7, X3, X3
	VSUBSD X3, X2, X2
	VMULSD X2, X2, X2
	VADDSD X2, X0, X0
	ADDQ $4, SI
	ADDQ $4, DI
	DECQ CX
	JNZ cosine_tail

cosine_done:
	VMOVSD X0, ret+40(FP)
	VZEROUPPER
	RET

// The same calculation with eight FP64 lanes on AVX-512.
TEXT ·cosineSquaredDifferenceFloat32AVX512(SB), NOSPLIT, $0-48
	MOVQ left+0(FP), SI
	MOVQ right+8(FP), DI
	MOVQ n+16(FP), CX
	VBROADCASTSD leftInvNorm+24(FP), Z6
	VBROADCASTSD rightInvNorm+32(FP), Z7
	VXORPD Z0, Z0, Z0
	VXORPD Z1, Z1, Z1

cosine512_loop16:
	CMPQ CX, $16
	JL cosine512_reduce
	VCVTPS2PD (SI), Z2
	VCVTPS2PD (DI), Z3
	VCVTPS2PD 32(SI), Z4
	VCVTPS2PD 32(DI), Z5
	VMULPD Z6, Z2, Z2
	VMULPD Z7, Z3, Z3
	VMULPD Z6, Z4, Z4
	VMULPD Z7, Z5, Z5
	VSUBPD Z3, Z2, Z2
	VSUBPD Z5, Z4, Z4
	VMULPD Z2, Z2, Z2
	VMULPD Z4, Z4, Z4
	VADDPD Z2, Z0, Z0
	VADDPD Z4, Z1, Z1
	ADDQ $64, SI
	ADDQ $64, DI
	SUBQ $16, CX
	JMP cosine512_loop16

cosine512_reduce:
	VADDPD Z1, Z0, Z0
	VEXTRACTF64X4 $1, Z0, Y1
	VADDPD Y1, Y0, Y0
	VEXTRACTF128 $1, Y0, X1
	VADDPD X1, X0, X0
	VUNPCKHPD X0, X0, X1
	VADDSD X1, X0, X0
	TESTQ CX, CX
	JZ cosine512_done

cosine512_tail:
	VCVTSS2SD (SI), X2, X2
	VCVTSS2SD (DI), X3, X3
	VMULSD X6, X2, X2
	VMULSD X7, X3, X3
	VSUBSD X3, X2, X2
	VMULSD X2, X2, X2
	VADDSD X2, X0, X0
	ADDQ $4, SI
	ADDQ $4, DI
	DECQ CX
	JNZ cosine512_tail

cosine512_done:
	VMOVSD X0, ret+40(FP)
	VZEROUPPER
	RET
