#include "textflag.h"

// func scanSpecial8AVX512(ptr unsafe.Pointer, target uint64) uint64
TEXT ·scanSpecial8AVX512(SB), NOSPLIT, $0-24
	MOVQ	ptr+0(FP), SI
	MOVQ	target+8(FP), DX

	// Broadcast target into Z0.
	MOVQ	DX, X0
	VPBROADCASTQ	X0, Z0

	// Broadcast tombstone (-1) into Z1.
	MOVQ	$-1, AX
	MOVQ	AX, X1
	VPBROADCASTQ	X1, Z1

	// Zero Z2.
	VPXORQ	Z2, Z2, Z2

	// Load 8 hashes into Z3.
	VMOVDQU64	(SI), Z3

	// Compare: target, empty (0), tombstone (-1).
	VPCMPEQQ	Z0, Z3, K1
	VPCMPEQQ	Z2, Z3, K2
	VPCMPEQQ	Z1, Z3, K3

	// Combine masks in AX (bits 0..7).
	KMOVQ	K1, AX
	KMOVQ	K2, BX
	ORQ		BX, AX
	KMOVQ	K3, BX
	ORQ		BX, AX

	MOVQ	AX, ret+16(FP)
	VZEROUPPER
	RET

