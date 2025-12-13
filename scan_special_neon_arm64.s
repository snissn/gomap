#include "textflag.h"

// func scanSpecial8NEON(ptr unsafe.Pointer, target uint64) uint64
TEXT ·scanSpecial8NEON(SB), NOSPLIT, $0-24
	MOVD	ptr+0(FP), R0
	MOVD	target+8(FP), R1

	// Broadcast target and tombstone to all lanes.
	VMOV	R1, V0.D2
	MOVD	$-1, R2
	VMOV	R2, V1.D2
	VEOR	V2.B16, V2.B16, V2.B16 // V2 = 0

	// Load 8 hashes (4x 128-bit vectors).
	VLD1	(R0), [V3.D2, V4.D2, V5.D2, V6.D2]

	// V7 = interesting mask for lanes 0-1.
	VCMEQ	V0.D2, V3.D2, V7.D2
	VCMEQ	V2.D2, V3.D2, V8.D2
	VCMEQ	V1.D2, V3.D2, V9.D2
	VORR	V8.B16, V7.B16, V7.B16
	VORR	V9.B16, V7.B16, V7.B16

	// V10 = interesting mask for lanes 2-3.
	VCMEQ	V0.D2, V4.D2, V10.D2
	VCMEQ	V2.D2, V4.D2, V11.D2
	VCMEQ	V1.D2, V4.D2, V12.D2
	VORR	V11.B16, V10.B16, V10.B16
	VORR	V12.B16, V10.B16, V10.B16

	// V13 = interesting mask for lanes 4-5.
	VCMEQ	V0.D2, V5.D2, V13.D2
	VCMEQ	V2.D2, V5.D2, V14.D2
	VCMEQ	V1.D2, V5.D2, V15.D2
	VORR	V14.B16, V13.B16, V13.B16
	VORR	V15.B16, V13.B16, V13.B16

	// V16 = interesting mask for lanes 6-7.
	VCMEQ	V0.D2, V6.D2, V16.D2
	VCMEQ	V2.D2, V6.D2, V17.D2
	VCMEQ	V1.D2, V6.D2, V18.D2
	VORR	V17.B16, V16.B16, V16.B16
	VORR	V18.B16, V16.B16, V16.B16

	// Build bitmask in R0 (bits 0..7).
	VMOV	V7.D[0], R3
	VMOV	V7.D[1], R4
	LSR	$63, R3, R3
	LSR	$63, R4, R4
	LSL	$1, R4, R4
	ORR	R4, R3, R0

	VMOV	V10.D[0], R3
	VMOV	V10.D[1], R4
	LSR	$63, R3, R3
	LSR	$63, R4, R4
	LSL	$1, R4, R4
	ORR	R4, R3, R3
	LSL	$2, R3, R3
	ORR	R3, R0, R0

	VMOV	V13.D[0], R3
	VMOV	V13.D[1], R4
	LSR	$63, R3, R3
	LSR	$63, R4, R4
	LSL	$1, R4, R4
	ORR	R4, R3, R3
	LSL	$4, R3, R3
	ORR	R3, R0, R0

	VMOV	V16.D[0], R3
	VMOV	V16.D[1], R4
	LSR	$63, R3, R3
	LSR	$63, R4, R4
	LSL	$1, R4, R4
	ORR	R4, R3, R3
	LSL	$6, R3, R3
	ORR	R3, R0, R0

	MOVD	R0, ret+16(FP)
	RET

