//go:build amd64 && !purego

#include "textflag.h"

// Same ordered scalar FMA tail as simd/f32's AVX and AVX-512 dot kernels.
// The Go wrapper checks AVX/FMA support and 0 < n < the packed width.
TEXT ·dotFloat32ShortFMA(SB), NOSPLIT, $0-28
    MOVQ left+0(FP), SI
    MOVQ right+8(FP), DI
    MOVQ n+16(FP), CX
    VXORPS X0, X0, X0
short_dot_loop:
    VMOVSS (SI), X1
    VMOVSS (DI), X2
    VFMADD231SS X1, X2, X0
    ADDQ $4, SI
    ADDQ $4, DI
    DECQ CX
    JNZ short_dot_loop
    VMOVSS X0, ret+24(FP)
    VZEROUPPER
    RET
