// Package rabitq defines TreeDB's clean-room rabitq_1bit v1 reference codec.
//
// The package is a correctness oracle for durable asset builders and future
// accelerated scorers. It deliberately does not publish collection assets or
// integrate search. The v1 storage contract is quantizedasset RolePackedCodes
// backed by typed-column packed_bit_vector rows: one LSB-first bit per code
// dimension, zero high padding bits in the final byte, and CodeWidthBits == 1.
//
// For cosine column_graph indexes, callers encode finite non-zero float32
// vectors after unit-L2 normalization. The reference rotation is a deterministic
// seeded signed permutation followed by a normalized Walsh-Hadamard transform
// over next-power-of-two padded dimensions. Data codes store the sign of each
// rotated component. Query encoding stores the rotated query sign bits and
// absolute per-dimension weights used by the pure-Go weighted-popcount scorer.
package rabitq
