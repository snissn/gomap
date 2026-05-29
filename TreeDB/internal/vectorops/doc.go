// Package vectorops contains allocation-free vector kernels shared by TreeDB
// packages. Kernels operate on already validated typed slices; callers that
// start from mapped bytes must validate and obtain typed views before calling
// into this package.
//
// Row-major batch dot wrappers accept flat []float32 payloads and ordinal/row-id
// tiles directly. They validate full-row shapes, leave dst unchanged for invalid
// shapes, and report whether a platform batch SIMD kernel or a non-batch
// fallback handled the call.
package vectorops
