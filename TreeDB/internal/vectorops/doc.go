// Package vectorops contains allocation-free vector kernels shared by TreeDB
// packages. Kernels operate on already validated typed slices; callers that
// start from mapped bytes must validate and obtain typed views before calling
// into this package.
package vectorops
