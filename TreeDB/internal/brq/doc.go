// Package brq implements the TreeDB brq_1bit v1 oracle.
//
// The package is intentionally internal and reference-oriented: it defines the
// codec identity, canonical config bytes, deterministic rotation, golden data
// encoding, query uint4 bit-plane encoding, and score formula used to validate
// future optimized asset/search implementations. It does not publish collection
// runtime behavior or mutate rabitq_1bit semantics.
package brq
