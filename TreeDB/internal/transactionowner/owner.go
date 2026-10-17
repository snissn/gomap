// Package transactionowner carries the private public-DB ownership fence.
// It is not persisted and grants no authority over a caller-owned backend.
package transactionowner

type Owner struct{ identity byte }

func New() *Owner { return &Owner{identity: 1} }
