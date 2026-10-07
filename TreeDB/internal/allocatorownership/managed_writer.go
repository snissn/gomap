// Package allocatorownership carries the trusted DB-only writer edge across
// the existing allocator lifecycle. It contains no owner registry or callbacks.
package allocatorownership

import "sync/atomic"

// ManagedWriter is internal authority, not a public caller-supplied ownership
// assertion. DB constructs it before exposing an index and binds it once.
type ManagedWriter struct{ claimed atomic.Bool }

func NewManagedWriter() *ManagedWriter { return &ManagedWriter{} }
func (owner *ManagedWriter) Claim() bool {
	return owner != nil && owner.claimed.CompareAndSwap(false, true)
}
