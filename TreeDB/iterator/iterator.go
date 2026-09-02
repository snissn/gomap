// Package iterator defines TreeDB's public, storage-neutral iterator contract.
package iterator

// Iterator scans one immutable point-in-time view over a fixed half-open key
// domain. Key and Value return read-only views that remain valid only until the
// next Next, Seek, or Close. Copy methods return caller-owned bytes.
type Iterator interface {
	Valid() bool
	Next()
	Seek(key []byte)
	Key() []byte
	Value() []byte
	KeyCopy(dst []byte) []byte
	ValueCopy(dst []byte) []byte
	Close() error
	Error() error
}
