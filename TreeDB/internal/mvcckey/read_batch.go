package mvcckey

// MaxReadBatch bounds one synchronous owned-successor operation.
const MaxReadBatch = 32

// VersionRangeResult owns its key and value independently of the read cut.
// Err belongs only to this input; errors must never be retried as misses.
type VersionRangeResult struct {
	Key, Value []byte
	Found      bool
	Err        error
}
