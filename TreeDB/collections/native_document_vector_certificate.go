package collections

import (
	"sync/atomic"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// The containing nativeDomainState owns the exact immutable base/pending/
// captured graph edge. This fixed tuple adds no retained storage reference.
// All tuple fields are initialized before publication; only verified may
// transition monotonically during a complete cold validation of those bytes.
type nativeDocumentGraphCertificate struct {
	incarnation uint64
	generation  uint64
	schema      [32]byte
	version     int
	verified    atomic.Bool
}

func (c *nativeDocumentGraphCertificate) matches(incarnation, generation uint64, schema [32]byte) bool {
	return c != nil && c.version == nativeDocumentVectorCoverageVersion && c.incarnation == incarnation && c.generation == generation && c.schema == schema
}

// bind runs only while the descriptor is still private. An imported graph is
// bound but unverified; selecting its interpretation is not graph validation.
func (c *nativeDocumentGraphCertificate) bind(incarnation, generation uint64, schema [32]byte) error {
	if c == nil || incarnation == 0 || c.version != 0 {
		return backenddb.ErrNativeSemanticUnsupported
	}
	c.version = nativeDocumentVectorCoverageVersion
	c.incarnation, c.generation, c.schema = incarnation, generation, schema
	return nil
}

func (c *nativeDocumentGraphCertificate) certify(incarnation, generation uint64, schema [32]byte) error {
	if !c.matches(incarnation, generation, schema) {
		return backenddb.ErrNativeSemanticUnsupported
	}
	c.verified.Store(true)
	return nil
}

func (c *nativeDocumentGraphCertificate) valid(incarnation, generation uint64, schema [32]byte) bool {
	return c.matches(incarnation, generation, schema) && c.verified.Load()
}

// preserve is only for an explicitly proven representation-only handoff of
// the SAME complete logical graph. Generic domain updates must not call it.
func (c *nativeDocumentGraphCertificate) preserve(old *nativeDocumentGraphCertificate) error {
	if old == nil || old.version == 0 {
		return nil
	}
	if err := c.bind(old.incarnation, old.generation, old.schema); err != nil {
		return err
	}
	if old.verified.Load() {
		c.verified.Store(true)
	}
	return nil
}

// The original domain lifetime calls this only after the last owned reference
// ends. Never copy a live atomic certificate or leave an external pointer to it.
func (c *nativeDocumentGraphCertificate) clear() {
	c.verified.Store(false)
	c.version, c.incarnation, c.generation, c.schema = 0, 0, 0, [32]byte{}
}
