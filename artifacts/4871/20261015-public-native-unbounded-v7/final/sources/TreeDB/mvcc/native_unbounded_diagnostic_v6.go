//go:build treedb_test

package mvcc

// PruneVersionsUnboundedDiagnosticForTest executes the SAME native public path,
// explicitly including synchronous original producer work. WorkRecords/Bytes
// report continuation work ONLY and are incomplete for this diagnostic.
// This grants no bounded-work, Q32 or latency-ceiling claim.
func (s *Store) PruneVersionsUnboundedDiagnosticForTest(options PruneOptions) (PruneStats, error) {
	options.diagnosticUnbounded = true
	return s.PruneVersions(options)
}
