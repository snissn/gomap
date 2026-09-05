package collections

import backenddb "github.com/snissn/gomap/TreeDB/db"

// ReplaceTypedSourceByID atomically deletes the explicit old ID set and inserts
// a complete typed replacement. Insert wins for IDs in both sets; the return
// value counts previously present deleted IDs. It uses the typed-batch schema,
// retained-JSON and ownership contract, and one command-WAL frame. An ambiguous
// error must not be blindly retried. Empty insertion permits nil columns and
// uses the existing delete-only source command payload.
func (c *Collection) ReplaceTypedSourceByID(deleteIDs, insertIDs, retained [][]byte, columns []TypedColumnBatch) (int, error) {
	if c == nil || c.db == nil {
		return 0, errCollectionNil
	}
	if err := c.ensureWriteDomainOpen(); err != nil {
		return 0, err
	}
	unlockSchema := c.lockCollectionSchemaRead()
	defer unlockSchema()
	unlockCoverage := c.lockVectorIndexCoverageMutation()
	defer unlockCoverage()
	if err := c.requireTypedBatchVectorAdmission(); err != nil {
		return 0, err
	}
	if !c.commandWALActive(nil) {
		return 0, backenddb.ErrCommandWALRejected
	}
	if len(insertIDs) == 0 && len(columns) == 0 {
		cfg := c.Meta().Options.ColumnStore
		if cfg != nil {
			columns = make([]TypedColumnBatch, len(cfg.Columns))
			for i, col := range cfg.Columns {
				columns[i].Name = col.Name
			}
		}
	}
	projection, err := newTrustedTypedProjection(c.Meta(), insertIDs, retained, columns)
	if err != nil {
		return 0, err
	}
	if err := c.requireColumnStoreCommandWAL(c.Meta(), nil); err != nil {
		return 0, err
	}
	deleted, err := c.replaceSourceDocumentsAtomicSchemaLocked(nil, deleteIDs, insertIDs, retained, nil, nil, projection)
	return deleted, c.invalidateVectorIndexCoverageOnAcceptedMutation(err)
}
