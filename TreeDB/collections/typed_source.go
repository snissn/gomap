package collections

import (
	"sync/atomic"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// Test-only observation before shared vector mutation admission.
var typedSourceBeforeAdmissionTestHook atomic.Pointer[func(*Collection)]

// ReplaceTypedSourceByID atomically deletes the explicit old ID set and inserts
// a complete typed replacement. Insert wins for IDs in both sets; the return
// value counts previously present deleted IDs. It uses the typed-batch schema,
// retained-JSON and ownership contract, and one command-WAL frame. An ambiguous
// error must not be blindly retried. Empty insertion permits nil columns and
// uses the existing delete-only source command payload.
func (c *Collection) ReplaceTypedSourceByID(deleteIDs, insertIDs, retained [][]byte, columns []TypedColumnBatch) (int, error) {
	return c.replaceTypedSourceByID(deleteIDs, insertIDs, retained, columns, false)
}

// UpsertTypedBatch atomically inserts missing IDs and replaces existing IDs.
// Its return value counts existing IDs, including unchanged values. Unchanged
// retained bytes and bitwise-equal typed values do not create a new version.
// Mixed changes use one source-replacement command-WAL frame; duplicate IDs and
// unique-index conflicts fail before publication. Inputs may be reused on return.
func (c *Collection) UpsertTypedBatch(ids, retained [][]byte, columns []TypedColumnBatch) (int, error) {
	return c.replaceTypedSourceByID(ids, ids, retained, columns, true)
}

func (c *Collection) replaceTypedSourceByID(deleteIDs, insertIDs, retained [][]byte, columns []TypedColumnBatch, upsert bool) (int, error) {
	if c == nil || c.db == nil {
		return 0, errCollectionNil
	}
	if err := c.ensureWriteDomainOpen(); err != nil {
		return 0, err
	}
	unlockSchema := c.lockCollectionSchemaRead()
	defer unlockSchema()
	if hook := typedSourceBeforeAdmissionTestHook.Load(); hook != nil {
		(*hook)(c)
	}
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
	deleted, err := c.replaceSourceDocumentsAtomicModeSchemaLocked(nil, deleteIDs, insertIDs, retained, nil, nil, projection, upsert)
	return deleted, c.invalidateVectorIndexCoverageOnAcceptedMutation(err)
}
