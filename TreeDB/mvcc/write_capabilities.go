package mvcc

import (
	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/mvccadmission"
)

type pointMVCCInputWriter interface {
	SetPointWithMVCCInput([]byte, []byte, bool, mvccadmission.Input) error
}

// Concrete public producers already implement these optional interfaces.
// Recognizing them directly avoids runtime interface-assertion cache updates.
// Recheck the current producer on every call so internal decorators and Store
// literals keep their own methods; support decisions remain with the caller.
func logicalMVCCWriterFor(db treeDB) logicalMVCCWriter {
	if concrete, ok := db.(*treedb.DB); ok {
		return concrete
	}
	writer, _ := db.(logicalMVCCWriter)
	return writer
}

func pointMVCCInputWriterFor(db treeDB) pointMVCCInputWriter {
	if concrete, ok := db.(*treedb.DB); ok {
		return concrete
	}
	writer, _ := db.(pointMVCCInputWriter)
	return writer
}
