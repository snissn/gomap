package db

import "github.com/snissn/gomap/TreeDB/page"

// Historical optional files are recognized during leaf-file retirement.
const leafGenerationRecordLengthIndexSuffix = ".lenidx"

func leafGenerationRecordLengthIndexPath(rootDir string, rawFileID uint32) string {
	path := leafGenerationFallbackPath(rootDir, rawFileID)
	if path == "" {
		return ""
	}
	return path + leafGenerationRecordLengthIndexSuffix
}

// NoteLeafGenerationRecordLength remains callable by older leaf-log producers.
// The built-in backend resolves an omitted length from the retained leaf file.
//
// Deprecated: the built-in backend no longer records optional length indexes.
func (db *DB) NoteLeafGenerationRecordLength(page.ValuePtr) {}
