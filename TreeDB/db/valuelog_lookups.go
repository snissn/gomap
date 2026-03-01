package db

import (
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	templ "github.com/snissn/gomap/TreeDB/template"
)

// SetValueLogDictLookup installs a dict lookup for decoding dict-compressed
// value-log frames. This is safe to call after Open; it updates the live
// value-log manager in place.
func (db *DB) SetValueLogDictLookup(lookup valuelog.DictLookup) {
	if db == nil {
		return
	}
	vm := db.valueLogManager
	if vm == nil {
		return
	}
	vm.SetDictLookup(lookup)
}

// SetValueLogTemplateLookup installs a template lookup for decoding template
// encoded payloads in the value log. Safe to call after Open.
func (db *DB) SetValueLogTemplateLookup(lookup valuelog.TemplateLookup, opts templ.DecodeOptions) {
	if db == nil {
		return
	}
	vm := db.valueLogManager
	if vm == nil {
		return
	}
	vm.SetTemplateLookup(lookup, opts)
}

