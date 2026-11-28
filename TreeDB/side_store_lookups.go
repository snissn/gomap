package treedb

import (
	"context"

	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/dictdb"
)

func wireSideStoreLookups(_ string, _ *Options) (func() error, error) {
	// Dictionaries and templates live directly inside maindb's SystemRootPageID;
	// there are no external side-store databases to open.
	return func() error { return nil }, nil
}

func wireMainBackendDictCallbacks(backend *db.DB, store *dictdb.Store, readOnly bool) {
	if backend == nil || store == nil {
		return
	}
	var (
		put                func(context.Context, []byte) (uint64, error)
		setCurrentForClass func(context.Context, string, uint64) error
		setLeafPayloadMode func(context.Context, uint64, bool) error
		publishRewriteLeaf func(context.Context, []byte, string, bool) (uint64, error)
	)
	if !readOnly {
		put = store.PutDictBytes
		setCurrentForClass = store.SetCurrentForClass
		setLeafPayloadMode = store.SetLeafPayloadMode
		publishRewriteLeaf = store.PublishRewriteLeafDict
	}
	backend.SetDictCallbacks(
		nil,
		store.GetCurrentForClass,
		store.GetLeafPayloadMode,
		put,
		setCurrentForClass,
		setLeafPayloadMode,
		publishRewriteLeaf,
	)
}
