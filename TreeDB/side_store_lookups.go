package treedb

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/dictdb"
	"github.com/snissn/gomap/TreeDB/internal/limits"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/templatedb"
	"github.com/snissn/gomap/TreeDB/template"
)

type templateBackendKV struct {
	db *db.DB
}

func (kv templateBackendKV) Get(key []byte) ([]byte, error) {
	if kv.db == nil {
		return nil, nil
	}
	return kv.db.Get(key)
}

func (kv templateBackendKV) SetSync(key, value []byte) error {
	if kv.db == nil {
		return nil
	}
	return kv.db.SetSync(key, value)
}

func (kv templateBackendKV) DeleteSync(key []byte) error {
	if kv.db == nil {
		return nil
	}
	return kv.db.DeleteSync(key)
}

func (kv templateBackendKV) NewBatch() templatedb.Batch {
	if kv.db == nil {
		return nil
	}
	b := kv.db.NewBatch()
	if b == nil {
		return nil
	}
	// batch.Interface implements templatedb.Batch (Set/Delete/WriteSync/Close).
	return b
}

func (kv templateBackendKV) AcquireStableTemplateSnapshot() (templatedb.StablePhysicalSnapshot, error) {
	return acquireStableTemplateSnapshot(kv.db), nil
}

func wireSideStoreLookups(rootDir string, opts *Options) (func() error, error) {
	if opts == nil || opts.DisableSideStores {
		return func() error { return nil }, nil
	}
	if rootDir == "" {
		return nil, fmt.Errorf("treedb: missing root dir for side-store lookup wiring")
	}

	var closers []func() error
	cleanup := func() error {
		var first error
		for i := len(closers) - 1; i >= 0; i-- {
			if err := closers[i](); err != nil && first == nil {
				first = err
			}
		}
		return first
	}

	if opts.ValueLog.TemplateLookup == nil {
		templateDir := filepath.Join(rootDir, "templatedb")
		indexPath := filepath.Join(templateDir, "index.db")
		indexInfo, err := os.Stat(indexPath)
		if err != nil {
			if !os.IsNotExist(err) {
				_ = cleanup()
				return nil, fmt.Errorf("treedb: stat templatedb index: %w", err)
			}
		} else if indexInfo.IsDir() {
			_ = cleanup()
			return nil, fmt.Errorf("treedb: templatedb index path is a directory: %s", indexPath)
		} else {
			templateChunk := opts.TemplateDBChunkSize
			if templateChunk <= 0 {
				templateChunk = defaultTemplateChunkSize
			}
			templateOpts := *opts
			templateOpts.PhysicalSnapshotSideStoreCapture = nil
			templateOpts.NegativeLookupFilterBytes = 0
			templateOpts.Dir = templateDir
			templateReadOnly := opts.ReadOnly || opts.ValueLog.TemplateMode == template.TemplateOff
			templateOpts.ReadOnly = templateReadOnly
			templateOpts.ChunkSize = templateChunk
			templateOpts.DisableBackgroundPrune = true
			templateOpts.IgnoreFormatConfig = false
			templateOpts.ResolvedProfile = ""
			templateOpts.DeprecatedProfileAlias = ""
			templateOpts.UnsafeBenchmarkProfile = false
			templateOpts.CommandWAL = false
			templateOpts.IndexOuterLeavesInValueLog = false
			templateOpts.ValueLog.DictLookup = nil
			templateOpts.ValueLog.Compression = db.ValueLogCompressionOff
			templateOpts.ValueLog.TemplateMode = template.TemplateOff
			templateOpts.ValueLog.TemplateLookup = nil
			templateOpts.ValueLog.TemplateDecodeOptions = template.DecodeOptions{}

			templateBackend, err := db.Open(templateOpts)
			if err != nil {
				_ = cleanup()
				return nil, fmt.Errorf("treedb: open templatedb: %w", err)
			}
			closers = append(closers, templateBackend.Close)
			previousCapture := opts.PhysicalSnapshotSideStoreCapture
			opts.PhysicalSnapshotSideStoreCapture = func(ctx context.Context, name string) (*db.PhysicalSnapshotCutV1, error) {
				if name == "templatedb" {
					if !templateReadOnly {
						if err := templateBackend.Checkpoint(); err != nil {
							return nil, err
						}
					}
					return templateBackend.CapturePhysicalSnapshotCutV1(ctx)
				}
				if previousCapture != nil {
					return previousCapture(ctx, name)
				}
				return nil, fmt.Errorf("treedb: snapshot side store %q has no owner", name)
			}
			store := templatedb.New(templateBackendKV{db: templateBackend}, templatedb.Config{})
			if !templateReadOnly && opts.ValueLog.TemplateMode != template.TemplateOff && opts.ValueLog.TemplateStore == nil {
				opts.ValueLog.TemplateStore = store
			}

			if opts.ValueLog.TemplateDecodeOptions == (template.DecodeOptions{}) {
				tcfg := template.NormalizeConfig(opts.ValueLog.TemplateConfig)
				decodeOpts := template.DecodeOptions{MaxGaps: tcfg.MaxGaps, MaxDecodedBytes: tcfg.MaxDecodedBytes, DefCacheSize: tcfg.DefCacheSize}
				if decodeOpts.MaxDecodedBytes <= 0 && limits.MaxRecordSize > 0 {
					decodeOpts.MaxDecodedBytes = int(limits.MaxRecordSize)
				}
				opts.ValueLog.TemplateDecodeOptions = decodeOpts
			}
			opts.ValueLog.TemplateLookup = func(templateID uint64) ([]byte, error) {
				return store.GetTemplateDef(context.Background(), templateID)
			}
		}
	}

	return cleanup, nil
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

func (kv templateBackendKV) AcquireStableTemplateSnapshotWithMetadata(metadata *retainedalloc.Owner) (templatedb.StablePhysicalSnapshot, error) {
	charge, err := admitTemplateSnapshotMetadata(metadata)
	if err != nil {
		return nil, err
	}
	captured, err := kv.AcquireStableTemplateSnapshot()
	if err != nil || captured == nil {
		metadata.RemovePending(charge)
		return captured, err
	}
	snapshot, ok := captured.(*templateStableSnapshot)
	if !ok {
		captured.Close()
		metadata.RemovePending(charge)
		return nil, rootpublication.ErrResourceOwnership
	}
	snapshot.metadata, snapshot.metadataCharge = metadata, charge
	return snapshot, nil
}
