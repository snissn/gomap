package treedb

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/dictdb"
	"github.com/snissn/gomap/TreeDB/internal/limits"
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

	if opts.ValueLog.DictLookup == nil {
		dictDir := filepath.Join(rootDir, "dictdb")
		indexPath := filepath.Join(dictDir, "index.db")
		indexInfo, err := os.Stat(indexPath)
		if err != nil {
			if !os.IsNotExist(err) {
				return nil, fmt.Errorf("treedb: stat dictdb index: %w", err)
			}
		} else if indexInfo.IsDir() {
			return nil, fmt.Errorf("treedb: dictdb index path is a directory: %s", indexPath)
		} else {
			dictChunk := opts.DictDBChunkSize
			if dictChunk <= 0 {
				dictChunk = defaultDictChunkSize
			}
			dictOpts := *opts
			dictOpts.Dir = dictDir
			dictOpts.ReadOnly = opts.ReadOnly
			dictOpts.ChunkSize = dictChunk
			dictOpts.DisableBackgroundPrune = true
			dictOpts.IgnoreFormatConfig = false
			dictOpts.ResolvedProfile = ""
			dictOpts.DeprecatedProfileAlias = ""
			dictOpts.UnsafeBenchmarkProfile = false
			dictOpts.CommandWAL = false
			// Side stores must never require dict/template lookups themselves.
			dictOpts.IndexOuterLeavesInValueLog = false
			dictOpts.ValueLog.DictLookup = nil
			dictOpts.ValueLog.Compression = db.ValueLogCompressionOff
			dictOpts.ValueLog.TemplateMode = template.TemplateOff
			dictOpts.ValueLog.TemplateLookup = nil
			dictOpts.ValueLog.TemplateDecodeOptions = template.DecodeOptions{}

			dictBackend, err := db.Open(dictOpts)
			if err != nil {
				return nil, fmt.Errorf("treedb: open dictdb: %w", err)
			}
			closers = append(closers, dictBackend.Close)
			store := dictdb.New(dictBackend)
			opts.ValueLog.DictLookup = func(dictID uint64) ([]byte, error) {
				return store.GetDictBytes(context.Background(), dictID)
			}
			if opts.ValueLog.DictCurrentForClass == nil {
				opts.ValueLog.DictCurrentForClass = func(ctx context.Context, class string) (uint64, error) {
					return store.GetCurrentForClass(ctx, class)
				}
			}
			if opts.ValueLog.DictLeafPayloadMode == nil {
				opts.ValueLog.DictLeafPayloadMode = func(ctx context.Context, dictID uint64) (bool, bool, error) {
					return store.GetLeafPayloadMode(ctx, dictID)
				}
			}
			if !dictOpts.ReadOnly && opts.ValueLog.DictPut == nil {
				opts.ValueLog.DictPut = func(ctx context.Context, dictBytes []byte) (uint64, error) {
					return store.PutDictBytes(ctx, dictBytes)
				}
			}
			if !dictOpts.ReadOnly && opts.ValueLog.DictSetCurrentForClass == nil {
				opts.ValueLog.DictSetCurrentForClass = func(ctx context.Context, class string, dictID uint64) error {
					return store.SetCurrentForClass(ctx, class, dictID)
				}
			}
			if !dictOpts.ReadOnly && opts.ValueLog.DictSetLeafPayloadMode == nil {
				opts.ValueLog.DictSetLeafPayloadMode = func(ctx context.Context, dictID uint64, useRawPages bool) error {
					return store.SetLeafPayloadMode(ctx, dictID, useRawPages)
				}
			}
		}
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
