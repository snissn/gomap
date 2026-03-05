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
		if info, err := os.Stat(dictDir); err == nil && info.IsDir() {
			dictChunk := opts.DictDBChunkSize
			if dictChunk <= 0 {
				dictChunk = defaultDictChunkSize
			}
			dictOpts := db.Options{
				Dir:                    dictDir,
				ReadOnly:               true,
				ChunkSize:              dictChunk,
				DisableBackgroundPrune: true,
				IgnoreFormatConfig:     true,
			}
			// Side stores must never require dict/template lookups themselves.
			dictOpts.IndexOuterLeavesInValueLog = false
			dictOpts.ValueLog.Compression = db.ValueLogCompressionOff
			dictOpts.ValueLog.TemplateMode = template.TemplateOff

			dictBackend, err := db.Open(dictOpts)
			if err != nil {
				return nil, fmt.Errorf("treedb: open dictdb: %w", err)
			}
			closers = append(closers, dictBackend.Close)
			store := dictdb.New(dictBackend)
			opts.ValueLog.DictLookup = func(dictID uint64) ([]byte, error) {
				return store.GetDictBytes(context.Background(), dictID)
			}
		}
	}

	if opts.ValueLog.TemplateLookup == nil {
		templateDir := filepath.Join(rootDir, "templatedb")
		if info, err := os.Stat(templateDir); err == nil && info.IsDir() {
			templateChunk := opts.TemplateDBChunkSize
			if templateChunk <= 0 {
				templateChunk = defaultDictChunkSize
			}
			templateOpts := db.Options{
				Dir:                    templateDir,
				ReadOnly:               true,
				ChunkSize:              templateChunk,
				DisableBackgroundPrune: true,
				IgnoreFormatConfig:     true,
			}
			templateOpts.IndexOuterLeavesInValueLog = false
			templateOpts.ValueLog.Compression = db.ValueLogCompressionOff
			templateOpts.ValueLog.TemplateMode = template.TemplateOff

			templateBackend, err := db.Open(templateOpts)
			if err != nil {
				_ = cleanup()
				return nil, fmt.Errorf("treedb: open templatedb: %w", err)
			}
			closers = append(closers, templateBackend.Close)
			store := templatedb.New(templateBackendKV{db: templateBackend}, templatedb.Config{})

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
