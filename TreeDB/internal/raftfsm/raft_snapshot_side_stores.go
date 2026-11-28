package raftfsm

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/compression"
	"github.com/snissn/gomap/TreeDB/internal/limits"
	"github.com/snissn/gomap/TreeDB/internal/templatedb"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/template"
)

const raftSnapshotSideStoreChunkSizeV1 = 64 * 1024

type raftSnapshotTemplateBackendKVV1 struct {
	db *backenddb.DB
}

func (kv raftSnapshotTemplateBackendKVV1) Get(key []byte) ([]byte, error) {
	if kv.db == nil {
		return nil, nil
	}
	return kv.db.Get(key)
}

func (kv raftSnapshotTemplateBackendKVV1) SetSync(key, value []byte) error {
	if kv.db == nil {
		return nil
	}
	return kv.db.SetSync(key, value)
}

func (kv raftSnapshotTemplateBackendKVV1) DeleteSync(key []byte) error {
	if kv.db == nil {
		return nil
	}
	return kv.db.DeleteSync(key)
}

func (kv raftSnapshotTemplateBackendKVV1) NewBatch() templatedb.Batch {
	if kv.db == nil {
		return nil
	}
	b := kv.db.NewBatch()
	if b == nil {
		return nil
	}
	return b
}

func wireRaftSnapshotSideStoreLookupsV1(rootDir string, opts *backenddb.Options) (func() error, error) {
	if opts == nil || opts.DisableSideStores {
		return func() error { return nil }, nil
	}
	if rootDir == "" {
		return nil, fmt.Errorf("raftfsm: missing root dir for restored side-store lookup wiring")
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

	if err := wireRaftSnapshotTemplateLookupV1(rootDir, opts, &closers); err != nil {
		_ = cleanup()
		return nil, err
	}
	return cleanup, nil
}

func wireRaftSnapshotTemplateLookupV1(rootDir string, opts *backenddb.Options, closers *[]func() error) error {
	templateDir := filepath.Join(rootDir, "templatedb")
	indexPath := filepath.Join(templateDir, "index.db")
	indexInfo, err := os.Stat(indexPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("raftfsm: stat restored templatedb index: %w", err)
	}
	if indexInfo.IsDir() {
		return fmt.Errorf("raftfsm: restored templatedb index path is a directory: %s", indexPath)
	}

	templateOpts := *opts
	templateOpts.Dir = templateDir
	templateReadOnly := opts.ReadOnly || opts.ValueLog.TemplateMode == template.TemplateOff
	templateOpts.ReadOnly = templateReadOnly
	if templateOpts.ChunkSize <= 0 {
		templateOpts.ChunkSize = raftSnapshotSideStoreChunkSizeV1
	}
	if opts.TemplateDBChunkSize > 0 {
		templateOpts.ChunkSize = opts.TemplateDBChunkSize
	}
	templateOpts.DisableBackgroundPrune = true
	templateOpts.IgnoreFormatConfig = false
	templateOpts.CommandWAL = false
	scrubRaftSnapshotSideStoreOptionsV1(&templateOpts)

	templateBackend, err := backenddb.Open(templateOpts)
	if err != nil {
		return fmt.Errorf("raftfsm: open restored templatedb: %w", err)
	}
	*closers = append(*closers, templateBackend.Close)
	previousCapture := opts.PhysicalSnapshotSideStoreCapture
	opts.PhysicalSnapshotSideStoreCapture = func(ctx context.Context, name string) (*backenddb.PhysicalSnapshotCutV1, error) {
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
		return nil, fmt.Errorf("raftfsm: snapshot side store %q has no owner", name)
	}
	store := templatedb.New(raftSnapshotTemplateBackendKVV1{db: templateBackend}, templatedb.Config{})
	if !templateReadOnly && opts.ValueLog.TemplateMode != template.TemplateOff {
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
	return nil
}

func scrubRaftSnapshotSideStoreOptionsV1(opts *backenddb.Options) {
	opts.PhysicalSnapshotSideStoreCapture = nil
	opts.IndexOuterLeavesInValueLog = false
	opts.ValueLog.DictLookup = nil
	opts.ValueLog.DictTrain = compression.TrainConfig{TrainBytes: -1}
	opts.ValueLog.ForcePointers = false
	opts.ValueLog.PointerThreshold = 0
	opts.ValueLog.DomainInlineThresholds = nil
	opts.ValueLog.Compression = backenddb.ValueLogCompressionOff
	opts.ValueLog.CompressionAutotune = valuelog.AutotuneOptions{Mode: valuelog.AutotuneOff}
	opts.ValueLog.TemplateMode = template.TemplateOff
	opts.ValueLog.TemplateLookup = nil
	opts.ValueLog.TemplateDecodeOptions = template.DecodeOptions{}
}
