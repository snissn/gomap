package treedb

import "github.com/snissn/gomap/TreeDB/internal/compression"

// EnableValueLogDictCompression enables background dictionary training for
// value-log frame compression (cached mode).
//
// This is a convenience helper intended to avoid requiring callers to set many
// low-level knobs. It sets TrainBytes to a safe default if unset and ensures
// side stores are enabled so dictionaries can be persisted in dictdb/.
//
// Advanced tuning remains available via opts.ValueLog.DictTrain and
// opts.ValueLog.CompressionAutotune.
func EnableValueLogDictCompression(opts *Options) {
	if opts == nil {
		return
	}
	// Dict compression relies on a persistent dict store (dictdb) by default.
	// If a caller explicitly disabled side stores, enabling dict compression
	// should also re-enable them to avoid a confusing no-op configuration.
	opts.DisableSideStores = false

	train := opts.ValueLog.DictTrain
	if train.TrainBytes <= 0 {
		train.TrainBytes = compression.DefaultTrainBytes
	}
	opts.ValueLog.DictTrain = train
}

// DisableValueLogDictCompression disables background dictionary training for
// value-log frame compression (cached mode).
//
// It does not remove dictdb state on disk; it only prevents training/publishing
// new dictionaries for future writes.
func DisableValueLogDictCompression(opts *Options) {
	if opts == nil {
		return
	}
	train := opts.ValueLog.DictTrain
	train.TrainBytes = -1
	opts.ValueLog.DictTrain = train
}
