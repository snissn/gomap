package treedb

import "github.com/snissn/gomap/TreeDB/internal/compression"

// EnableValueLogDictCompression enables background dictionary training for
// value-log frame compression (cached mode).
//
// Dictionaries are persisted inside maindb's SystemRootPageID.
//
// Advanced tuning remains available via opts.ValueLog.DictTrain and
// opts.ValueLog.CompressionAutotune.
func EnableValueLogDictCompression(opts *Options) {
	if opts == nil {
		return
	}
	train := opts.ValueLog.DictTrain
	if train.TrainBytes <= 0 {
		train.TrainBytes = compression.DefaultTrainBytes
	}
	opts.ValueLog.DictTrain = train
}

// DisableValueLogDictCompression disables background dictionary training for
// value-log frame compression (cached mode).
//
// It does not remove existing dictionaries from SystemRootPageID; it only
// prevents training/publishing new dictionaries for future writes.
func DisableValueLogDictCompression(opts *Options) {
	if opts == nil {
		return
	}
	train := opts.ValueLog.DictTrain
	train.TrainBytes = -1
	opts.ValueLog.DictTrain = train
}
