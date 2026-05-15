package db

// LeafPageLogCloser is a standalone leaf-page log that should be closed by the
// owner after the DB is closed.
type LeafPageLogCloser interface {
	LeafPageLog
	Close() error
}

// StandaloneLeafPageLogOptions configures a leaf-page log for direct backend
// users that enable IndexOuterLeavesInValueLog without the cached layer.
type StandaloneLeafPageLogOptions struct {
	MaxSegmentBytes int64
	Compression     ValueLogCompressionMode
	AutoPolicy      ValueLogAutoPolicy
	BlockCodec      ValueLogBlockCodec
}

// NewStandaloneLeafPageLog creates a persistent leaf-page log for direct DB
// users. Cached TreeDB opens install their own leaf log; this helper is for
// direct backend adapters and tests that intentionally bypass cached mode.
func NewStandaloneLeafPageLog(dir string, opts StandaloneLeafPageLogOptions) (LeafPageLogCloser, error) {
	if err := ensureStorageLayoutDirs(dir); err != nil {
		return nil, err
	}
	segments, err := listValueLogSegments(dir)
	if err != nil {
		return nil, err
	}
	maxSegmentBytes := opts.MaxSegmentBytes
	if maxSegmentBytes == 0 {
		maxSegmentBytes = int64(^uint32(0)) - 4
	}
	compression := opts.Compression
	if compression == 0 {
		compression = ValueLogCompressionAuto
	}
	writer := newRewriteWriter(ValueLogDirPath(dir), 0, 0, maxSegmentBytes)
	writer.ConfigureLeafLog(LeafLogDirPath(dir), rewriteLeafLogLaneID, maxRewriteLaneSeq(segments, rewriteLeafLogLaneID))
	writer.blockCompression = compression != ValueLogCompressionOff
	writer.blockCodec = valuelogBlockCodecFromDB(opts.BlockCodec)
	writer.leafBlockCodec = leafPageBlockCodecFromOptions(compression, opts.AutoPolicy, opts.BlockCodec, true)
	return writer, nil
}
