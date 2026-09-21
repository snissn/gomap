package pebblecompat

import (
	"context"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage"
)

func (d *DB) ensureShadowLocked() error {
	if err := d.ensureOpenLocked(); err != nil {
		return err
	}
	if d.shadow == nil {
		return ErrClosed
	}
	return nil
}

// Compact forwards compaction to the shadow engine.
func (d *DB) Compact(start, end []byte, parallelize bool) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.ensureShadowLocked(); err != nil {
		return err
	}
	return d.shadow.Compact(start, end, parallelize)
}

// Metrics returns shadow Pebble metrics.
func (d *DB) Metrics() *pebble.Metrics {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.ensureShadowLocked(); err != nil {
		return &pebble.Metrics{}
	}
	return d.shadow.Metrics()
}

// EstimateDiskUsage returns shadow Pebble disk-usage estimates.
func (d *DB) EstimateDiskUsage(start, end []byte) (uint64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.ensureShadowLocked(); err != nil {
		return 0, err
	}
	return d.shadow.EstimateDiskUsage(start, end)
}

// EstimateDiskUsageByBackingType returns shadow Pebble backing-type usage estimates.
func (d *DB) EstimateDiskUsageByBackingType(start, end []byte) (totalSize, remoteSize, externalSize uint64, _ error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.ensureShadowLocked(); err != nil {
		return 0, 0, 0, err
	}
	return d.shadow.EstimateDiskUsageByBackingType(start, end)
}

// SSTables returns shadow Pebble sstable metadata.
func (d *DB) SSTables(opts ...pebble.SSTablesOption) ([][]pebble.SSTableInfo, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.ensureShadowLocked(); err != nil {
		return nil, err
	}
	return d.shadow.SSTables(opts...)
}

// ScanStatistics returns shadow Pebble scan statistics.
func (d *DB) ScanStatistics(
	ctx context.Context,
	lower, upper []byte,
	opts pebble.ScanStatisticsOptions,
) (pebble.LSMKeyStatistics, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.ensureShadowLocked(); err != nil {
		return pebble.LSMKeyStatistics{}, err
	}
	return d.shadow.ScanStatistics(ctx, lower, upper, opts)
}

// AsyncFlush forwards to shadow Pebble async flush.
func (d *DB) AsyncFlush() (<-chan struct{}, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.ensureShadowLocked(); err != nil {
		return nil, err
	}
	return d.shadow.AsyncFlush()
}

// Download forwards to shadow Pebble download API.
func (d *DB) Download(ctx context.Context, spans []pebble.DownloadSpan) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.ensureShadowLocked(); err != nil {
		return err
	}
	return d.shadow.Download(ctx, spans)
}

// ObjProvider returns the shadow Pebble object provider.
func (d *DB) ObjProvider() objstorage.Provider {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.ensureShadowLocked(); err != nil {
		return nil
	}
	return d.shadow.ObjProvider()
}

// FormatMajorVersion returns the shadow Pebble format major version.
func (d *DB) FormatMajorVersion() pebble.FormatMajorVersion {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.ensureShadowLocked(); err != nil {
		return 0
	}
	return d.shadow.FormatMajorVersion()
}

// RatchetFormatMajorVersion forwards to shadow Pebble format ratcheting.
func (d *DB) RatchetFormatMajorVersion(fmv pebble.FormatMajorVersion) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.ensureShadowLocked(); err != nil {
		return err
	}
	return d.shadow.RatchetFormatMajorVersion(fmv)
}

// SetCreatorID forwards to shadow Pebble creator-id configuration.
func (d *DB) SetCreatorID(creatorID uint64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.ensureShadowLocked(); err != nil {
		return err
	}
	return d.shadow.SetCreatorID(creatorID)
}

// NewEventuallyFileOnlySnapshot returns a shadow Pebble EFOS.
func (d *DB) NewEventuallyFileOnlySnapshot(keyRanges []pebble.KeyRange) *pebble.EventuallyFileOnlySnapshot {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.ensureShadowLocked(); err != nil {
		return nil
	}
	return d.shadow.NewEventuallyFileOnlySnapshot(keyRanges)
}
