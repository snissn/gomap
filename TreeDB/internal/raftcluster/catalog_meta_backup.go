package raftcluster

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"

	hraft "github.com/hashicorp/raft"
)

const (
	catalogMetaBackupFormatV1      uint16 = 1
	catalogMetaBackupHeaderBytesV1        = 64
)

var (
	catalogMetaBackupMagicV1 = [8]byte{'T', 'C', 'M', 'E', 'T', 'A', '1', '\n'}

	// ErrInvalidCatalogMetaBackup classifies a corrupt, unsupported, or
	// non-canonical catalog backup archive.
	ErrInvalidCatalogMetaBackup = errors.New("raftcluster: invalid catalog meta backup")
	// ErrCatalogMetaBackupRestoreTarget classifies attempts to use disaster
	// recovery restore on a live catalog authority.
	ErrCatalogMetaBackupRestoreTarget = errors.New("raftcluster: catalog meta backup restore target is not fresh")
)

type catalogMetaBackupArchiveV1 struct {
	snapshotVersion hraft.SnapshotVersion
	term            uint64
	index           uint64
	payload         []byte
}

// ExportCatalogMetaBackupV1 forces or reuses the latest retained HashiCorp
// snapshot on the catalog leader and packages its exact FSM bytes with bounded,
// checksummed Raft metadata. The archive is suitable only for
// RestoreCatalogMetaBackupV1 into a fresh disaster-recovery cluster.
func (p *CatalogMetaRaftProviderV1) ExportCatalogMetaBackupV1(ctx context.Context) ([]byte, error) {
	if p == nil || p.raft == nil || p.snapshots == nil || p.state == nil {
		return nil, ErrInvalidHashicorpRaftProvider
	}
	p.mutationMu.Lock()
	defer p.mutationMu.Unlock()
	if ctx == nil {
		ctx = context.Background()
	}
	if err := requireCatalogMetaBackupLeaderV1(ctx, p); err != nil {
		return nil, err
	}
	backupState, ok := p.state.(CatalogMetaBackupStateV1)
	if !ok {
		return nil, errors.Join(ErrInvalidHashicorpRaftProvider, fmt.Errorf("catalog state does not support backup validation"))
	}

	future := p.raft.Snapshot()
	snapshotErr := waitHashicorpRaftFuture(ctx, future)
	var (
		meta   *hraft.SnapshotMeta
		reader io.ReadCloser
		err    error
	)
	switch {
	case snapshotErr == nil:
		meta, reader, err = future.Open()
	case errors.Is(snapshotErr, hraft.ErrNothingNewToSnapshot):
		meta, reader, err = openLatestCatalogMetaSnapshotV1(p.snapshots)
	default:
		return nil, mapCatalogMetaBackupRaftErrorV1(snapshotErr)
	}
	if err != nil {
		return nil, errors.Join(ErrInvalidCatalogMetaBackup, err)
	}
	if reader == nil {
		return nil, errors.Join(ErrInvalidCatalogMetaBackup, fmt.Errorf("snapshot reader is unavailable"))
	}
	defer reader.Close()

	payload, err := readCatalogMetaBackupSnapshotV1(meta, reader)
	if err != nil {
		return nil, err
	}
	if err := backupState.ValidateCatalogMetaSnapshotBytesV1(payload); err != nil {
		return nil, errors.Join(ErrInvalidCatalogMetaBackup, fmt.Errorf("validate catalog snapshot: %w", err))
	}
	return encodeCatalogMetaBackupArchiveV1(meta, payload)
}

// RestoreCatalogMetaBackupV1 restores a checksummed backup through HashiCorp
// Raft's external Restore API. It is leader-only and refuses a target whose
// local authority already exposes any catalog generation. HashiCorp Raft then
// republishes the restored snapshot to followers and commits a no-op before
// returning.
func (p *CatalogMetaRaftProviderV1) RestoreCatalogMetaBackupV1(ctx context.Context, archive []byte) error {
	if p == nil || p.raft == nil || p.state == nil {
		return ErrInvalidHashicorpRaftProvider
	}
	decoded, err := decodeCatalogMetaBackupArchiveV1(archive)
	if err != nil {
		return err
	}
	p.mutationMu.Lock()
	defer p.mutationMu.Unlock()
	if ctx == nil {
		ctx = context.Background()
	}
	if err := requireCatalogMetaBackupLeaderV1(ctx, p); err != nil {
		return err
	}
	timeout := catalogMetaBackupTimeoutV1(ctx, p.applyTimeout)
	if timeout <= 0 {
		return ctx.Err()
	}
	// A node can report Leader before its initial fixed-voter configuration
	// entry is applied. Restore refuses while that configuration is pending;
	// the barrier also makes the fresh-cluster precondition observable at a
	// stable applied prefix.
	if err := waitHashicorpRaftFuture(ctx, p.raft.Barrier(timeout)); err != nil {
		return mapCatalogMetaBackupRaftErrorV1(err)
	}
	backupState, ok := p.state.(CatalogMetaBackupStateV1)
	if !ok {
		return errors.Join(ErrInvalidHashicorpRaftProvider, fmt.Errorf("catalog state does not support backup validation"))
	}
	if err := backupState.ValidateCatalogMetaSnapshotBytesV1(decoded.payload); err != nil {
		return errors.Join(ErrInvalidCatalogMetaBackup, fmt.Errorf("validate catalog snapshot: %w", err))
	}
	if err := backupState.ValidateCatalogMetaBackupRestoreTargetV1(); err != nil {
		return errors.Join(ErrCatalogMetaBackupRestoreTarget, err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	meta := &hraft.SnapshotMeta{
		Version: decoded.snapshotVersion,
		Term:    decoded.term,
		Index:   decoded.index,
		Size:    int64(len(decoded.payload)),
	}
	if err := p.raft.Restore(meta, bytes.NewReader(decoded.payload), timeout); err != nil {
		return mapCatalogMetaBackupRaftErrorV1(err)
	}
	return nil
}

func catalogMetaBackupTimeoutV1(ctx context.Context, fallback time.Duration) time.Duration {
	timeout := fallback
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return 0
		}
		if timeout <= 0 || remaining < timeout {
			timeout = remaining
		}
	}
	return timeout
}

func requireCatalogMetaBackupLeaderV1(ctx context.Context, p *CatalogMetaRaftProviderV1) error {
	status, err := p.ClusterAdmissionStatus(ctx)
	if err != nil {
		return err
	}
	if status.Unavailable {
		return ErrAdmissionUnavailable
	}
	if !status.Leader {
		return ErrNotLeader
	}
	return nil
}

func openLatestCatalogMetaSnapshotV1(store hraft.SnapshotStore) (*hraft.SnapshotMeta, io.ReadCloser, error) {
	if store == nil {
		return nil, nil, ErrRaftSnapshotUnsupported
	}
	metas, err := store.List()
	if err != nil {
		return nil, nil, err
	}
	if len(metas) == 0 || metas[0] == nil || metas[0].ID == "" {
		return nil, nil, fmt.Errorf("no retained catalog snapshot")
	}
	return store.Open(metas[0].ID)
}

func readCatalogMetaBackupSnapshotV1(meta *hraft.SnapshotMeta, reader io.Reader) ([]byte, error) {
	if meta == nil {
		return nil, errors.Join(ErrInvalidCatalogMetaBackup, fmt.Errorf("snapshot metadata is missing"))
	}
	if meta.Version < hraft.SnapshotVersionMin || meta.Version > hraft.SnapshotVersionMax {
		return nil, errors.Join(ErrInvalidCatalogMetaBackup, fmt.Errorf("unsupported snapshot version %d", meta.Version))
	}
	if meta.Term == 0 || meta.Index == 0 {
		return nil, errors.Join(ErrInvalidCatalogMetaBackup, fmt.Errorf("snapshot term/index %d/%d", meta.Term, meta.Index))
	}
	if meta.Size <= 0 || meta.Size > catalogMetaRaftSnapshotMaxBytesV1 {
		return nil, errors.Join(ErrInvalidCatalogMetaBackup, fmt.Errorf("snapshot size %d", meta.Size))
	}
	payload, err := io.ReadAll(io.LimitReader(reader, catalogMetaRaftSnapshotMaxBytesV1+1))
	if err != nil {
		return nil, errors.Join(ErrInvalidCatalogMetaBackup, err)
	}
	if len(payload) > catalogMetaRaftSnapshotMaxBytesV1 || int64(len(payload)) != meta.Size {
		return nil, errors.Join(ErrInvalidCatalogMetaBackup, fmt.Errorf("snapshot payload size %d does not match metadata %d", len(payload), meta.Size))
	}
	return payload, nil
}

func encodeCatalogMetaBackupArchiveV1(meta *hraft.SnapshotMeta, payload []byte) ([]byte, error) {
	if _, err := readCatalogMetaBackupSnapshotV1(meta, bytes.NewReader(payload)); err != nil {
		return nil, err
	}
	archive := make([]byte, catalogMetaBackupHeaderBytesV1+len(payload))
	copy(archive[0:8], catalogMetaBackupMagicV1[:])
	binary.BigEndian.PutUint16(archive[8:10], catalogMetaBackupFormatV1)
	binary.BigEndian.PutUint16(archive[10:12], uint16(meta.Version))
	binary.BigEndian.PutUint64(archive[12:20], meta.Term)
	binary.BigEndian.PutUint64(archive[20:28], meta.Index)
	binary.BigEndian.PutUint32(archive[28:32], uint32(len(payload)))
	copy(archive[catalogMetaBackupHeaderBytesV1:], payload)
	checksum := catalogMetaBackupChecksumV1(archive[:32], payload)
	copy(archive[32:64], checksum[:])
	return archive, nil
}

func decodeCatalogMetaBackupArchiveV1(archive []byte) (catalogMetaBackupArchiveV1, error) {
	if len(archive) < catalogMetaBackupHeaderBytesV1 || len(archive) > catalogMetaBackupHeaderBytesV1+catalogMetaRaftSnapshotMaxBytesV1 {
		return catalogMetaBackupArchiveV1{}, errors.Join(ErrInvalidCatalogMetaBackup, fmt.Errorf("archive size %d", len(archive)))
	}
	if !bytes.Equal(archive[0:8], catalogMetaBackupMagicV1[:]) {
		return catalogMetaBackupArchiveV1{}, errors.Join(ErrInvalidCatalogMetaBackup, fmt.Errorf("archive magic"))
	}
	if format := binary.BigEndian.Uint16(archive[8:10]); format != catalogMetaBackupFormatV1 {
		return catalogMetaBackupArchiveV1{}, errors.Join(ErrInvalidCatalogMetaBackup, fmt.Errorf("archive format %d", format))
	}
	version := hraft.SnapshotVersion(binary.BigEndian.Uint16(archive[10:12]))
	if version < hraft.SnapshotVersionMin || version > hraft.SnapshotVersionMax {
		return catalogMetaBackupArchiveV1{}, errors.Join(ErrInvalidCatalogMetaBackup, fmt.Errorf("unsupported snapshot version %d", version))
	}
	term := binary.BigEndian.Uint64(archive[12:20])
	index := binary.BigEndian.Uint64(archive[20:28])
	if term == 0 || index == 0 {
		return catalogMetaBackupArchiveV1{}, errors.Join(ErrInvalidCatalogMetaBackup, fmt.Errorf("snapshot term/index %d/%d", term, index))
	}
	payloadLen := int(binary.BigEndian.Uint32(archive[28:32]))
	if payloadLen == 0 || payloadLen > catalogMetaRaftSnapshotMaxBytesV1 || payloadLen != len(archive)-catalogMetaBackupHeaderBytesV1 {
		return catalogMetaBackupArchiveV1{}, errors.Join(ErrInvalidCatalogMetaBackup, fmt.Errorf("snapshot payload size %d", payloadLen))
	}
	payload := bytes.Clone(archive[catalogMetaBackupHeaderBytesV1:])
	checksum := catalogMetaBackupChecksumV1(archive[:32], payload)
	if subtle.ConstantTimeCompare(archive[32:64], checksum[:]) != 1 {
		return catalogMetaBackupArchiveV1{}, errors.Join(ErrInvalidCatalogMetaBackup, fmt.Errorf("archive checksum"))
	}
	return catalogMetaBackupArchiveV1{
		snapshotVersion: version,
		term:            term,
		index:           index,
		payload:         payload,
	}, nil
}

func catalogMetaBackupChecksumV1(header, payload []byte) [sha256.Size]byte {
	digest := sha256.New()
	_, _ = digest.Write(header)
	_, _ = digest.Write(payload)
	var result [sha256.Size]byte
	copy(result[:], digest.Sum(nil))
	return result
}

func mapCatalogMetaBackupRaftErrorV1(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return err
	case errors.Is(err, hraft.ErrNotLeader),
		errors.Is(err, hraft.ErrLeadershipLost),
		errors.Is(err, hraft.ErrLeadershipTransferInProgress):
		return errors.Join(ErrNotLeader, err)
	case errors.Is(err, hraft.ErrRaftShutdown),
		errors.Is(err, hraft.ErrEnqueueTimeout),
		errors.Is(err, hraft.ErrAbortedByRestore):
		return errors.Join(ErrHashicorpRaftUnavailable, err)
	default:
		return errors.Join(ErrInvalidCatalogMetaBackup, err)
	}
}
