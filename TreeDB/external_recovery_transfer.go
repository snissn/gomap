package treedb

import (
	"archive/tar"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// ExportCheckpoint streams the exact requested certificate and its complete
// physical DATA image. It refuses a stale request rather than substituting a
// newer semantic cut. The stream is a portable snapshot, unlike root.json.
// Checkpoint/Close wait for export; Apply can continue while pinned file prefixes
// are copied. Full image work is confined to explicit snapshot transfer.
func (o *ExternalRecovery) ExportCheckpoint(ctx context.Context, dst io.Writer, expectedGeneration uint64, expectedDigest string) error {
	if o == nil || dst == nil {
		return ErrExternalRecovery
	}
	if ctx == nil {
		ctx = context.Background()
	}
	o.transferMu.RLock()
	defer o.transferMu.RUnlock()
	o.mu.RLock()
	if o.closed {
		o.mu.RUnlock()
		return ErrClosed
	}
	if o.poison != nil {
		err := o.poison
		o.mu.RUnlock()
		return err
	}
	cert := o.checkpoint
	if cert.Generation == 0 || cert.Generation != expectedGeneration || cert.Digest != expectedDigest {
		o.mu.RUnlock()
		return fmt.Errorf("%w: stale checkpoint export request", ErrExternalRecovery)
	}
	var cut *backenddb.PhysicalSnapshotCutV1
	for _, retained := range o.retained {
		if retained.generation == cert.Generation {
			cut = retained.cut
		}
	}
	o.mu.RUnlock()
	if cut == nil {
		return ErrExternalRecovery
	}
	root, err := o.dataPath(cert.DataDirectory)
	if err != nil {
		return err
	}
	parent, err := rootpublication.OpenStableParent(root)
	if err != nil {
		return err
	}
	defer parent.Close()
	if err := cut.ValidateStorageDirectoryV1(parent); err != nil {
		return err
	}
	type captured struct {
		name string
		size int64
		file *os.File
	}
	var files []captured
	defer func() {
		for _, file := range files {
			_ = file.file.Close()
		}
	}()
	var capturedBytes int64 = cut.SizeBytes()
	directoryCount := 0
	var captureDir func(*os.File, string, int) error
	captureDir = func(directory *os.File, prefix string, depth int) error {
		directoryCount++
		if depth > 16 || directoryCount > o.config.MaxCheckpointFiles {
			return ErrExternalRecovery
		}
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			entries, err := directory.ReadDir(64)
			if err != nil && !errors.Is(err, io.EOF) {
				return err
			}
			for _, entry := range entries {
				if entry.Type()&os.ModeSymlink != 0 {
					return ErrExternalRecovery
				}
				file, err := rootpublication.OpenStableChildFile(directory, entry.Name(), os.O_RDONLY, 0)
				if err != nil {
					return err
				}
				info, statErr := file.Stat()
				linkErr := rootpublication.ValidateStableChildLink(directory, file, entry.Name())
				if statErr != nil || linkErr != nil {
					return errors.Join(statErr, linkErr, file.Close())
				}
				name := path.Join(prefix, entry.Name())
				if info.IsDir() {
					captureErr := captureDir(file, name, depth+1)
					if err := errors.Join(captureErr, file.Close()); err != nil {
						return err
					}
					continue
				}
				if !info.Mode().IsRegular() || len(files)+1 >= o.config.MaxCheckpointFiles || info.Size() < 0 || info.Size() > o.config.MaxCheckpointBytes-capturedBytes {
					_ = file.Close()
					return ErrExternalRecovery
				}
				capturedBytes += info.Size()
				files = append(files, captured{name, info.Size(), file})
			}
			if errors.Is(err, io.EOF) {
				return nil
			}
		}
	}
	// Match the maintained physical archive seam: retain exact child handles
	// before deferred copy, with index bytes exported by PhysicalSnapshotCutV1.
	for _, name := range []string{"format.json", "value_vlog", "leaf_vlog"} {
		file, err := rootpublication.OpenStableChildFile(parent, name, os.O_RDONLY, 0)
		if errors.Is(err, os.ErrNotExist) && name != "format.json" {
			continue
		}
		if err != nil {
			return err
		}
		info, err := file.Stat()
		if err != nil {
			_ = file.Close()
			return err
		}
		if err := rootpublication.ValidateStableChildLink(parent, file, name); err != nil {
			_ = file.Close()
			return err
		}
		if info.IsDir() {
			captureErr := captureDir(file, name, 0)
			if err := errors.Join(captureErr, file.Close()); err != nil {
				return err
			}
		} else {
			if name != "format.json" || !info.Mode().IsRegular() || info.Size() > o.config.MaxCheckpointBytes-capturedBytes || len(files)+1 >= o.config.MaxCheckpointFiles {
				_ = file.Close()
				return ErrExternalRecovery
			}
			capturedBytes += info.Size()
			files = append(files, captured{name, info.Size(), file})
		}
	}
	if capturedBytes > o.config.MaxCheckpointBytes {
		return ErrExternalRecovery
	}
	writer := tar.NewWriter(dst)
	var inventory []externalCheckpointFile
	write := func(name string, size int64, copy func(io.Writer) error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := writer.WriteHeader(&tar.Header{Name: name, Mode: 0600, Size: size, Typeflag: tar.TypeReg}); err != nil {
			return err
		}
		hash := sha256.New()
		if err := copy(io.MultiWriter(writer, hash)); err != nil {
			return err
		}
		inventory = append(inventory, externalCheckpointFile{name, size, hex.EncodeToString(hash.Sum(nil))})
		return nil
	}
	raw, err := json.Marshal(cert)
	if err != nil {
		return err
	}
	if err := write("source.json", int64(len(raw)), func(w io.Writer) error { _, err := w.Write(raw); return err }); err != nil {
		return err
	}
	if err := write("db/index.db", cut.SizeBytes(), func(w io.Writer) error { return cut.WriteToContext(ctx, w) }); err != nil {
		return err
	}
	buffer := make([]byte, 64<<10)
	for _, file := range files {
		if err := write("db/"+file.name, file.size, func(w io.Writer) error {
			_, err := io.CopyBuffer(w, externalContextReader{ctx, io.NewSectionReader(file.file, 0, file.size)}, buffer)
			return err
		}); err != nil {
			return err
		}
	}
	if err := cut.ValidateStorageDirectoryV1(parent); err != nil {
		return err
	}
	raw, err = json.Marshal(inventory)
	if err != nil || len(raw) > 4<<20 {
		return errors.Join(err, ErrExternalRecovery)
	}
	if err := writer.WriteHeader(&tar.Header{Name: "inventory.json", Mode: 0600, Size: int64(len(raw)), Typeflag: tar.TypeReg}); err != nil {
		return err
	}
	if _, err := writer.Write(raw); err != nil {
		return err
	}
	return writer.Close()
}

type externalContextReader struct {
	context context.Context
	reader  io.Reader
}

func (reader externalContextReader) Read(dst []byte) (int, error) {
	if err := reader.context.Err(); err != nil {
		return 0, err
	}
	return reader.reader.Read(dst)
}

// InstallCheckpoint requires the external application to drain ALL outstanding
// DB/MVCC snapshots, iterators, readers and queued writes before calling it.
// Success changes DB()'s pointer; the caller must rebuild its adapters before
// read/write admission resumes. authorize MUST validate the source semantic
// group/index/term and accepted snapshot authority, then explicitly return the
// receiver's progress metadata. It runs after complete archive validation and
// before any receiver CURRENT publication; a cross-store caller must durably
// persist its accepted install intent here. It must not call into this owner.
// Preparation errors preserve the old DB. Ambiguous publication poisons the
// owner and requires Close/reopen before any further use.
func (o *ExternalRecovery) InstallCheckpoint(ctx context.Context, src io.Reader, authorize func(ExternalRecoveryCheckpoint) ([]byte, error)) (ExternalRecoveryCheckpoint, error) {
	if o == nil || src == nil || authorize == nil {
		return ExternalRecoveryCheckpoint{}, ErrExternalRecovery
	}
	if ctx == nil {
		ctx = context.Background()
	}
	o.transferMu.Lock()
	defer o.transferMu.Unlock()
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return ExternalRecoveryCheckpoint{}, ErrClosed
	}
	if o.poison != nil {
		return ExternalRecoveryCheckpoint{}, o.poison
	}
	stage, err := externalMkdirTemp(o.dir, ".install-")
	if err != nil {
		return ExternalRecoveryCheckpoint{}, err
	}
	defer externalRemoveAll(stage)
	source, err := o.extractCheckpoint(ctx, src, stage)
	if err != nil {
		return ExternalRecoveryCheckpoint{}, err
	}
	// The private stage edge is stable too before external acceptance becomes
	// durable. The caller still retains its original archive for restart replay.
	if err := externalSyncDir(o.dir); err != nil {
		return ExternalRecoveryCheckpoint{}, err
	}
	metadata, err := authorize(cloneExternalCheckpoint(source))
	if err != nil {
		return ExternalRecoveryCheckpoint{}, err
	}
	if len(metadata) > 1<<20 {
		return ExternalRecoveryCheckpoint{}, ErrExternalRecovery
	}
	if err := ctx.Err(); err != nil {
		return ExternalRecoveryCheckpoint{}, err
	}
	name := "materialized-" + filepath.Base(stage)[len(".install-"):]
	final, err := o.dataPath(name)
	if err != nil {
		return ExternalRecoveryCheckpoint{}, err
	}
	if err := externalRename(stage, final); err != nil {
		return ExternalRecoveryCheckpoint{}, err
	}
	publicationAttempted := false
	defer func() {
		if !publicationAttempted {
			_ = externalRemoveAll(final)
		}
	}()
	if err := externalSyncDir(o.dir); err != nil {
		return ExternalRecoveryCheckpoint{}, err
	}
	if err := backenddb.RebindDurableRootSnapshotLayoutWithContextV1(ctx, final, ""); err != nil {
		return ExternalRecoveryCheckpoint{}, err
	}
	if err := o.cut("install-data-stable"); err != nil {
		return ExternalRecoveryCheckpoint{}, err
	}
	marker, _ := json.Marshal(struct {
		Version  int
		Identity string
	}{2, o.config.Identity})
	if err := externalAtomicFile(filepath.Join(final, externalRecoveryMarker), marker); err != nil {
		return ExternalRecoveryCheckpoint{}, err
	}
	opts := o.options
	opts.Dir = final
	database, err := openResolved(opts)
	if err != nil {
		return ExternalRecoveryCheckpoint{}, err
	}
	installed := false
	defer func() {
		if !installed {
			_ = database.Close()
		}
	}()
	state, present := database.backend.StateToken()
	if !present || externalPersistentRoot(state) != source.Root {
		return ExternalRecoveryCheckpoint{}, fmt.Errorf("%w: transferred state differs from source certificate", ErrExternalRecovery)
	}
	cut, err := database.backend.CapturePhysicalSnapshotCutV1(ctx)
	if err != nil {
		return ExternalRecoveryCheckpoint{}, err
	}
	// publishCut retains the new cut when publication may have happened. An
	// error before that point permits deleting the unpublished physical namespace.
	cert, err := o.publishCut(ctx, cut, metadata, name)
	publicationAttempted = o.poison != nil || cert.Generation != 0 || o.checkpoint.DataDirectory == name
	if err != nil {
		if publicationAttempted {
			o.poison = fmt.Errorf("%w: install publication requires reopen: %v", ErrExternalRecovery, err)
		}
		return ExternalRecoveryCheckpoint{}, err
	}
	old := o.db
	o.db, o.options, o.dataDirectory = database, opts, name
	installed = true
	if err := old.Close(); err != nil {
		o.poison = err
		return ExternalRecoveryCheckpoint{}, err
	}
	return cert, nil
}

func externalTransferPath(name string) (string, error) {
	if !strings.HasPrefix(name, "db/") || path.Clean(name) != name || strings.Contains(name, "\\") {
		return "", ErrExternalRecovery
	}
	rel := strings.TrimPrefix(name, "db/")
	parts := strings.Split(rel, "/")
	if len(parts) > 18 || len(name) > 4096 {
		return "", ErrExternalRecovery
	}
	if rel != "index.db" && rel != "format.json" && !(len(parts) > 1 && (parts[0] == "value_vlog" || parts[0] == "leaf_vlog")) {
		return "", ErrExternalRecovery
	}
	return rel, nil
}

func (o *ExternalRecovery) extractCheckpoint(ctx context.Context, src io.Reader, stage string) (ExternalRecoveryCheckpoint, error) {
	var source ExternalRecoveryCheckpoint
	reader := tar.NewReader(externalContextReader{ctx, src})
	var actual []externalCheckpointFile
	seen := make(map[string]bool)
	buffer := make([]byte, 64<<10)
	var bytes int64
	for {
		header, err := reader.Next()
		if err != nil {
			return source, errors.Join(err, ErrExternalRecovery)
		}
		if header.Typeflag != tar.TypeReg || header.Size < 0 || seen[header.Name] {
			return source, ErrExternalRecovery
		}
		seen[header.Name] = true
		if header.Name == "inventory.json" {
			if header.Size > 4<<20 {
				return source, ErrExternalRecovery
			}
			raw, err := io.ReadAll(reader)
			if err != nil {
				return source, err
			}
			var expected []externalCheckpointFile
			if err := json.Unmarshal(raw, &expected); err != nil {
				return source, err
			}
			if len(expected) != len(actual) || !seen["db/index.db"] || !seen["db/format.json"] {
				return source, ErrExternalRecovery
			}
			for i := range actual {
				if actual[i] != expected[i] {
					return source, ErrExternalRecovery
				}
			}
			if _, err := reader.Next(); !errors.Is(err, io.EOF) {
				return source, ErrExternalRecovery
			}
			return source, nil
		}
		if len(actual) >= o.config.MaxCheckpointFiles+1 {
			return source, ErrExternalRecovery
		}
		hash := sha256.New()
		if header.Name == "source.json" {
			if len(actual) != 0 || header.Size > 4<<20 {
				return source, ErrExternalRecovery
			}
			raw, err := io.ReadAll(io.TeeReader(reader, hash))
			if err != nil {
				return source, err
			}
			if err := json.Unmarshal(raw, &source); err != nil {
				return source, err
			}
			digest, err := externalDigest(source)
			if err != nil || source.Version != 2 || source.Generation == 0 || digest != source.Digest || len(source.Metadata) > 1<<20 {
				return source, ErrExternalRecovery
			}
		} else {
			if !seen["source.json"] || header.Size > o.config.MaxCheckpointBytes-bytes {
				return source, ErrExternalRecovery
			}
			rel, err := externalTransferPath(header.Name)
			if err != nil {
				return source, err
			}
			bytes += header.Size
			filePath := filepath.Join(stage, filepath.FromSlash(rel))
			if err := backenddb.EnsureStorageLayoutDirs(0700, filepath.Dir(filePath)); err != nil {
				return source, err
			}
			file, err := os.OpenFile(filePath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
			if err != nil {
				return source, err
			}
			if err := durabilitycut.EmitNamespace(durabilitycut.NamespaceCreate, durabilitycut.ResourceAuxiliary, stage, "", filePath); err != nil {
				_ = file.Close()
				return source, err
			}
			_, copyErr := io.CopyBuffer(io.MultiWriter(file, hash), reader, buffer)
			resource := durabilitycut.ResourceAuxiliary
			switch {
			case rel == "index.db":
				resource = durabilitycut.ResourceIndex
			case strings.HasPrefix(rel, "value_vlog/"):
				resource = durabilitycut.ResourceValueLog
			case strings.HasPrefix(rel, "leaf_vlog/"):
				resource = durabilitycut.ResourceOuterLeaf
			}
			if err := errors.Join(copyErr, externalSyncFile(file, resource), file.Close()); err != nil {
				return source, err
			}
			if err := externalSyncDir(filepath.Dir(filePath)); err != nil {
				return source, err
			}
		}
		actual = append(actual, externalCheckpointFile{header.Name, header.Size, hex.EncodeToString(hash.Sum(nil))})
	}
}
