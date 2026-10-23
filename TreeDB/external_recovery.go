package treedb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"unicode/utf8"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/lockfile"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// ErrExternalRecovery identifies a refused external-recovery contract. This
// experimental API deliberately does not change the meaning of WriteSync.
var ErrExternalRecovery = errors.New("treedb: external recovery contract")

const externalRecoveryMarker = "external-recovery.json"

// ExternalRecoveryConfig names the authoritative recovery owner. The caller
// must durably retain committed commands/decisions until a certified checkpoint
// covers them. Limits apply to one physical checkpoint, not to upstream log debt.
type ExternalRecoveryConfig struct {
	Identity           string
	MaxCheckpointBytes int64
	MaxCheckpointFiles int
}

// ExternalRecoveryCheckpoint certifies one immutable physical image together
// with opaque application progress. Metadata must be sampled at the same
// application barrier as the data. Digest binds the complete file inventory.
type ExternalRecoveryCheckpoint struct {
	Version    int                      `json:"version"`
	Identity   string                   `json:"identity"`
	Generation uint64                   `json:"generation"`
	Root       backenddb.StateToken     `json:"root"`
	Metadata   []byte                   `json:"metadata"`
	Files      []externalCheckpointFile `json:"files"`
	Digest     string                   `json:"digest"`
}

type externalCheckpointFile struct {
	Path   string `json:"path"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}

// ExternalRecovery owns an opt-in, externally logged materialization. Apply
// calls may overlap; Checkpoint and Close exclude them. DB must only be mutated
// inside Apply, and no asynchronous mutation may outlive its callback.
//
// Prototype checkpoints copy a maintained physical snapshot into an immutable
// generation. This proves exact-root recovery without adding a new durable root
// pin implementation; its O(database image) checkpoint cost is explicit.
type ExternalRecovery struct {
	mu         sync.RWMutex
	db         *DB
	dir        string
	config     ExternalRecoveryConfig
	lock       *lockfile.Lock
	checkpoint ExternalRecoveryCheckpoint
	closed     bool
	poison     error
	// Per-owner fault injection never changes another database's behavior.
	testCut func(string) error
}

// OpenExternalRecovery opens a fresh or previously owned prototype directory.
// It restores exactly CURRENT before opening TreeDB: even cleanly flushed work
// beyond the certificate is discarded and must be replayed by the external owner.
// First-time initialization must be exclusive across recovery policies: an
// ordinary Open racing before this owner's marker exists uses a different lock.
func OpenExternalRecovery(opts Options, config ExternalRecoveryConfig) (_ *ExternalRecovery, err error) {
	if opts.Dir == "" || config.Identity == "" || len(config.Identity) > 4096 || !utf8.ValidString(config.Identity) || opts.ReadOnly {
		return nil, fmt.Errorf("%w: directory, identity and writable owner required", ErrExternalRecovery)
	}
	if !rootpublication.StableRelativeNamespaceSupported() {
		return nil, fmt.Errorf("%w: durable relative namespaces unsupported", ErrExternalRecovery)
	}
	if err := resolveOpenProfileOptions(&opts); err != nil {
		return nil, err
	}
	if opts.Durability != DurabilityWALOffRelaxed || opts.CommandWAL || opts.MemtableMode != "cow_ordered" {
		return nil, fmt.Errorf("%w: explicit no_wal_fast cow_ordered materialization required", ErrExternalRecovery)
	}
	if config.MaxCheckpointBytes == 0 {
		config.MaxCheckpointBytes = 8 << 30
	}
	if config.MaxCheckpointFiles == 0 {
		config.MaxCheckpointFiles = 8192
	}
	if config.MaxCheckpointBytes < 1 || config.MaxCheckpointFiles < 1 {
		return nil, ErrExternalRecovery
	}
	markerBytes, _ := json.Marshal(struct {
		Version  int
		Identity string
	}{1, config.Identity})
	if len(markerBytes) > 8192 {
		return nil, fmt.Errorf("%w: encoded owner identity exceeds marker limit", ErrExternalRecovery)
	}
	dir, err := filepath.Abs(opts.Dir)
	if err != nil {
		return nil, err
	}
	if err := backenddb.EnsureStorageLayoutDirsForOpen(0700, filepath.Join(dir, externalRecoveryMarker), dir); err != nil {
		return nil, err
	}
	lock, err := lockfile.Acquire(filepath.Join(dir, "EXTERNAL.LOCK"))
	if err != nil {
		return nil, err
	}
	o := &ExternalRecovery{dir: dir, config: config, lock: lock}
	defer func() {
		if err != nil {
			_ = lock.Close()
		}
	}()
	markerPath := filepath.Join(dir, externalRecoveryMarker)
	marker, readErr := readExternalBounded(markerPath, 8192)
	if errors.Is(readErr, os.ErrNotExist) {
		entries, e := os.ReadDir(dir)
		if e != nil {
			return nil, e
		}
		for _, entry := range entries {
			if entry.Name() != "EXTERNAL.LOCK" {
				return nil, fmt.Errorf("%w: refusing nonempty unowned directory", ErrExternalRecovery)
			}
		}
		if err := externalAtomicFile(markerPath, markerBytes); err != nil {
			return nil, err
		}
	} else if readErr != nil {
		return nil, readErr
	} else {
		var m struct {
			Version  int
			Identity string
		}
		if json.Unmarshal(marker, &m) != nil || m.Version != 1 || m.Identity != config.Identity {
			return nil, fmt.Errorf("%w: owner identity/version mismatch", ErrExternalRecovery)
		}
	}
	if err := backenddb.EnsureStorageLayoutDirs(0700, filepath.Join(dir, "checkpoints")); err != nil {
		return nil, err
	}
	cert, ok, err := o.readCurrent()
	if err != nil {
		return nil, err
	}
	// Validate the immutable image completely before removing disposable work.
	if ok {
		if err := o.verifyImage(cert); err != nil {
			return nil, err
		}
		o.checkpoint = cert
	}
	working := filepath.Join(dir, "working")
	if err := os.RemoveAll(working); err != nil {
		return nil, err
	}
	if ok {
		if err := externalCopyTree(context.Background(), o.imageDir(cert.Generation), working, config); err != nil {
			return nil, err
		}
		if err := backenddb.RebindDurableRootSnapshotV1(working); err != nil {
			return nil, err
		}
	}
	opts.Dir, opts.DisableSideStores = working, true
	opts.BackgroundCheckpointInterval, opts.BackgroundCheckpointIdleDuration = -1, -1
	opts.BackgroundIndexVacuumInterval, opts.DisableBackgroundPrune = -1, true
	o.db, err = Open(opts)
	if err != nil {
		return nil, err
	}
	if ok {
		state, present := o.db.backend.StateToken()
		if !present || externalPersistentRoot(state) != cert.Root {
			_ = o.db.Close()
			return nil, fmt.Errorf("%w: restored root %+v differs from certificate %+v", ErrExternalRecovery, state, cert.Root)
		}
	}
	if err := o.cleanupImages(); err != nil {
		_ = o.db.Close()
		return nil, err
	}
	return o, nil
}

// DB exposes the existing TreeDB/MVCC implementation, not a second publisher.
// Its lifetime and all mutations remain owned by ExternalRecovery.
func (o *ExternalRecovery) DB() *DB { return o.db }

// Apply acknowledges complete materialization, not a local durable WriteSync.
// Its caller must already possess the durable external recovery authority.
func (o *ExternalRecovery) Apply(fn func() error) error {
	if o == nil || fn == nil {
		return ErrExternalRecovery
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	if o.closed {
		return ErrClosed
	}
	if o.poison != nil {
		return o.poison
	}
	return fn()
}

func (o *ExternalRecovery) RecoveredCheckpoint() (ExternalRecoveryCheckpoint, bool) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	c := o.checkpoint
	return cloneExternalCheckpoint(c), c.Generation != 0
}

func cloneExternalCheckpoint(c ExternalRecoveryCheckpoint) ExternalRecoveryCheckpoint {
	c.Metadata = append([]byte(nil), c.Metadata...)
	c.Files = append([]externalCheckpointFile(nil), c.Files...)
	return c
}

// Checkpoint seals data and metadata before atomically publishing CURRENT. The
// application must hold its semantic apply barrier while selecting metadata and
// calling this method. A returned error never authorizes upstream log deletion.
func (o *ExternalRecovery) Checkpoint(ctx context.Context, metadata []byte) (ExternalRecoveryCheckpoint, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return ExternalRecoveryCheckpoint{}, ErrClosed
	}
	if o.poison != nil {
		return ExternalRecoveryCheckpoint{}, o.poison
	}
	if len(metadata) > 1<<20 {
		return ExternalRecoveryCheckpoint{}, ErrExternalRecovery
	}
	if err := ctx.Err(); err != nil {
		return ExternalRecoveryCheckpoint{}, err
	}
	if err := o.db.Checkpoint(); err != nil {
		return ExternalRecoveryCheckpoint{}, err
	}
	cut, err := o.db.backend.CapturePhysicalSnapshotCutV1(ctx)
	if err != nil {
		return ExternalRecoveryCheckpoint{}, err
	}
	defer cut.Close()
	gen := o.checkpoint.Generation + 1
	if gen == 0 {
		return ExternalRecoveryCheckpoint{}, ErrExternalRecovery
	}
	stage, err := os.MkdirTemp(filepath.Join(o.dir, "checkpoints"), ".staging-")
	if err != nil {
		return ExternalRecoveryCheckpoint{}, err
	}
	defer os.RemoveAll(stage)
	image := filepath.Join(stage, "db")
	if err := externalCopyTree(ctx, filepath.Join(o.dir, "working"), image, o.config); err != nil {
		return ExternalRecoveryCheckpoint{}, err
	}
	// Use the maintained immutable cut instead of copying mutable meta/free pages.
	if err := externalWriteFile(filepath.Join(image, "index.db"), func(w io.Writer) error { return cut.WriteToContext(ctx, w) }); err != nil {
		return ExternalRecoveryCheckpoint{}, err
	}
	if cut.PrimarySizeBytes() != 0 {
		if err := externalWriteFile(filepath.Join(image, "index.db.primary"), func(w io.Writer) error { return cut.WritePrimaryToContext(ctx, w) }); err != nil {
			return ExternalRecoveryCheckpoint{}, err
		}
	}
	if err := backenddb.RebindDurableRootSnapshotV1(image); err != nil {
		return ExternalRecoveryCheckpoint{}, err
	}
	cert := ExternalRecoveryCheckpoint{Version: 1, Identity: o.config.Identity, Generation: gen, Root: externalPersistentRoot(cut.StateToken()), Metadata: append([]byte(nil), metadata...)}
	cert.Files, err = externalInventory(ctx, image, o.config)
	if err != nil {
		return ExternalRecoveryCheckpoint{}, err
	}
	cert.Digest, err = externalDigest(cert)
	if err != nil {
		return ExternalRecoveryCheckpoint{}, err
	}
	raw, err := json.Marshal(cert)
	if err != nil {
		return ExternalRecoveryCheckpoint{}, err
	}
	if len(raw) > 4<<20 {
		return ExternalRecoveryCheckpoint{}, fmt.Errorf("%w: encoded certificate exceeds recovery read limit", ErrExternalRecovery)
	}
	if err := externalAtomicFile(filepath.Join(stage, "certificate.json"), raw); err != nil {
		return ExternalRecoveryCheckpoint{}, err
	}
	if err := o.cut("image-stable"); err != nil {
		return ExternalRecoveryCheckpoint{}, err
	}
	final := filepath.Join(o.dir, "checkpoints", fmt.Sprintf("%020d", gen))
	// A failed previous attempt can leave the next generation unreferenced.
	if err := os.RemoveAll(final); err != nil {
		return ExternalRecoveryCheckpoint{}, err
	}
	if err := os.Rename(stage, final); err != nil {
		return ExternalRecoveryCheckpoint{}, err
	}
	if err := externalSyncDir(filepath.Dir(final)); err != nil {
		return ExternalRecoveryCheckpoint{}, err
	}
	if err := o.cut("before-current"); err != nil {
		return ExternalRecoveryCheckpoint{}, err
	}
	pointer, _ := json.Marshal(struct {
		Generation uint64
		Digest     string
	}{gen, cert.Digest})
	if err := externalAtomicFile(filepath.Join(o.dir, "CURRENT"), pointer); err != nil {
		o.poison = fmt.Errorf("%w: ambiguous checkpoint publication: %v", ErrExternalRecovery, err)
		return ExternalRecoveryCheckpoint{}, o.poison
	}
	o.checkpoint = cert
	if err := o.cut("current-stable"); err != nil {
		o.poison = err
		return ExternalRecoveryCheckpoint{}, err
	}
	return cloneExternalCheckpoint(cert), o.cleanupImages()
}

func externalPersistentRoot(state backenddb.StateToken) backenddb.StateToken {
	// This is a process-local leaf-generation view version, reset on Open.
	// The actual persistent root IDs, revision and commit sequence are retained.
	state.LeafGenerationStateVersion = 0
	return state
}

func (o *ExternalRecovery) cleanupImages() error {
	root := filepath.Join(o.dir, "checkpoints")
	entries, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	current := o.checkpoint.Generation
	for _, entry := range entries {
		if !entry.IsDir() || entry.Type()&os.ModeSymlink != 0 {
			return ErrExternalRecovery
		}
		name := entry.Name()
		remove := strings.HasPrefix(name, ".staging-")
		if !remove {
			var n uint64
			_, err := fmt.Sscanf(name, "%d", &n)
			if err != nil || fmt.Sprintf("%020d", n) != name {
				return ErrExternalRecovery
			}
			remove = n > current || (n < current && current-n > 1)
		}
		if remove {
			if err := os.RemoveAll(filepath.Join(root, name)); err != nil {
				return err
			}
		}
	}
	return externalSyncDir(root)
}

func (o *ExternalRecovery) cut(stage string) error {
	if o.testCut != nil {
		return o.testCut(stage)
	}
	return nil
}

// Close releases working storage without advancing certified progress. A later
// open discards any suffix persisted incidentally by the ordinary clean Close.
func (o *ExternalRecovery) Close() error {
	if o == nil {
		return nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return nil
	}
	o.closed = true
	return errors.Join(o.db.Close(), o.lock.Close())
}

func (o *ExternalRecovery) imageDir(gen uint64) string {
	return filepath.Join(o.dir, "checkpoints", fmt.Sprintf("%020d", gen), "db")
}

func (o *ExternalRecovery) readCurrent() (ExternalRecoveryCheckpoint, bool, error) {
	var cert ExternalRecoveryCheckpoint
	raw, err := readExternalBounded(filepath.Join(o.dir, "CURRENT"), 8192)
	if errors.Is(err, os.ErrNotExist) {
		return cert, false, nil
	}
	if err != nil {
		return cert, false, err
	}
	var pointer struct {
		Generation uint64
		Digest     string
	}
	if json.Unmarshal(raw, &pointer) != nil || pointer.Generation == 0 || len(pointer.Digest) != 64 {
		return cert, false, ErrExternalRecovery
	}
	raw, err = readExternalBounded(filepath.Join(filepath.Dir(o.imageDir(pointer.Generation)), "certificate.json"), 4<<20)
	if err != nil {
		return cert, false, err
	}
	if err := json.Unmarshal(raw, &cert); err != nil {
		return cert, false, err
	}
	digest, err := externalDigest(cert)
	if err != nil || cert.Version != 1 || cert.Identity != o.config.Identity || cert.Generation != pointer.Generation || digest != cert.Digest || digest != pointer.Digest {
		return cert, false, fmt.Errorf("%w: certificate identity/digest mismatch", ErrExternalRecovery)
	}
	return cert, true, nil
}

func (o *ExternalRecovery) verifyImage(cert ExternalRecoveryCheckpoint) error {
	files, err := externalInventory(context.Background(), o.imageDir(cert.Generation), o.config)
	if err != nil {
		return err
	}
	if len(files) != len(cert.Files) {
		return ErrExternalRecovery
	}
	for i := range files {
		if files[i] != cert.Files[i] {
			return fmt.Errorf("%w: checkpoint file mismatch: %s", ErrExternalRecovery, files[i].Path)
		}
	}
	return nil
}

func externalDigest(cert ExternalRecoveryCheckpoint) (string, error) {
	cert.Digest = ""
	raw, err := json.Marshal(cert)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func readExternalBounded(path string, max int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, max+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > max {
		return nil, ErrExternalRecovery
	}
	return b, nil
}

func externalInventory(ctx context.Context, root string, config ExternalRecoveryConfig) ([]externalCheckpointFile, error) {
	var files []externalCheckpointFile
	var bytes int64
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return ErrExternalRecovery
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || len(files) >= config.MaxCheckpointFiles || info.Size() > config.MaxCheckpointBytes-bytes {
			return ErrExternalRecovery
		}
		bytes += info.Size()
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		h := sha256.New()
		_, copyErr := io.Copy(h, f)
		if err := errors.Join(copyErr, f.Close()); err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		files = append(files, externalCheckpointFile{filepath.ToSlash(rel), info.Size(), hex.EncodeToString(h.Sum(nil))})
		return nil
	})
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	return files, err
}

// externalCopyTree only copies known physical raw-MVCC storage; application,
// side-store and arbitrary lifecycle namespaces are refused rather than omitted.
func externalCopyTree(ctx context.Context, source, target string, config ExternalRecoveryConfig) error {
	var bytes int64
	var count int
	return filepath.WalkDir(source, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		rel, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return ErrExternalRecovery
		}
		if rel != "." {
			parts := strings.Split(filepath.ToSlash(rel), "/")
			if parts[0] == "LOCK" || parts[0] == "vlog_ref_counts.meta" {
				// Refcounts are an optional scan cache, never recovery authority.
				return nil
			}
			// Open creates this namespace even for raw MVCC. Native assets are
			// outside this prototype's replay surface; accept only its empty dir.
			if parts[0] == "column_assets" && entry.IsDir() {
				entries, err := os.ReadDir(path)
				if err != nil {
					return err
				}
				if len(entries) != 0 {
					return fmt.Errorf("%w: native assets unsupported", ErrExternalRecovery)
				}
				return backenddb.EnsureStorageLayoutDirs(0700, filepath.Join(target, rel))
			}
			allowed := parts[0] == "index.db" || parts[0] == "index.db.primary" || parts[0] == "format.json" || parts[0] == "value_vlog" || parts[0] == "leaf_vlog" || parts[0] == "wal"
			if !allowed {
				return fmt.Errorf("%w: unsupported checkpoint namespace %q", ErrExternalRecovery, parts[0])
			}
		}
		dest := filepath.Join(target, rel)
		if entry.IsDir() {
			return backenddb.EnsureStorageLayoutDirs(0700, dest)
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		count++
		if !info.Mode().IsRegular() || count > config.MaxCheckpointFiles || info.Size() > config.MaxCheckpointBytes-bytes {
			return ErrExternalRecovery
		}
		bytes += info.Size()
		return externalWriteFile(dest, func(w io.Writer) error {
			f, err := os.Open(path)
			if err != nil {
				return err
			}
			defer f.Close()
			_, err = io.CopyN(w, f, info.Size())
			return err
		})
	})
}

func externalWriteFile(path string, write func(io.Writer) error) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	writeErr := write(f)
	if writeErr == nil {
		writeErr = rootpublication.SyncStableFile(f)
	}
	if err := errors.Join(writeErr, f.Close()); err != nil {
		return err
	}
	return externalSyncDir(filepath.Dir(path))
}

func externalAtomicFile(path string, data []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".publish-")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	_, err = f.Write(data)
	if err == nil {
		err = rootpublication.SyncStableFile(f)
	}
	if err := errors.Join(err, f.Close()); err != nil {
		return err
	}
	if err := os.Rename(name, path); err != nil {
		return err
	}
	return externalSyncDir(filepath.Dir(path))
}

func externalSyncDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(rootpublication.SyncStableFile(f), f.Close())
}
