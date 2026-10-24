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
	"strings"
	"sync"
	"unicode/utf8"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/atomicfile"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/lockfile"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// ErrExternalRecovery identifies a refused external-recovery contract. This
// opt-in API deliberately does not change the meaning of WriteSync.
var ErrExternalRecovery = errors.New("treedb: external recovery contract")

const externalRecoveryMarker = "external-recovery.json"

// ExternalRecoveryConfig names the authoritative recovery owner. The caller
// must durably retain committed commands/decisions until a certified checkpoint
// covers them. Limits apply to each retained physical closure (conservatively
// counting index high-water), not to upstream log debt or total process memory.
type ExternalRecoveryConfig struct {
	Identity           string
	MaxCheckpointBytes int64
	MaxCheckpointFiles int
}

// ExternalRecoveryCheckpoint certifies one retained physical root together
// with opaque application progress. Metadata must be sampled at the same
// application barrier as the data. Digest binds the bounded root descriptor;
// Files describes that descriptor, not a portable copy of the database.
type ExternalRecoveryCheckpoint struct {
	Version            int                      `json:"version"`
	Identity           string                   `json:"identity"`
	Generation         uint64                   `json:"generation"`
	Root               backenddb.StateToken     `json:"root"`
	Metadata           []byte                   `json:"metadata"`
	Files              []externalCheckpointFile `json:"files"`
	Digest             string                   `json:"digest"`
	PreviousGeneration uint64                   `json:"previous_generation,omitempty"`
	PreviousDigest     string                   `json:"previous_digest,omitempty"`
	DataDirectory      string                   `json:"data_directory"`
}

type externalCheckpointFile struct {
	Path   string `json:"path"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}

type externalRetainedCut struct {
	cut           *backenddb.PhysicalSnapshotCutV1
	dataDirectory string
	generation    uint64
}

// ExternalRecovery owns an opt-in, externally logged materialization. Apply
// calls may overlap; Checkpoint and Close exclude them. DB must only be mutated
// inside Apply, and no asynchronous mutation may outlive its callback.
//
// Checkpoints retain the maintained DATA publisher's exact physical cut and
// persist bounded meta operands. No full image copy or second publisher exists.
type ExternalRecovery struct {
	// Transfer exclusion is separate from Apply exclusion: a queued checkpoint
	// waits here without triggering RWMutex writer preference against Apply.
	transferMu    sync.RWMutex
	mu            sync.RWMutex
	db            *DB
	dir           string
	config        ExternalRecoveryConfig
	lock          *lockfile.Lock
	checkpoint    ExternalRecoveryCheckpoint
	closed        bool
	poison        error
	retained      []externalRetainedCut
	options       Options
	dataDirectory string
	// Per-owner fault injection never changes another database's behavior.
	testCut func(string) error
}

// OpenExternalRecovery opens a fresh or previously owned DATA directory. It
// restores only the certified current/previous meta operands before TreeDB can
// reclaim or reuse any resources. Uncertified suffix work must be replayed.
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
	if err := validateCOWOpenOptions(&opts); err != nil {
		return nil, err
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
	}{2, config.Identity})
	if len(markerBytes) > 8192 {
		return nil, fmt.Errorf("%w: encoded owner identity exceeds marker limit", ErrExternalRecovery)
	}
	dir, err := resolveOpenOwnershipDirectory(opts.Dir)
	if err != nil {
		return nil, err
	}
	ownershipDir := openOwnershipRoot(dir)
	if err := refuseExternalRecoveryEnclosingLayout(dir, ownershipDir); err != nil {
		return nil, err
	}
	if err := backenddb.EnsureStorageLayoutDirsForOpen(0700, filepath.Join(ownershipDir, "INIT.LOCK"), ownershipDir); err != nil {
		return nil, err
	}
	lock, err := lockfile.Acquire(filepath.Join(ownershipDir, "INIT.LOCK"))
	if err != nil {
		return nil, err
	}
	o := &ExternalRecovery{dir: dir, config: config, lock: lock}
	defer func() {
		if err != nil {
			_ = lock.Close()
		}
	}()
	if err := refuseExternalRecoveryEnclosingLayout(dir, ownershipDir); err != nil {
		return nil, err
	}
	if ownershipDir != dir {
		if err := backenddb.EnsureStorageLayoutDirsForOpen(0700, filepath.Join(dir, externalRecoveryMarker), dir); err != nil {
			return nil, err
		}
	}
	markerPath := filepath.Join(dir, externalRecoveryMarker)
	marker, readErr := readExternalBounded(markerPath, 8192)
	if errors.Is(readErr, os.ErrNotExist) {
		entries, e := os.ReadDir(dir)
		if e != nil {
			return nil, e
		}
		for _, entry := range entries {
			if entry.Name() != "INIT.LOCK" {
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
		if json.Unmarshal(marker, &m) != nil || m.Version != 2 || m.Identity != config.Identity {
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
	o.dataDirectory = "working"
	if ok {
		o.dataDirectory = cert.DataDirectory
	}
	working, err := o.dataPath(o.dataDirectory)
	if err != nil {
		return nil, err
	}
	var previous *backenddb.RetainedPhysicalRootV1
	if ok {
		if err := o.verifyImage(cert); err != nil {
			return nil, err
		}
		current, err := o.readRetainedRoot(cert)
		if err != nil {
			return nil, err
		}
		if cert.PreviousGeneration != 0 {
			prior, err := o.readCertificate(cert.PreviousGeneration, cert.PreviousDigest)
			if err != nil {
				return nil, err
			}
			if err := o.verifyImage(prior); err != nil {
				return nil, err
			}
			root, err := o.readRetainedRoot(prior)
			if err != nil {
				return nil, err
			}
			if prior.DataDirectory == cert.DataDirectory {
				previous = &root
			} else {
				priorDir, err := o.dataPath(prior.DataDirectory)
				if err != nil {
					return nil, err
				}
				if err := backenddb.RestoreRetainedPhysicalRootsV1(priorDir, root, nil); err != nil {
					return nil, err
				}
			}
		}
		if err := backenddb.RestoreRetainedPhysicalRootsV1(working, current, previous); err != nil {
			return nil, err
		}
		o.checkpoint = cert
	} else {
		if err := o.validateUncertifiedGenesis(); err != nil {
			return nil, err
		}
		// Without a certificate genesis has no retained user state.
		if err := externalRemoveAll(working); err != nil {
			return nil, err
		}
	}
	if err := backenddb.EnsureStorageLayoutDirs(0700, working); err != nil {
		return nil, err
	}
	// All ordinary public entry points refuse this internal physical namespace.
	if err := externalAtomicFile(filepath.Join(working, externalRecoveryMarker), markerBytes); err != nil {
		return nil, err
	}
	opts.Dir, opts.DisableSideStores = working, true
	opts.BackgroundCheckpointInterval, opts.BackgroundCheckpointIdleDuration = -1, -1
	opts.BackgroundIndexVacuumInterval, opts.DisableBackgroundPrune = -1, true
	o.options = opts
	o.db, err = openResolved(opts)
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
	if ok {
		// Normal Open has validated/pinned both restored slots. Capture its maintained
		// union before returning any mutation authority to the external caller.
		cut, err := o.db.backend.CapturePhysicalSnapshotCutV1(context.Background())
		if err != nil {
			_ = o.db.Close()
			return nil, err
		}
		o.retained = append(o.retained, externalRetainedCut{cut: cut, dataDirectory: o.dataDirectory, generation: cert.Generation})
	}
	if err := o.cleanupImages(); err != nil {
		_ = o.db.Close()
		for _, cut := range o.retained {
			_ = cut.cut.Close()
		}
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
	return o.publishCut(ctx, cut, metadata, o.dataDirectory)
}

// publishCut consumes the cut even on failure. Caller holds exclusive o.mu.
func (o *ExternalRecovery) publishCut(ctx context.Context, cut *backenddb.PhysicalSnapshotCutV1, metadata []byte, dataDirectory string) (ExternalRecoveryCheckpoint, error) {
	keepCut := false
	defer func() {
		if !keepCut {
			_ = cut.Close()
		}
	}()
	gen := o.checkpoint.Generation + 1
	if gen == 0 {
		return ExternalRecoveryCheckpoint{}, ErrExternalRecovery
	}
	stage, err := externalMkdirTemp(filepath.Join(o.dir, "checkpoints"), ".staging-")
	if err != nil {
		return ExternalRecoveryCheckpoint{}, err
	}
	defer externalRemoveAll(stage)
	image := filepath.Join(stage, "db")
	if err := backenddb.EnsureStorageLayoutDirs(0700, image); err != nil {
		return ExternalRecoveryCheckpoint{}, err
	}
	root, err := cut.RetainedRootV1()
	if err != nil {
		return ExternalRecoveryCheckpoint{}, err
	}
	if root.ResourceBytes > uint64(o.config.MaxCheckpointBytes) || root.ResourceFiles > uint64(o.config.MaxCheckpointFiles) {
		return ExternalRecoveryCheckpoint{}, ErrExternalRecovery
	}
	rootBytes, err := json.Marshal(root)
	if err != nil {
		return ExternalRecoveryCheckpoint{}, err
	}
	if int64(len(rootBytes)) > o.config.MaxCheckpointBytes || o.config.MaxCheckpointFiles < 1 {
		return ExternalRecoveryCheckpoint{}, ErrExternalRecovery
	}
	if err := externalAtomicFile(filepath.Join(image, "root.json"), rootBytes); err != nil {
		return ExternalRecoveryCheckpoint{}, err
	}
	cert := ExternalRecoveryCheckpoint{Version: 2, Identity: o.config.Identity, Generation: gen, Root: externalPersistentRoot(cut.StateToken()), Metadata: append([]byte(nil), metadata...), PreviousGeneration: o.checkpoint.Generation, PreviousDigest: o.checkpoint.Digest, DataDirectory: dataDirectory}
	sum := sha256.Sum256(rootBytes)
	cert.Files = []externalCheckpointFile{{Path: "root.json", Bytes: int64(len(rootBytes)), SHA256: hex.EncodeToString(sum[:])}}
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
	if err := externalRemoveAll(final); err != nil {
		return ExternalRecoveryCheckpoint{}, err
	}
	if err := externalRename(stage, final); err != nil {
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
	// Publication failure may have installed CURRENT. Retain the candidate until
	// Close as well; the poisoned owner cannot accept another mutation/checkpoint.
	o.retained = append(o.retained, externalRetainedCut{cut: cut, dataDirectory: dataDirectory, generation: gen})
	keepCut = true
	if err := externalAtomicFile(filepath.Join(o.dir, "CURRENT"), pointer); err != nil {
		o.poison = fmt.Errorf("%w: ambiguous checkpoint publication: %v", ErrExternalRecovery, err)
		return ExternalRecoveryCheckpoint{}, o.poison
	}
	o.checkpoint = cert
	// Keep the previous certificate's original cut until CURRENT is stable.
	// A reopened cut already contains the restored current+previous slot union.
	if len(o.retained) > 2 {
		if err := o.retained[0].cut.Close(); err != nil {
			o.poison = err
			return ExternalRecoveryCheckpoint{}, err
		}
		copy(o.retained, o.retained[1:])
		o.retained[len(o.retained)-1] = externalRetainedCut{}
		o.retained = o.retained[:len(o.retained)-1]
	}
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
			if err := externalRemoveAll(filepath.Join(root, name)); err != nil {
				return err
			}
		}
	}
	return errors.Join(externalSyncDir(root), o.cleanupDataDirectories())
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
	o.transferMu.Lock()
	defer o.transferMu.Unlock()
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return nil
	}
	o.closed = true
	err := o.db.Close()
	for _, cut := range o.retained {
		err = errors.Join(err, cut.cut.Close())
	}
	o.retained = nil
	return errors.Join(err, o.lock.Close())
}

func (o *ExternalRecovery) imageDir(gen uint64) string {
	return filepath.Join(o.dir, "checkpoints", fmt.Sprintf("%020d", gen), "db")
}

// A lone first generation may be an interrupted initial publication. Later
// generations require an earlier stable CURRENT and cannot be reset to genesis
// when that authority is missing. Validate before any namespace cleanup.
func (o *ExternalRecovery) validateUncertifiedGenesis() error {
	entries, err := os.ReadDir(filepath.Join(o.dir, "checkpoints"))
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".staging-") && entry.IsDir() {
			continue
		}
		if entry.Name() != fmt.Sprintf("%020d", 1) || !entry.IsDir() {
			return fmt.Errorf("%w: checkpoint %q exists without CURRENT", ErrExternalRecovery, entry.Name())
		}
		raw, err := readExternalBounded(filepath.Join(o.dir, "checkpoints", entry.Name(), "certificate.json"), 4<<20)
		if err != nil {
			return err
		}
		var cert ExternalRecoveryCheckpoint
		if json.Unmarshal(raw, &cert) != nil || cert.Version != 2 || cert.Identity != o.config.Identity || cert.Generation != 1 || cert.PreviousGeneration != 0 || cert.PreviousDigest != "" {
			return fmt.Errorf("%w: invalid initial checkpoint without CURRENT", ErrExternalRecovery)
		}
		digest, err := externalDigest(cert)
		if err != nil || digest != cert.Digest {
			return fmt.Errorf("%w: corrupt initial checkpoint without CURRENT", ErrExternalRecovery)
		}
	}
	return nil
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
	cert, err = o.readCertificate(pointer.Generation, pointer.Digest)
	return cert, err == nil, err
}

func (o *ExternalRecovery) readCertificate(generation uint64, expectedDigest string) (ExternalRecoveryCheckpoint, error) {
	var cert ExternalRecoveryCheckpoint
	raw, err := readExternalBounded(filepath.Join(filepath.Dir(o.imageDir(generation)), "certificate.json"), 4<<20)
	if err != nil {
		return cert, err
	}
	if err := json.Unmarshal(raw, &cert); err != nil {
		return cert, err
	}
	digest, err := externalDigest(cert)
	if err != nil || cert.Version != 2 || cert.Identity != o.config.Identity || cert.Generation != generation || digest != cert.Digest || digest != expectedDigest || len(cert.Metadata) > 1<<20 || (cert.PreviousGeneration != 0 && cert.PreviousGeneration >= cert.Generation) {
		return cert, fmt.Errorf("%w: certificate identity/digest mismatch", ErrExternalRecovery)
	}
	if _, err := o.dataPath(cert.DataDirectory); err != nil {
		return cert, err
	}
	return cert, nil
}

func (o *ExternalRecovery) verifyImage(cert ExternalRecoveryCheckpoint) error {
	if len(cert.Files) != 1 || cert.Files[0].Path != "root.json" || cert.Files[0].Bytes < 1 || cert.Files[0].Bytes > 64<<10 || cert.Files[0].Bytes > o.config.MaxCheckpointBytes || o.config.MaxCheckpointFiles < 1 {
		return ErrExternalRecovery
	}
	raw, err := readExternalBounded(filepath.Join(o.imageDir(cert.Generation), "root.json"), 64<<10)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(raw)
	if int64(len(raw)) != cert.Files[0].Bytes || hex.EncodeToString(sum[:]) != cert.Files[0].SHA256 {
		return fmt.Errorf("%w: retained root digest mismatch", ErrExternalRecovery)
	}
	return nil
}

func (o *ExternalRecovery) readRetainedRoot(cert ExternalRecoveryCheckpoint) (backenddb.RetainedPhysicalRootV1, error) {
	var root backenddb.RetainedPhysicalRootV1
	raw, err := readExternalBounded(filepath.Join(o.imageDir(cert.Generation), "root.json"), 64<<10)
	if err != nil {
		return root, err
	}
	if err := json.Unmarshal(raw, &root); err != nil {
		return root, err
	}
	if root.State != cert.Root {
		return root, ErrExternalRecovery
	}
	if root.ResourceBytes > uint64(o.config.MaxCheckpointBytes) || root.ResourceFiles > uint64(o.config.MaxCheckpointFiles) {
		return root, ErrExternalRecovery
	}
	return root, nil
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

func externalAtomicFile(path string, data []byte) error {
	return atomicfile.WriteDurable(path, data, 0600)
}

func externalSyncDir(path string) error {
	return atomicfile.SyncDirectory(path)
}

func externalSyncFile(file *os.File, resource durabilitycut.Resource) error {
	path := file.Name()
	if err := durabilitycut.EmitStablePath(durabilitycut.BeforeDependencyFileSync, resource, filepath.Dir(path), path, file); err != nil {
		return err
	}
	if err := rootpublication.SyncStableFile(file); err != nil {
		return err
	}
	return durabilitycut.EmitStablePath(durabilitycut.AfterDependencyFileSync, resource, filepath.Dir(path), path, file)
}

func externalMkdirTemp(dir, pattern string) (string, error) {
	path, err := os.MkdirTemp(dir, pattern)
	if err != nil {
		return "", err
	}
	if err := durabilitycut.EmitNamespace(durabilitycut.NamespaceCreate, durabilitycut.ResourceAuxiliary, dir, "", path); err != nil {
		return "", errors.Join(err, externalRemoveAll(path))
	}
	return path, nil
}

func externalRename(oldPath, newPath string) error {
	if err := os.Rename(oldPath, newPath); err != nil {
		return err
	}
	return durabilitycut.EmitNamespace(durabilitycut.NamespaceRename, durabilitycut.ResourceAuxiliary, filepath.Dir(newPath), oldPath, newPath)
}

func externalRemoveAll(path string) error {
	_, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := os.RemoveAll(path); err != nil {
		return err
	}
	return durabilitycut.EmitNamespace(durabilitycut.NamespaceUnlink, durabilitycut.ResourceAuxiliary, filepath.Dir(path), path, "")
}

func (o *ExternalRecovery) dataPath(name string) (string, error) {
	if name != "working" && (!strings.HasPrefix(name, "materialized-") || len(name) > 100) {
		return "", ErrExternalRecovery
	}
	if filepath.Base(name) != name || name == "." || strings.ContainsAny(name, "/\\") {
		return "", ErrExternalRecovery
	}
	return filepath.Join(o.dir, name), nil
}

func (o *ExternalRecovery) cleanupDataDirectories() error {
	keep := map[string]bool{o.dataDirectory: true}
	if o.checkpoint.Generation != 0 {
		keep[o.checkpoint.DataDirectory] = true
	}
	if o.checkpoint.PreviousGeneration != 0 {
		prior, err := o.readCertificate(o.checkpoint.PreviousGeneration, o.checkpoint.PreviousDigest)
		if err != nil {
			return err
		}
		keep[prior.DataDirectory] = true
	}
	for _, retained := range o.retained {
		keep[retained.dataDirectory] = true
	}
	entries, err := os.ReadDir(o.dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		if keep[name] || !(name == "working" || strings.HasPrefix(name, "materialized-") || strings.HasPrefix(name, ".install-")) {
			continue
		}
		if !entry.IsDir() || entry.Type()&os.ModeSymlink != 0 {
			return ErrExternalRecovery
		}
		if err := externalRemoveAll(filepath.Join(o.dir, name)); err != nil {
			return err
		}
	}
	return externalSyncDir(o.dir)
}
