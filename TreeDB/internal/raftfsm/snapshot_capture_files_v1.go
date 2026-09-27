package raftfsm

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/raftapply"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

type snapshotCapturedFileV1 struct {
	name   string
	mode   os.FileMode
	size   int64
	file   *os.File
	index  *backenddb.PhysicalSnapshotCutV1
	prefix *raftapply.DurableSnapshotPrefixV1
}

type snapshotCapturedFilesV1 struct {
	entries        []snapshotCapturedFileV1
	bytes          int64
	limits         SnapshotCaptureLimitsV1
	stage          string
	partialArchive string
}

func (c *snapshotCapturedFilesV1) close() error {
	var err error
	for i := range c.entries {
		e := &c.entries[i]
		if e.file != nil {
			err = errors.Join(err, e.file.Close())
			e.file = nil
		}
		if e.index != nil {
			err = errors.Join(err, e.index.Close())
			e.index = nil
		}
		if e.prefix != nil {
			err = errors.Join(err, e.prefix.Close())
			e.prefix = nil
		}
	}
	for _, target := range []*string{&c.stage, &c.partialArchive} {
		if *target == "" {
			continue
		}
		var cleanupErr error
		if raftSnapshotBeforeCleanupForTest != nil {
			cleanupErr = raftSnapshotBeforeCleanupForTest(*target)
		}
		if cleanupErr == nil {
			if target == &c.stage {
				cleanupErr = os.RemoveAll(*target)
			} else {
				cleanupErr = os.Remove(*target)
			}
		}
		if cleanupErr == nil || errors.Is(cleanupErr, os.ErrNotExist) {
			*target = ""
		} else {
			err = errors.Join(err, cleanupErr)
		}
	}
	return err
}

var raftSnapshotBeforeCleanupForTest func(string) error

// add consumes e only on success. Directory entries count against the same
// admission ceiling so directory discovery cannot grow unbounded state.
func (c *snapshotCapturedFilesV1) add(e snapshotCapturedFileV1) error {
	if len(c.entries) >= c.limits.MaxFiles || e.size < 0 || e.size > c.limits.MaxBytes-c.bytes {
		return fmt.Errorf("raftfsm: snapshot file/byte admission exceeded")
	}
	c.entries = append(c.entries, e)
	c.bytes += e.size
	return nil
}

func (c *snapshotCapturedFilesV1) captureStorage(ctx context.Context, prefix, root string, index *backenddb.PhysicalSnapshotCutV1) error {
	parent, err := rootpublication.OpenStableParent(root)
	if err != nil {
		return err
	}
	defer parent.Close()
	if err := index.ValidateStorageDirectoryV1(parent); err != nil {
		return err
	}
	rootInfo, err := parent.Stat()
	if err != nil {
		return err
	}
	if err := c.add(snapshotCapturedFileV1{name: prefix, mode: os.ModeDir | 0700}); err != nil {
		return err
	}
	if err := c.add(snapshotCapturedFileV1{name: prefix + "/index.db", mode: 0600, size: index.SizeBytes(), index: index}); err != nil {
		return err
	}
	for _, name := range raftSnapshotMainDBEntriesV1 {
		if name == "index.db" {
			continue
		}
		path := filepath.Join(root, name)
		info, err := os.Lstat(path)
		if errors.Is(err, os.ErrNotExist) {
			// The archive contract represents an absent lifecycle namespace as
			// an explicit empty directory, exactly as synchronous export did.
			if name == "vector_partitions" {
				if err := c.add(snapshotCapturedFileV1{name: prefix + "/" + name, mode: os.ModeDir | 0700}); err != nil {
					return err
				}
			}
			continue
		}
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("raftfsm: snapshot symlink %q", path)
		}
		if info.IsDir() {
			dir, err := rootpublication.OpenStableChildFile(parent, name, os.O_RDONLY, 0)
			if err != nil {
				return err
			}
			got, statErr := dir.Stat()
			if statErr != nil || !os.SameFile(info, got) {
				_ = dir.Close()
				return fmt.Errorf("raftfsm: snapshot directory changed %q: %v", path, statErr)
			}
			if name == "vector_partitions" {
				err = c.captureLifecycle(ctx, prefix+"/"+name, dir)
			} else {
				err = c.captureDirectory(ctx, prefix+"/"+name, dir, 0)
			}
			err = errors.Join(err, dir.Close())
			if err != nil {
				return err
			}
		} else {
			before := len(c.entries)
			if err := c.captureChild(ctx, prefix+"/"+name, parent, name, 0); err != nil {
				return err
			}
			got, err := c.entries[before].file.Stat()
			if err != nil || !os.SameFile(info, got) {
				return fmt.Errorf("raftfsm: snapshot file changed %q: %v", path, err)
			}

		}
	}
	current, err := os.Lstat(root)
	if err != nil || !os.SameFile(rootInfo, current) {
		return fmt.Errorf("raftfsm: snapshot root changed %q: %v", root, err)
	}
	return index.ValidateStorageDirectoryV1(parent)
}

func (c *snapshotCapturedFilesV1) captureDirectory(ctx context.Context, prefix string, dir *os.File, depth int) error {
	if depth > 16 {
		return fmt.Errorf("raftfsm: snapshot directory depth exceeded")
	}
	if err := c.add(snapshotCapturedFileV1{name: prefix, mode: os.ModeDir | 0700}); err != nil {
		return err
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		entries, err := dir.ReadDir(64)
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		for _, entry := range entries {
			if shouldSkipRaftSnapshotTreeDBFileV1(entry.Name()) {
				continue
			}
			if entry.Type()&os.ModeSymlink != 0 {
				return fmt.Errorf("raftfsm: snapshot symlink %q", entry.Name())
			}
			if err := c.captureChild(ctx, prefix+"/"+entry.Name(), dir, entry.Name(), depth+1); err != nil {
				return err
			}
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
	}
}

func (c *snapshotCapturedFilesV1) captureChild(ctx context.Context, name string, parent *os.File, base string, depth int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(c.entries) >= c.limits.MaxFiles {
		return fmt.Errorf("raftfsm: snapshot file admission exceeded")
	}
	if raftSnapshotBeforeOpenForTest != nil {
		raftSnapshotBeforeOpenForTest(filepath.Join(parent.Name(), base))
	}
	file, err := rootpublication.OpenStableChildFile(parent, base, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	owned := false
	defer func() {
		if !owned {
			_ = file.Close()
		}
	}()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if err := rootpublication.ValidateStableChildLink(parent, file, base); err != nil {
		return err
	}
	if info.IsDir() {
		return c.captureDirectory(ctx, name, file, depth)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("raftfsm: nonregular snapshot entry %q", name)
	}
	if err := c.add(snapshotCapturedFileV1{name: name, size: info.Size(), mode: info.Mode().Perm(), file: file}); err != nil {
		return err
	}
	owned = true
	return nil
}

func (c *snapshotCapturedFilesV1) captureLifecycle(ctx context.Context, prefix string, dir *os.File) error {
	if err := c.add(snapshotCapturedFileV1{name: prefix, mode: os.ModeDir | 0700}); err != nil {
		return err
	}
	if raftSnapshotBeforeVectorPartitionRootOpenForTest != nil {
		raftSnapshotBeforeVectorPartitionRootOpenForTest(dir.Name())
	}
	entries, err := collections.VectorPartitionSnapshotEntriesWithContextV1(ctx, dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		before := len(c.entries)
		if err := c.captureChild(ctx, prefix+"/"+entry.Name, dir, entry.Name, 0); err != nil {
			return err
		}
		e := c.entries[before]
		if e.file == nil || e.size < 0 || uint64(e.size) != entry.Bytes {
			return fmt.Errorf("raftfsm: lifecycle snapshot length changed")
		}
		identity, err := rootpublication.StableIdentityFromFile(e.file)
		if err != nil {
			return err
		}
		if !rootpublication.SamePhysicalIdentity(identity, entry.Identity) {
			return fmt.Errorf("raftfsm: lifecycle snapshot identity changed")
		}
	}
	return nil
}

func (e snapshotCapturedFileV1) writeTo(ctx context.Context, dst io.Writer) error {
	if raftSnapshotBeforeCopyForTest != nil {
		raftSnapshotBeforeCopyForTest()
	}
	if e.index != nil {
		return e.index.WriteToContext(ctx, dst)
	}
	if e.prefix != nil {
		return e.prefix.WriteToContext(ctx, dst)
	}
	if e.file == nil {
		return fmt.Errorf("raftfsm: snapshot source closed")
	}
	var buffer [64 * 1024]byte
	for offset := int64(0); offset < e.size; {
		if err := ctx.Err(); err != nil {
			return err
		}
		n := min(int64(len(buffer)), e.size-offset)
		if _, err := e.file.ReadAt(buffer[:n], offset); err != nil {
			return err
		}
		written, err := dst.Write(buffer[:n])
		if err != nil {
			return err
		}
		if int64(written) != n {
			return io.ErrShortWrite
		}
		offset += n
	}
	return ctx.Err()
}
