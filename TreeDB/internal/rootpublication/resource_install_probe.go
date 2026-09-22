package rootpublication

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
)

// StableChildFileInstallProbePrefix reserves harmless hidden names used to
// test exact retained-handle installation on a target filesystem. These names
// are never valid TreeDB value-log segment names.
const StableChildFileInstallProbePrefix = ".treedb-install-probe-"

type stableChildFileInstallProbeMove func(*os.File, *os.File, string, *os.File, string) (bool, error)

// ProbeStableChildFileNoReplaceInstall exercises the real exact-handle,
// no-replace install primitive in parent. It creates no stable authority and
// deliberately does not fsync the transient namespace mutations; callers may
// remove crash-left probe names during ordinary startup orphan cleanup.
func ProbeStableChildFileNoReplaceInstall(parent *os.File) error {
	if parent == nil {
		return fmt.Errorf("%w: stable install probe requires an exact target parent", ErrUnresolvedResource)
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return fmt.Errorf("stable install probe random name: %w", err)
	}
	stem := StableChildFileInstallProbePrefix + hex.EncodeToString(nonce[:])
	return probeStableChildFileNoReplaceInstall(
		parent,
		stem+"-source",
		stem+"-installed",
		MoveStableChildFileNoReplace,
		RemoveStableChildFile,
	)
}

func probeStableChildFileNoReplaceInstall(
	parent *os.File,
	sourceName, destinationName string,
	move stableChildFileInstallProbeMove,
	remove func(*os.File, string) error,
) (resultErr error) {
	if parent == nil || !stableChildBaseName(sourceName) || !stableChildBaseName(destinationName) || sourceName == destinationName || move == nil || remove == nil {
		return fmt.Errorf("%w: invalid stable install probe", ErrUnresolvedResource)
	}
	source, err := OpenStableChildFile(parent, sourceName, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	installed := false
	defer func() {
		var cleanupErrs []error
		if installed {
			if err := remove(parent, destinationName); err != nil {
				cleanupErrs = append(cleanupErrs, fmt.Errorf("remove installed probe alias: %w", err))
			}
		}
		if err := remove(parent, sourceName); err != nil {
			cleanupErrs = append(cleanupErrs, fmt.Errorf("remove probe source: %w", err))
		}
		if err := source.Close(); err != nil {
			cleanupErrs = append(cleanupErrs, fmt.Errorf("close probe source: %w", err))
		}
		resultErr = errors.Join(resultErr, errors.Join(cleanupErrs...))
	}()

	installed, resultErr = move(parent, source, sourceName, parent, destinationName)
	if resultErr != nil {
		return resultErr
	}
	if !installed {
		return fmt.Errorf("%w: stable install probe reported no mutation", ErrNamespaceUnstable)
	}
	return nil
}
