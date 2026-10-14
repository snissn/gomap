package rootpublication

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"sync"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
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

// StableChildFileInstallProbe retains the original probe operands after a
// cleanup refusal. Selected callers embed it in their already admitted
// containing owner; it borrows the exact parent and never closes that parent.
type StableChildFileInstallProbe struct {
	once                        sync.Once
	parent                      *os.File
	source                      *os.File
	sourceName, destinationName string
	metadata                    *retainedalloc.Owner
	nameCharge, fileCharge      uint64
	resultErr, cleanupErr       error
	failure, cleanupFailure     stableInstallProbeFailure
}

// Both receipts are embedded in the original admitted operand. Their immutable
// constituent arrays avoid allocating retained Join backing after an effect.
type stableInstallProbeFailure struct{ causes [4]error }

func (*stableInstallProbeFailure) Error() string {
	return "stable install probe original cleanup failed"
}
func (failure *stableInstallProbeFailure) Unwrap() []error { return failure.causes[:] }

// Run admits the two exact names and original child wrapper before creating
// the transient child. The containing probe must already be admitted.
func (probe *StableChildFileInstallProbe) Run(parent *os.File, metadata *retainedalloc.Owner) error {
	return probe.run(parent, metadata, MoveStableChildFileNoReplace, RemoveStableChildFile, func(file *os.File) error { return file.Close() })
}

func (probe *StableChildFileInstallProbe) run(parent *os.File, metadata *retainedalloc.Owner, move stableChildFileInstallProbeMove, remove func(*os.File, string) error, closeFile func(*os.File) error) error {
	if probe == nil || metadata == nil || move == nil || remove == nil || closeFile == nil {
		return ErrUnresolvedResource
	}
	probe.once.Do(func() {
		if parent == nil {
			probe.resultErr = ErrUnresolvedResource
			return
		}
		var nonce [16]byte
		if _, err := rand.Read(nonce[:]); err != nil {
			probe.resultErr = err
			return
		}
		const prefix = StableChildFileInstallProbePrefix
		const sourceSize = len(prefix) + 32 + len("-source")
		const destinationSize = len(prefix) + 32 + len("-installed")
		nameCharge := retainedalloc.AllocationCharge(uint64(sourceSize)) + retainedalloc.AllocationCharge(uint64(destinationSize))
		fileCharge, err := StableFileMetadataCharge(uint64(sourceSize))
		if err != nil {
			probe.resultErr = err
			return
		}
		if err := metadata.AddPending(nameCharge + fileCharge); err != nil {
			probe.resultErr = err
			return
		}
		probe.metadata, probe.nameCharge, probe.fileCharge = metadata, nameCharge, fileCharge
		var storage [destinationSize]byte
		copy(storage[:], prefix)
		hex.Encode(storage[len(prefix):len(prefix)+32], nonce[:])
		copy(storage[len(prefix)+32:], "-source")
		probe.sourceName = string(storage[:sourceSize])
		copy(storage[len(prefix)+32:], "-installed")
		probe.destinationName = string(storage[:])
		probe.parent = parent
		probe.execute(move, remove, closeFile)
	})
	return probe.resultErr
}

// CleanupError distinguishes a refused physical cleanup from a failed probe
// whose original operands were successfully consumed. Run must have returned.
func (probe *StableChildFileInstallProbe) CleanupError() error {
	if probe == nil {
		return nil
	}
	return probe.cleanupErr
}

func probeStableChildFileNoReplaceInstall(
	parent *os.File,
	sourceName, destinationName string,
	move stableChildFileInstallProbeMove,
	remove func(*os.File, string) error,
) error {
	probe := StableChildFileInstallProbe{parent: parent, sourceName: sourceName, destinationName: destinationName}
	probe.execute(move, remove, func(file *os.File) error { return file.Close() })
	return probe.resultErr
}

// execute is shared by the compatibility probe and admitted original operand.
// A hard cleanup refusal is terminal; successful siblings clear independently.
func (probe *StableChildFileInstallProbe) execute(
	move stableChildFileInstallProbeMove,
	remove func(*os.File, string) error,
	closeFile func(*os.File) error,
) {
	if probe.parent == nil || !stableChildBaseName(probe.sourceName) || !stableChildBaseName(probe.destinationName) || probe.sourceName == probe.destinationName || move == nil || remove == nil || closeFile == nil {
		probe.resultErr = fmt.Errorf("%w: invalid stable install probe", ErrUnresolvedResource)
		probe.refundIfComplete()
		return
	}
	source, err := OpenStableChildFile(probe.parent, probe.sourceName, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		probe.resultErr = err
		probe.refundIfComplete()
		return
	}
	probe.source = source
	installed, operationErr := move(probe.parent, source, probe.sourceName, probe.parent, probe.destinationName)
	if operationErr == nil && !installed {
		operationErr = fmt.Errorf("%w: stable install probe reported no mutation", ErrNamespaceUnstable)
	}
	var cleanup [3]error
	if installed {
		if err := remove(probe.parent, probe.destinationName); err != nil {
			cleanup[0] = err
		}
	}
	if err := remove(probe.parent, probe.sourceName); err != nil {
		cleanup[1] = err
	}
	if err := closeFile(source); err != nil {
		cleanup[2] = err
	} else {
		probe.source = nil
		if probe.metadata != nil {
			probe.metadata.RemovePending(probe.fileCharge)
			probe.fileCharge = 0
		}
	}
	for i, err := range cleanup {
		probe.cleanupFailure.causes[i] = err
		if err != nil {
			probe.cleanupErr = &probe.cleanupFailure
		}
	}
	probe.resultErr = operationErr
	if probe.cleanupErr != nil {
		if operationErr == nil {
			probe.resultErr = probe.cleanupErr
		} else {
			probe.failure.causes[0], probe.failure.causes[1] = operationErr, probe.cleanupErr
			probe.resultErr = &probe.failure
		}
	}
	probe.refundIfComplete()
}

func (probe *StableChildFileInstallProbe) refundIfComplete() {
	if probe.cleanupErr != nil {
		return
	}
	owner, charge := probe.metadata, probe.nameCharge+probe.fileCharge
	probe.source, probe.parent = nil, nil
	probe.sourceName, probe.destinationName = "", ""
	probe.metadata, probe.nameCharge, probe.fileCharge = nil, 0, 0
	if owner != nil {
		owner.RemovePending(charge)
	}
}
