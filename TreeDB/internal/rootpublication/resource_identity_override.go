package rootpublication

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type stableIdentityOverride struct {
	identity StableIdentity
	owner    uint64
}

var stableIdentityOverrides struct {
	sync.RWMutex
	nextOwner uint64
	byNative  map[StableIdentity]stableIdentityOverride
}

func stableIdentityFromFile(file *os.File) (StableIdentity, error) {
	if file == nil {
		return StableIdentity{}, os.ErrInvalid
	}
	native, err := platformStableIdentityFromFile(file)
	if err != nil {
		return StableIdentity{}, err
	}
	if identity, ok := testingStableIdentityOverride(native); ok {
		return identity, nil
	}
	return native, nil
}

func testingStableIdentityOverride(native StableIdentity) (StableIdentity, bool) {
	native.Generation = 0
	stableIdentityOverrides.RLock()
	defer stableIdentityOverrides.RUnlock()
	if len(stableIdentityOverrides.byNative) == 0 {
		return StableIdentity{}, false
	}
	if entry, ok := stableIdentityOverrides.byNative[native]; ok {
		return entry.identity, true
	}
	return StableIdentity{}, false
}

// InstallStableIdentityOverridesForTesting installs a scoped physical-identity
// view for deterministic power-loss images. Materializing an oracle image must
// recreate its files, which changes host inode/file IDs even though a real
// crash preserves them. The oracle uses this hook to carry the captured
// physical identities across that test-only materialization boundary.
//
// Production callers must never use this function. It is internal to TreeDB,
// defaults to disabled, rejects overlapping scopes, and does not weaken the
// normal handle-based identity checks.
func InstallStableIdentityOverridesForTesting(overrides map[string]StableIdentity) (func(), error) {
	if len(overrides) == 0 {
		return func() {}, nil
	}
	rebased := make(map[StableIdentity]StableIdentity, len(overrides))
	for path, identity := range overrides {
		if !filepath.IsAbs(path) {
			return nil, fmt.Errorf("stable identity test override path is not absolute: %q", path)
		}
		identity.Generation = 0
		if !identity.valid() {
			return nil, fmt.Errorf("stable identity test override for %q is invalid", path)
		}
		file, err := os.Open(filepath.Clean(path))
		if err != nil {
			return nil, fmt.Errorf("open stable identity test override %q: %w", path, err)
		}
		native, nativeErr := platformStableIdentityFromFile(file)
		closeErr := file.Close()
		if nativeErr != nil {
			return nil, fmt.Errorf("capture native identity for test override %q: %w", path, nativeErr)
		}
		if closeErr != nil {
			return nil, fmt.Errorf("close stable identity test override %q: %w", path, closeErr)
		}
		native.Generation = 0
		if !native.valid() {
			return nil, fmt.Errorf("native stable identity test override for %q is invalid", path)
		}
		if previous, exists := rebased[native]; exists && previous != identity {
			return nil, fmt.Errorf("stable identity test override aliases conflicting native identity for %q", path)
		}
		rebased[native] = identity
	}

	stableIdentityOverrides.Lock()
	defer stableIdentityOverrides.Unlock()
	if stableIdentityOverrides.byNative == nil {
		stableIdentityOverrides.byNative = make(map[StableIdentity]stableIdentityOverride)
	}
	for native := range rebased {
		if _, exists := stableIdentityOverrides.byNative[native]; exists {
			return nil, fmt.Errorf("stable identity test override already installed for native identity %+v", native)
		}
	}
	stableIdentityOverrides.nextOwner++
	owner := stableIdentityOverrides.nextOwner
	for native, identity := range rebased {
		stableIdentityOverrides.byNative[native] = stableIdentityOverride{identity: identity, owner: owner}
	}

	var once sync.Once
	return func() {
		once.Do(func() {
			stableIdentityOverrides.Lock()
			defer stableIdentityOverrides.Unlock()
			for native := range rebased {
				if entry, exists := stableIdentityOverrides.byNative[native]; exists && entry.owner == owner {
					delete(stableIdentityOverrides.byNative, native)
				}
			}
		})
	}, nil
}
