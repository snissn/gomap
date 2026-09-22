package nativewire

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

const peerStorageMarkerV1 = "fixed-peer-storage-v1.json"

type peerStorageIdentityV1 struct {
	Version           uint32
	PairID            string
	LocalConfigSHA256 string
}

// Keep a matching identity on every persistent root before opening stores.
// Losing one volume must not silently turn the retained identity into a new
// bootstrap. Partial first-time creation also refuses until explicitly repaired.
func preparePeerStorageV1(config FixedPeerTCPConfigV1, normalized []byte) error {
	digest := sha256.Sum256(normalized)
	localDigest := hex.EncodeToString(digest[:])
	roots := []string{config.RaftRoot}
	for _, group := range config.Groups {
		if _, hosted := config.RaftListen[group.ID]; hosted {
			roots = append(roots, config.DataRoot)
			break
		}
	}
	old := make([][]byte, len(roots))
	found := 0
	for i, root := range roots {
		file, err := os.Open(filepath.Join(root, peerStorageMarkerV1))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		old[i], err = io.ReadAll(io.LimitReader(file, 4097))
		err = errors.Join(err, file.Close())
		if err != nil {
			return err
		}
		if len(old[i]) > 4096 {
			return fmt.Errorf("%w: oversized persistent root identity", raftcluster.ErrInvalidConfig)
		}
		found++
	}
	if found != 0 {
		if found != len(roots) {
			return fmt.Errorf("%w: missing persistent root identity; restore the paired roots", raftcluster.ErrInvalidConfig)
		}
		var record peerStorageIdentityV1
		decoder := json.NewDecoder(bytes.NewReader(old[0]))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&record); err != nil {
			return err
		}
		var trailing any
		if err := decoder.Decode(&trailing); err != io.EOF {
			return fmt.Errorf("%w: trailing persistent root identity", raftcluster.ErrInvalidConfig)
		}
		pair, err := hex.DecodeString(record.PairID)
		if err != nil || len(pair) != 32 || record.Version != 1 || record.LocalConfigSHA256 != localDigest {
			return fmt.Errorf("%w: persistent root identity mismatch", raftcluster.ErrInvalidConfig)
		}
		for _, raw := range old[1:] {
			if !bytes.Equal(raw, old[0]) {
				return fmt.Errorf("%w: mismatched persistent root pair", raftcluster.ErrInvalidConfig)
			}
		}
		return validatePeerRootSeparationV1(roots)
	}
	// A missing identity never authorizes adoption of nonempty directories.
	for _, root := range roots {
		file, err := os.Open(root)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		names, err := file.Readdirnames(1)
		err = errors.Join(ignorePeerEmptyDirectoryV1(err), file.Close())
		if err != nil {
			return err
		}
		if len(names) != 0 {
			return fmt.Errorf("%w: nonempty root lacks persistent identity", raftcluster.ErrInvalidConfig)
		}
	}
	for _, root := range roots {
		if err := os.MkdirAll(root, 0700); err != nil {
			return err
		}
	}
	if err := validatePeerRootSeparationV1(roots); err != nil {
		return err
	}
	var pair [32]byte
	if _, err := rand.Read(pair[:]); err != nil {
		return err
	}
	raw, err := json.Marshal(peerStorageIdentityV1{Version: 1, PairID: hex.EncodeToString(pair[:]), LocalConfigSHA256: localDigest})
	if err != nil {
		return err
	}
	for _, root := range roots {
		file, err := os.OpenFile(filepath.Join(root, peerStorageMarkerV1), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return err
		}
		_, err = file.Write(raw)
		err = errors.Join(err, file.Sync(), file.Close())
		if err != nil {
			return err
		}
		if err := syncFixedPeerDirectoryV1(root); err != nil {
			return err
		}
	}
	return nil
}

func ignorePeerEmptyDirectoryV1(err error) error {
	if err == io.EOF {
		return nil
	}
	return err
}

func validatePeerRootSeparationV1(roots []string) error {
	resolved := make([]string, len(roots))
	for i, root := range roots {
		var err error
		resolved[i], err = filepath.EvalSymlinks(root)
		if err != nil {
			return err
		}
	}
	if len(resolved) == 2 {
		for _, pair := range [][2]string{{resolved[0], resolved[1]}, {resolved[1], resolved[0]}} {
			rel, err := filepath.Rel(pair[0], pair[1])
			if err != nil || rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))) {
				return fmt.Errorf("%w: persistent roots overlap after resolving links", raftcluster.ErrInvalidConfig)
			}
		}
	}
	return nil
}
