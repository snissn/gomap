package db

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func TestRotateLeafCaptureJoinsInstalledRotationAndCreatedIdentityErrors(t *testing.T) {
	dir := t.TempDir()
	fileID, err := valuelog.EncodeFileID(rewriteLeafLogLaneID, 1)
	if err != nil {
		t.Fatal(err)
	}
	path := valuelog.SegmentPath(dir, fileID)
	writer, err := valuelog.NewStagingWriter(path, fileID)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()

	rewrite := newRewriteWriter("", 0, 0, 0)
	rewrite.ConfigureLeafLog(dir, rewriteLeafLogLaneID, 1)
	rewrite.leafW = writer
	rewrite.leafCurrentPath = path
	rewrite.leafCurrentFileID = fileID

	rotationErr := errors.New("installed rotation ambiguity")
	capture := rewriteLeafRotationCaptureFunc(func(active *valuelog.Writer, nextPath string, nextFileID uint32, syncCurrent bool) (bool, error) {
		if active != writer || filepath.Dir(nextPath) != dir || nextFileID == fileID || !syncCurrent {
			t.Fatalf("unexpected capture arguments writer=%p path=%q file=%d sync=%v", active, nextPath, nextFileID, syncCurrent)
		}
		if err := active.Close(); err != nil {
			t.Fatalf("close installed writer: %v", err)
		}
		return true, errors.Join(rotationErr, ErrRecoveryRequired)
	})

	err = rewrite.rotateLeafCaptureWith(capture)
	if !errors.Is(err, rotationErr) || !errors.Is(err, ErrRecoveryRequired) || !errors.Is(err, rootpublication.ErrUnresolvedResource) {
		t.Fatalf("rotateLeafCapture error=%v want rotation ambiguity, recovery required, and created identity failure", err)
	}
}
