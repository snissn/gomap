//go:build windows

package rootpublication_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func TestStableProducerRotationRetryResourcePlateau(t *testing.T) {
	const iterations = 320
	root := t.TempDir()
	valueDir := filepath.Join(root, "value_vlog")
	if err := os.Mkdir(valueDir, 0o700); err != nil {
		t.Fatal(err)
	}
	valuePath := filepath.Join(valueDir, "000001.vlog")
	valueSuccessor := filepath.Join(valueDir, "000002.vlog")
	valueWriter, err := valuelog.NewWriter(valuePath, 1)
	if err != nil {
		t.Fatal(err)
	}
	commandJournal, err := commitlog.OpenCommandJournal(filepath.Join(root, "wal"), commitlog.CommandJournalOptions{Lane: 5})
	if err != nil {
		_ = valueWriter.Close()
		t.Fatal(err)
	}
	commandPath, _ := commandJournal.ActiveSegmentSnapshot()
	commandSuccessor := filepath.Join(filepath.Dir(commandPath), commitlog.CommandSegmentName(5, 2))
	commandEnvelope := commitlog.CommandEnvelope{
		Kind: commitlog.CommandKindRawKVBatch, Scope: commitlog.CommandScopeRawKV, PayloadFormat: commitlog.PayloadFormatRawKVBatchV1,
	}

	for i := 0; i < iterations; i++ {
		seq := uint64(i + 1)
		if i%2 == 0 {
			if _, err := valueWriter.Append(0, nil, seq, []byte("unsupported-rotation-stress")); err != nil {
				t.Fatal(err)
			}
			rotation, err := valueWriter.RotateToWithStableResources(valueSuccessor, 2, false,
				valuelog.StableResourceRegistration{
					LogicalLane: "main", Generation: 1, DiagnosticPath: "maindb/value_vlog/000001.vlog",
					Reachability: rootpublication.ReachabilityValueLogPointer,
				},
				valuelog.StableResourceRegistration{
					LogicalLane: "main", Generation: 2, DiagnosticPath: "maindb/value_vlog/000002.vlog",
					Reachability: rootpublication.ReachabilityValueLogPointer, ParentGeneration: 2,
					NamespaceOperation: rootpublication.NamespaceCreate,
				})
			if !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) || rotation != nil {
				if rotation != nil {
					rotation.Release()
				}
				t.Fatalf("value-log rotation %d resources=%v error=%v", seq, rotation, err)
			}
			if valueWriter.FileID() != 1 {
				t.Fatalf("value-log rotation %d changed active file id=%d", seq, valueWriter.FileID())
			}
		} else {
			if _, err := commandJournal.AppendCommand(commandEnvelope); err != nil {
				t.Fatal(err)
			}
			rotation, err := commandJournal.RotateActiveSegmentWithStableResources(false)
			if !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) || rotation != nil {
				if rotation != nil {
					rotation.Release()
				}
				t.Fatalf("command-WAL rotation %d resources=%v error=%v", seq, rotation, err)
			}
			activePath, _ := commandJournal.ActiveSegmentSnapshot()
			if activePath != commandPath {
				t.Fatalf("command-WAL rotation %d changed active path=%q", seq, activePath)
			}
		}
		for _, successor := range []string{valueSuccessor, commandSuccessor} {
			if _, err := os.Stat(successor); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("unsupported rotation %d exposed successor %q: %v", seq, successor, err)
			}
		}
	}

	if err := valueWriter.Close(); err != nil {
		t.Fatal(err)
	}
	if err := commandJournal.Close(); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{valuePath, commandPath} {
		if err := os.Rename(path, path+".released"); err != nil {
			t.Fatalf("stable rotation retries leaked a pin for %q: %v", path, err)
		}
	}
}
