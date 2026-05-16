package docs_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type commandWALNativeWireAlignment struct {
	Version            int                                  `json:"version"`
	Owner              string                               `json:"owner"`
	Tracker            string                               `json:"tracker"`
	RelationshipValues []string                             `json:"relationship_values"`
	AckRecoverability  map[string]string                    `json:"ack_recoverability"`
	Entries            []commandWALNativeWireAlignmentEntry `json:"entries"`
}

type commandWALNativeWireAlignmentEntry struct {
	NativeWireCommand       string `json:"nativewire_command"`
	NativeWireFixture       string `json:"nativewire_fixture"`
	NativeWireFixtureSHA256 string `json:"nativewire_fixture_sha256"`
	CommandWALKind          string `json:"command_wal_kind"`
	CommandWALPayloadFormat string `json:"command_wal_payload_format"`
	LocalFixture            string `json:"local_fixture"`
	LocalFixtureSHA256      string `json:"local_fixture_sha256"`
	SupportMatrixStatus     string `json:"support_matrix_status"`
	Relationship            string `json:"relationship"`
	Notes                   string `json:"notes"`
}

func TestCommandWALPayloadMatchesNativeWireDeterministicFixture(t *testing.T) {
	alignment := loadCommandWALNativeWireAlignment(t)
	matrix := loadCommandWALSupportMatrix(t)
	matrixByNativeWire := make(map[string]commandWALSupportEntry)
	for _, entry := range matrix.Entries {
		if entry.Surface == "nativewire" {
			matrixByNativeWire[entry.EntryPoint] = entry
		}
	}
	seen := make(map[string]struct{}, len(alignment.Entries))
	for _, entry := range alignment.Entries {
		if entry.NativeWireCommand == "" || entry.CommandWALKind == "" || entry.CommandWALPayloadFormat == "" {
			t.Fatalf("incomplete alignment entry: %+v", entry)
		}
		if _, ok := seen[entry.NativeWireCommand]; ok {
			t.Fatalf("duplicate native-wire alignment entry for %s", entry.NativeWireCommand)
		}
		seen[entry.NativeWireCommand] = struct{}{}
		matrixEntry, ok := matrixByNativeWire[entry.NativeWireCommand]
		if !ok {
			t.Fatalf("alignment entry %s missing from command WAL support matrix", entry.NativeWireCommand)
		}
		if matrixEntry.Status != entry.SupportMatrixStatus || matrixEntry.Command != entry.CommandWALKind {
			t.Fatalf("alignment entry %s disagrees with support matrix: alignment=%+v matrix=%+v", entry.NativeWireCommand, entry, matrixEntry)
		}
		assertHexFixtureDigest(t, entry.NativeWireFixture, entry.NativeWireFixtureSHA256)
		if entry.SupportMatrixStatus == "WAL-supported" {
			if entry.LocalFixture == "" || entry.LocalFixtureSHA256 == "" {
				t.Fatalf("%s is WAL-supported without local command-WAL fixture", entry.NativeWireCommand)
			}
			assertHexFixtureDigest(t, entry.LocalFixture, entry.LocalFixtureSHA256)
		}
	}
	for command := range matrixByNativeWire {
		if _, ok := seen[command]; !ok {
			t.Fatalf("native-wire matrix command %s missing alignment entry", command)
		}
	}
}

func TestNativeWireAndLocalCommandDigestStable(t *testing.T) {
	alignment := loadCommandWALNativeWireAlignment(t)
	allowed := make(map[string]struct{}, len(alignment.RelationshipValues))
	for _, value := range alignment.RelationshipValues {
		allowed[value] = struct{}{}
	}
	fixtureDigests := make(map[string]string)
	for _, entry := range alignment.Entries {
		if _, ok := allowed[entry.Relationship]; !ok {
			t.Fatalf("%s has unknown relationship %q", entry.NativeWireCommand, entry.Relationship)
		}
		if entry.Notes == "" {
			t.Fatalf("%s missing alignment notes", entry.NativeWireCommand)
		}
		fixtureDigests[entry.NativeWireFixture] = entry.NativeWireFixtureSHA256
		if entry.LocalFixture != "" {
			fixtureDigests[entry.LocalFixture] = entry.LocalFixtureSHA256
		}
		if entry.Relationship == "future_rejected_v1" && entry.LocalFixture != "" {
			t.Fatalf("%s is rejected/future but has local fixture %s", entry.NativeWireCommand, entry.LocalFixture)
		}
		if entry.Relationship == "lowered_equivalent_v1" && entry.SupportMatrixStatus != "WAL-supported" {
			t.Fatalf("%s lowered-equivalent relationship requires WAL-supported status", entry.NativeWireCommand)
		}
	}
	if len(fixtureDigests) == 0 {
		t.Fatal("alignment has no fixture digests")
	}
}

func TestNativeWireAckFlushedRequiresRootPublishAndAppliedLSN(t *testing.T) {
	alignment := loadCommandWALNativeWireAlignment(t)
	if got := alignment.AckRecoverability["visible"]; !strings.Contains(got, "does not require root publication or AppliedLSN") {
		t.Fatalf("visible recoverability rule = %q", got)
	}
	if got := alignment.AckRecoverability["flushed"]; !strings.Contains(got, "roots and AppliedLSN") || !strings.Contains(got, "same backend commit") {
		t.Fatalf("flushed recoverability rule = %q", got)
	}
	protocol := readRepoText(t, "TreeDB/docs/spec/native-wire-protocol.md")
	for _, required := range []string{
		"It does not require root publication or\n`AppliedLSN` advancement.",
		"`flushed` means all touched collection state for the command has been published\nto backend roots, and WAL-backed commands have `AppliedLSN` advanced in the same\nbackend commit.",
	} {
		if !strings.Contains(protocol, required) {
			t.Fatalf("native-wire protocol missing ack/recoverability text: %q", required)
		}
	}
}

func TestNativeWirePostFramePublishFailureCommitAmbiguous(t *testing.T) {
	protocol := readRepoText(t, "TreeDB/docs/spec/native-wire-protocol.md")
	for _, required := range []string{
		"If a complete command frame reached the required local boundary but root\npublication, `AppliedLSN` advancement, visible install, flush, checkpoint, or\nresponse construction fails",
		"the server returns `commit_ambiguous`",
		"The server must not report\n`not_committed` after a complete command frame may be recovered and replayed.",
	} {
		if !strings.Contains(protocol, required) {
			t.Fatalf("native-wire protocol missing post-frame failure rule: %q", required)
		}
	}
}

func TestRaftApplyDoesNotReportRecoverableBeforeCommandWALAppliedLSN(t *testing.T) {
	walSpec := readRepoText(t, "TreeDB/docs/spec/user-command-wal.md")
	for _, required := range []string{
		"Native-wire and Raft alignment",
		"must lower to a\nlocal command-WAL frame and satisfy the requested local ack boundary before\nreporting local recoverability",
		"`raft_committed` is not local WAL append",
	} {
		if !strings.Contains(walSpec, required) {
			t.Fatalf("user-command WAL spec missing Raft/local recoverability rule: %q", required)
		}
	}
}

func TestRaftCommandEntryAndLocalCommandPayloadUseSharedCanonicalSchema(t *testing.T) {
	alignment := loadCommandWALNativeWireAlignment(t)
	for _, entry := range alignment.Entries {
		switch entry.Relationship {
		case "lowered_equivalent_v1":
			if !strings.Contains(entry.Notes, "lower") && !strings.Contains(entry.Notes, "stores") {
				t.Fatalf("%s lowered-equivalent entry does not document lowering/storage relationship: %q", entry.NativeWireCommand, entry.Notes)
			}
		case "future_rejected_v1":
			if entry.SupportMatrixStatus != "WAL-rejected" || !strings.Contains(entry.Notes, "reject") {
				t.Fatalf("%s future entry must stay explicitly rejected: %+v", entry.NativeWireCommand, entry)
			}
		default:
			t.Fatalf("%s has unrecognized relationship %q", entry.NativeWireCommand, entry.Relationship)
		}
	}
}

func loadCommandWALNativeWireAlignment(t *testing.T) commandWALNativeWireAlignment {
	t.Helper()
	path := repoPath(t, "TreeDB/docs/spec/command-wal-nativewire-alignment.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read native-wire alignment manifest: %v", err)
	}
	var alignment commandWALNativeWireAlignment
	if err := json.Unmarshal(raw, &alignment); err != nil {
		t.Fatalf("decode native-wire alignment manifest: %v", err)
	}
	if alignment.Version != 1 || alignment.Owner == "" || alignment.Tracker == "" {
		t.Fatalf("incomplete native-wire alignment manifest: %+v", alignment)
	}
	if len(alignment.Entries) == 0 {
		t.Fatal("native-wire alignment manifest has no entries")
	}
	return alignment
}

func assertHexFixtureDigest(t *testing.T, fixture, want string) {
	t.Helper()
	if fixture == "" || want == "" {
		t.Fatalf("missing fixture digest: fixture=%q sha256=%q", fixture, want)
	}
	raw := readRepoText(t, fixture)
	compact := strings.Join(strings.Fields(raw), "")
	if _, err := hex.DecodeString(compact); err != nil {
		t.Fatalf("%s is not valid hex fixture: %v", fixture, err)
	}
	sum := sha256.Sum256([]byte(raw))
	if got := hex.EncodeToString(sum[:]); got != want {
		t.Fatalf("%s sha256=%s want %s", fixture, got, want)
	}
}

func readRepoText(t *testing.T, rel string) string {
	t.Helper()
	raw, err := os.ReadFile(repoPath(t, rel))
	if err != nil {
		t.Fatalf("read %s: %v", rel, err)
	}
	return string(raw)
}

func repoPath(t *testing.T, rel string) string {
	t.Helper()
	_, repoRoot := repoRoots(t)
	return filepath.Join(repoRoot, filepath.FromSlash(rel))
}
