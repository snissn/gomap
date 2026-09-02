package mongogateway

import (
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestMongoGatewayCapabilityManifestIsValidAndExecutable(t *testing.T) {
	manifest := MongoGatewayCapabilities()
	if manifest.Schema != MongoGatewayCapabilitySchema {
		t.Fatalf("schema=%q want %q", manifest.Schema, MongoGatewayCapabilitySchema)
	}
	if manifest.Version != MongoGatewayCapabilityVersion {
		t.Fatalf("version=%d want %d", manifest.Version, MongoGatewayCapabilityVersion)
	}
	if err := ValidateMongoGatewayCapabilityManifest(manifest); err != nil {
		t.Fatalf("validate manifest: %v", err)
	}
	if len(manifest.Capabilities) < 40 {
		t.Fatalf("capabilities=%d want at least 40", len(manifest.Capabilities))
	}

	probes := mongoCompatibilityMatrixProbes()
	if err := validateMongoCompatibilityProbes(manifest, probes); err != nil {
		t.Fatalf("validate executable probes: %v", err)
	}
}

func TestMongoGatewayCapabilityManifestRejectsDuplicateIdentity(t *testing.T) {
	manifest := MongoGatewayCapabilities()
	manifest.Capabilities = append(manifest.Capabilities, manifest.Capabilities[0])
	if err := ValidateMongoGatewayCapabilityManifest(manifest); err == nil || !strings.Contains(err.Error(), "duplicate capability id") {
		t.Fatalf("duplicate validation err=%v want duplicate capability id", err)
	}
}

func TestMongoGatewayCapabilityManifestRejectsMissingProbe(t *testing.T) {
	manifest := MongoGatewayCapabilities()
	probes := mongoCompatibilityMatrixProbes()
	probes = probes[1:]
	if err := validateMongoCompatibilityProbes(manifest, probes); err == nil || !strings.Contains(err.Error(), "missing executable probe") {
		t.Fatalf("missing probe validation err=%v want missing executable probe", err)
	}
}

// Probe classifications are deliberately independent of manifest rows so a
// documentation-only status edit cannot silently rewrite executable evidence.
func TestMongoGatewayCapabilityManifestRejectsProbeStatusDrift(t *testing.T) {
	manifest := MongoGatewayCapabilities()
	probes := mongoCompatibilityMatrixProbes()
	manifest.Capabilities[0].Status = MongoCapabilityRejected
	if err := validateMongoCompatibilityProbes(manifest, probes); err == nil || !strings.Contains(err.Error(), "does not match executable probe status") {
		t.Fatalf("status drift validation err=%v want executable probe status mismatch", err)
	}
}

func TestMongoGatewayAdvertisedMetadataMatchesManifest(t *testing.T) {
	manifest := MongoGatewayCapabilities()
	advertised := manifest.Advertised
	server := &Server{MaxMessageLength: 1 << 20}

	helloBytes, err := bson.Marshal(helloResponse(server.maxMessageLength(), true))
	if err != nil {
		t.Fatalf("marshal hello: %v", err)
	}
	hello := bson.Raw(helloBytes)
	if got, ok := hello.Lookup("maxWireVersion").Int32OK(); !ok || got != advertised.MaxWireVersion {
		t.Fatalf("hello maxWireVersion=%d ok=%v want %d", got, ok, advertised.MaxWireVersion)
	}
	if got, ok := hello.Lookup("logicalSessionTimeoutMinutes").Int32OK(); !ok || got != advertised.LogicalSessionTimeoutMinutes {
		t.Fatalf("hello logicalSessionTimeoutMinutes=%d ok=%v want %d", got, ok, advertised.LogicalSessionTimeoutMinutes)
	}
	if got, ok := hello.Lookup("maxMessageSizeBytes").Int32OK(); !ok || got != server.maxMessageLength() {
		t.Fatalf("hello maxMessageSizeBytes=%d ok=%v want %d", got, ok, server.maxMessageLength())
	}

	buildInfoBytes, err := bson.Marshal(buildInfoResponse())
	if err != nil {
		t.Fatalf("marshal buildInfo: %v", err)
	}
	buildInfo := bson.Raw(buildInfoBytes)
	if got, ok := buildInfo.Lookup("version").StringValueOK(); !ok || got != advertised.MongoVersion {
		t.Fatalf("buildInfo version=%q ok=%v want %q", got, ok, advertised.MongoVersion)
	}
	manifestDoc, ok := buildInfo.Lookup("treedbCapabilityManifest").DocumentOK()
	if !ok {
		t.Fatalf("buildInfo is missing treedbCapabilityManifest: %v", buildInfo)
	}
	if got, ok := manifestDoc.Lookup("identity").StringValueOK(); !ok || got != MongoGatewayCapabilityIdentity() {
		t.Fatalf("buildInfo capability identity=%q ok=%v want %q", got, ok, MongoGatewayCapabilityIdentity())
	}
}

func TestMongoGatewayCapabilityIdentityIncludesSchemaAndVersion(t *testing.T) {
	identity := MongoGatewayCapabilityIdentity()
	if !strings.Contains(identity, MongoGatewayCapabilitySchema) || !strings.Contains(identity, "v1") || !strings.Contains(identity, "sha256:") {
		t.Fatalf("identity=%q does not bind schema, version, and digest", identity)
	}
}

func TestMongoGatewayCapabilityManifestRejectsDuplicateProbe(t *testing.T) {
	manifest := MongoGatewayCapabilities()
	probes := mongoCompatibilityMatrixProbes()
	probes = append(probes, probes[0])
	if err := validateMongoCompatibilityProbes(manifest, probes); err == nil || !strings.Contains(err.Error(), "duplicate executable probe") {
		t.Fatalf("duplicate probe validation err=%v want duplicate executable probe", err)
	}
}

func TestMongoGatewayCapabilityManifestRejectsExtraProbe(t *testing.T) {
	manifest := MongoGatewayCapabilities()
	probes := mongoCompatibilityMatrixProbes()
	probes = append(probes, mongoCompatibilityMatrixProbe{
		capabilityID: "hand-maintained.extra-row",
		probe:        func(*testing.T, *Server) {},
	})
	if err := validateMongoCompatibilityProbes(manifest, probes); err == nil || !strings.Contains(err.Error(), "unknown capability") {
		t.Fatalf("extra probe validation err=%v want unknown capability", err)
	}
}
