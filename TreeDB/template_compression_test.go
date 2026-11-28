package treedb_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/caching"
	"github.com/snissn/gomap/TreeDB/template"
)

func makeMultiSchemaValue(schema, seq int) []byte {
	return []byte(fmt.Sprintf(
		`{"schema":"svc_%02d_order_event_v4","region":"us-west-%d","cluster":"prod-%02d","tenant":"org_%02d_enterprise","seq":"%010d","id":"ord_%010d","status":"confirmed","payload":"shared_static_template_padding_for_schema_%02d_value_log_compression_verification_block_0123456789abcdef"}`,
		schema, schema%4, schema%8, schema%16, seq, seq*17+schema, schema,
	))
}

func waitForTemplatePublished(t *testing.T, db *treedb.DB, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		stats := db.Stats()
		if n, _ := strconv.ParseUint(stats["treedb.cache.vlog_template.templates_published_total"], 10, 64); n > 0 {
			return
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for template publication; stats=%v", db.Stats())
}

func TestPublicTemplateOnlyAndPrepassReopenAndGC(t *testing.T) {
	modes := []struct {
		name           string
		mode           template.Mode
		outerLeafInLog bool
	}{
		{name: "TemplateOnly", mode: template.TemplateOnly},
		{name: "TemplatePrepass", mode: template.TemplatePrepass},
		{name: "TemplatePrepassOuterLeaf", mode: template.TemplatePrepass, outerLeafInLog: true},
	}

	for _, tc := range modes {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			opts := treedb.Options{
				Dir:                        dir,
				FlushThreshold:             256 << 10,
				IndexOuterLeavesInValueLog: tc.outerLeafInLog,
				ValueLog: treedb.ValueLogOptions{
					PointerThreshold: 64,
					Compression:      treedb.ValueLogCompressionAuto,
					TemplateMode:     tc.mode,
					TemplateConfig: template.Config{
						TrainSampleStride:      1,
						SynthesizeEverySamples: 64,
						PublishBatchSize:       1,
						PublishFlushEvery:      20 * time.Millisecond,
					},
				},
			}
			db, err := treedb.Open(opts)
			if err != nil {
				t.Fatalf("Open(%s): %v", tc.name, err)
			}

			expected := make(map[string][]byte, 2000)
			writeBatch := func(start, count int) {
				b := db.NewBatch()
				for i := start; i < start+count; i++ {
					k := fmt.Sprintf("k/%06d", i)
					v := makeMultiSchemaValue(i%4, i)
					expected[k] = v
					if err := b.Set([]byte(k), v); err != nil {
						t.Fatalf("Set(%s): %v", k, err)
					}
				}
				if err := b.Write(); err != nil {
					t.Fatalf("batch.Write: %v", err)
				}
				if err := b.Close(); err != nil {
					t.Fatalf("batch.Close: %v", err)
				}
			}

			// Phase 1: train templates and checkpoint so staged defs become durable.
			writeBatch(0, 600)
			waitForTemplatePublished(t, db, 3*time.Second)
			if err := db.Checkpoint(); err != nil {
				t.Fatalf("Checkpoint(1): %v", err)
			}

			// Phase 2: write post-activation records that must use templates.
			writeBatch(600, 1400)
			if err := db.Checkpoint(); err != nil {
				t.Fatalf("Checkpoint(2): %v", err)
			}

			stats := db.Stats()
			kept, _ := strconv.ParseUint(stats["treedb.cache.vlog_template.kept"], 10, 64)
			if kept == 0 {
				t.Fatalf("%s: expected vlog_template.kept > 0 after activation, stats=%v", tc.name, stats)
			}

			// Audit + GC must recognize live templates and never delete them.
			audit, err := db.SystemMetadataAudit(context.Background(), treedb.SystemMetadataAuditOptions{
				KeepRecentPublishedTemplates: -1,
			})
			if err != nil {
				t.Fatalf("SystemMetadataAudit: %v", err)
			}
			if audit.TemplatesLive == 0 {
				t.Fatalf("%s: expected TemplatesLive > 0 in audit, got %+v", tc.name, audit)
			}
			gcStats, err := db.SystemMetadataGC(context.Background(), treedb.SystemMetadataGCOptions{
				Audit: treedb.SystemMetadataAuditOptions{
					KeepRecentPublishedTemplates: -1,
				},
			})
			if err != nil {
				t.Fatalf("SystemMetadataGC: %v", err)
			}
			if gcStats.Audit.TemplatesLive == 0 {
				t.Fatalf("%s: expected TemplatesLive > 0 in GC audit, got %+v", tc.name, gcStats)
			}

			if err := db.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			// Reopen with DEFAULT TemplateOff and verify every value decodes seamlessly!
			reopened, err := treedb.Open(treedb.Options{Dir: dir})
			if err != nil {
				t.Fatalf("Reopen(TemplateOff): %v", err)
			}
			defer func() { _ = reopened.Close() }()

			for k, want := range expected {
				got, err := reopened.Get([]byte(k))
				if err != nil {
					t.Fatalf("Get(%s) after reopen: %v", k, err)
				}
				if !bytes.Equal(got, want) {
					t.Fatalf("Get(%s) mismatch after reopen: len=%d want=%d", k, len(got), len(want))
				}
			}
		})
	}
}

func TestPublicCOWRejectsNonOffTemplateModeWithoutStorageEffects(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "cow-no-create")
	opts := treedb.Options{
		Dir:          dir,
		MemtableMode: "cow_btree",
		Durability:   treedb.DurabilityWALOffRelaxed,
		ValueLog: treedb.ValueLogOptions{
			TemplateMode: template.TemplatePrepass,
		},
	}
	db, err := treedb.Open(opts)
	if db != nil {
		_ = db.Close()
		t.Fatalf("expected COW Open with TemplatePrepass to fail")
	}
	if !errors.Is(err, caching.ErrCOWUnsupported) {
		t.Fatalf("err=%v want ErrCOWUnsupported", err)
	}
	if _, statErr := os.Stat(dir); !os.IsNotExist(statErr) {
		t.Fatalf("COW preflight created storage directory %s despite refusal", dir)
	}
}
