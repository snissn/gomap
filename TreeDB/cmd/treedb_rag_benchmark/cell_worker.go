package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// applicationCellWorkerBuildRevision is set with -ldflags=-X for exact
// cross-revision workers when the Go toolchain cannot stamp a linked worktree.
// The coordinator also binds the resulting binary SHA-256.
var applicationCellWorkerBuildRevision string

const applicationCellWorkerEnvironmentPolicy = "fresh_per_cell"

type applicationCellWorkerRequest struct {
	Ordinal int `json:"ordinal"`
}

type applicationCellWorkerReady struct {
	Ready             bool   `json:"ready"`
	CellCount         int    `json:"cell_count"`
	ProductBaseSHA    string `json:"product_base_sha"`
	HarnessRevision   string `json:"harness_revision"`
	EnvironmentPolicy string `json:"environment_policy"`
}

type applicationCellWorkerResponse struct {
	Ordinal int             `json:"ordinal"`
	Row     *applicationRow `json:"row,omitempty"`
	Error   string          `json:"error,omitempty"`
}

type applicationCellWorkerEnvironment struct {
	env          *applicationEnvironment
	queryVectors map[string][]float32
}

func applicationCellCount() int {
	count := 0
	for _, embeddingCell := range applicationEmbeddings {
		count += len(applicationCellMatrix(embeddingCell))
	}
	return count
}

func applicationCellByOrdinal(ordinal int) (applicationCellIdentity, bool) {
	if ordinal < 0 {
		return applicationCellIdentity{}, false
	}
	for _, embeddingCell := range applicationEmbeddings {
		cells := applicationCellMatrix(embeddingCell)
		if ordinal < len(cells) {
			return cells[ordinal], true
		}
		ordinal -= len(cells)
	}
	return applicationCellIdentity{}, false
}

func runApplicationCellWorker(cfg applicationConfig, input io.Reader, output io.Writer) error {
	if err := validateApplicationConfig(cfg); err != nil {
		return err
	}
	if cfg.FinalEvidence {
		if applicationCellWorkerBuildRevision != "" {
			if !isFullRevision(applicationCellWorkerBuildRevision) || cfg.HarnessRevision != applicationCellWorkerBuildRevision {
				return fmt.Errorf("provenance: requested harness revision %q does not match linked cell-worker revision %q", cfg.HarnessRevision, applicationCellWorkerBuildRevision)
			}
		} else {
			settings, ok := runtimeBuildInfo()
			if _, err := resolveApplicationHarnessRevision(cfg, settings, ok); err != nil {
				return err
			}
		}
	}
	fixture := buildApplicationFixture()
	if err := validateApplicationFixture(&fixture); err != nil {
		return err
	}
	bundle, err := loadSemanticVectors()
	if err != nil {
		return err
	}
	if err := validateSemanticVectors(&fixture, bundle); err != nil {
		return err
	}
	if err := registerSemanticProvider(bundle); err != nil {
		return err
	}

	root := cfg.Dir
	removeRoot := false
	if root == "" {
		root, err = os.MkdirTemp("", "treedb_rag_cell_worker_*")
		if err != nil {
			return err
		}
		removeRoot = !cfg.KeepDir
	} else {
		if err := os.RemoveAll(root); err != nil {
			return err
		}
		if err := os.MkdirAll(root, 0o755); err != nil {
			return err
		}
	}
	if removeRoot {
		defer os.RemoveAll(root)
	}

	openEnvironment := func(embeddingCell string) (*applicationCellWorkerEnvironment, error) {
		environmentDir := filepath.Join(root, embeddingCell)
		if err := os.RemoveAll(environmentDir); err != nil {
			return nil, err
		}
		dims, provider := embeddingCellConfig(embeddingCell, bundle)
		env, lifecycle, err := openApplicationEnvironment(cfg, &fixture, bundle, embeddingCell, provider, dims, environmentDir)
		if err != nil {
			return nil, err
		}
		if err := validateLifecycleEvidence(embeddingCell, lifecycle); err != nil {
			env.close()
			return nil, err
		}
		queryVectors, err := applicationQueryVectors(&fixture, bundle, embeddingCell, provider, dims)
		if err != nil {
			env.close()
			return nil, err
		}
		return &applicationCellWorkerEnvironment{env: env, queryVectors: queryVectors}, nil
	}

	buffered := bufio.NewWriter(output)
	encoder := json.NewEncoder(buffered)
	if err := encoder.Encode(applicationCellWorkerReady{Ready: true, CellCount: applicationCellCount(), ProductBaseSHA: cfg.ProductBaseSHA, HarnessRevision: cfg.HarnessRevision, EnvironmentPolicy: applicationCellWorkerEnvironmentPolicy}); err != nil {
		return err
	}
	if err := buffered.Flush(); err != nil {
		return err
	}

	decoder := json.NewDecoder(input)
	for {
		var request applicationCellWorkerRequest
		if err := decoder.Decode(&request); err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("decode cell request: %w", err)
		}
		cell, ok := applicationCellByOrdinal(request.Ordinal)
		if !ok {
			response := applicationCellWorkerResponse{Ordinal: request.Ordinal, Error: fmt.Sprintf("cell ordinal %d outside [0,%d)", request.Ordinal, applicationCellCount())}
			if err := encoder.Encode(response); err != nil {
				return err
			}
			if err := buffered.Flush(); err != nil {
				return err
			}
			continue
		}

		var state *applicationCellWorkerEnvironment
		var env *applicationEnvironment
		var queryVectors map[string][]float32
		if unsupportedCapability(cell) == nil {
			state, err := openEnvironment(cell.Embedding)
			if err != nil {
				response := applicationCellWorkerResponse{Ordinal: request.Ordinal, Error: err.Error()}
				_ = encoder.Encode(response)
				_ = buffered.Flush()
				return err
			}
			env, queryVectors = state.env, state.queryVectors
		}
		row, err := runApplicationCell(cfg, &fixture, env, queryVectors, cell)
		if state != nil {
			state.env.close()
		}
		response := applicationCellWorkerResponse{Ordinal: request.Ordinal, Row: &row}
		if err != nil {
			response.Row = nil
			response.Error = err.Error()
		}
		if err := encoder.Encode(response); err != nil {
			return err
		}
		if err := buffered.Flush(); err != nil {
			return err
		}
		if response.Error != "" {
			return fmt.Errorf("cell %d: %s", request.Ordinal, response.Error)
		}
	}
}
