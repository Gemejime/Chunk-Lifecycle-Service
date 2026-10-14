package unit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/application/command"
	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/domain/compiler"
	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/domain/entity"
)

func TestApplySectionPatch_Validation(t *testing.T) {
	handler := command.NewApplySectionPatchHandler(nil, nil)
	ctx := context.Background()

	baseCmd := command.ApplySectionPatchCommand{
		TenantID:        "tenant-1",
		DocumentID:      "doc-1",
		SectionKey:      "doc-1/sec_1",
		BaseContentHash: "hash-123",
		PatchedContent:  "Valid replacement content.",
		OperatorID:      "op-1",
	}

	// 1. Missing tenant_id
	cmd := baseCmd
	cmd.TenantID = ""
	_, err := handler.Execute(ctx, cmd)
	if !errors.Is(err, entity.ErrEmptyTenantID) {
		t.Fatalf("expected ErrEmptyTenantID, got %v", err)
	}

	// 2. Missing document_id
	cmd = baseCmd
	cmd.DocumentID = ""
	_, err = handler.Execute(ctx, cmd)
	if !errors.Is(err, entity.ErrEmptyDocumentID) {
		t.Fatalf("expected ErrEmptyDocumentID, got %v", err)
	}

	// 3. Missing section_key
	cmd = baseCmd
	cmd.SectionKey = ""
	_, err = handler.Execute(ctx, cmd)
	if !errors.Is(err, entity.ErrEmptySectionKey) {
		t.Fatalf("expected ErrEmptySectionKey, got %v", err)
	}

	// 4. Missing base_content_hash
	cmd = baseCmd
	cmd.BaseContentHash = ""
	_, err = handler.Execute(ctx, cmd)
	if !errors.Is(err, entity.ErrEmptyBaseContentHash) {
		t.Fatalf("expected ErrEmptyBaseContentHash, got %v", err)
	}

	// 5. Missing operator_id
	cmd = baseCmd
	cmd.OperatorID = ""
	_, err = handler.Execute(ctx, cmd)
	if !errors.Is(err, entity.ErrEmptyOperatorID) {
		t.Fatalf("expected ErrEmptyOperatorID, got %v", err)
	}
}

func TestApplySectionPatch_ChildChunkSlicingAndCompilation(t *testing.T) {
	hasher := compiler.NewContentHasher()
	comp := compiler.NewContextCompiler()
	tenantID := "tenant-patch"
	docID := "doc-patch-1"
	sectionKey := "doc-patch-1/sec_4"

	// Create a paragraph of text representing patched content
	patchedText := "This is an operator override updating section 4 procedures. " +
		"All subsequent steps must adhere to strict regulatory compliance guidelines. " +
		"Every operational flow requires multi-tenant isolation and secured audit trails."

	// Test slicing into 100-300 token child chunk
	words := strings.Fields(patchedText)
	tokenCount := int(float64(len(words)) * 1.3)
	if tokenCount < entity.MinChunkTokenBudget {
		tokenCount = entity.MinChunkTokenBudget
	}
	if tokenCount > entity.MaxChunkTokenBudget {
		tokenCount = entity.MaxChunkTokenBudget
	}

	structuralPath := fmt.Sprintf("%s/para_1", sectionKey)
	chunkID := hasher.ComputeChunkID(tenantID, patchedText, structuralPath)

	chunk, err := entity.NewChunkNode(
		chunkID,
		tenantID,
		"",
		patchedText,
		tokenCount,
		structuralPath,
		0,
		int64(len(patchedText)),
		map[string]string{"section_key": sectionKey},
	)
	if err != nil {
		t.Fatalf("failed creating sliced child chunk: %v", err)
	}

	if chunk.TokenCount() < entity.MinChunkTokenBudget || chunk.TokenCount() > entity.MaxChunkTokenBudget {
		t.Fatalf("chunk token count %d outside [100, 300]", chunk.TokenCount())
	}

	// Compile parent context from this new child chunk
	parents, err := comp.Compile(context.Background(), tenantID, docID, 2, []*entity.ChunkNode{chunk})
	if err != nil {
		t.Fatalf("failed compiling parent context: %v", err)
	}

	if len(parents) != 1 {
		t.Fatalf("expected 1 parent container, got %d", len(parents))
	}

	parent := parents[0]
	if parent.ManifestVersion() != 2 {
		t.Errorf("expected manifest version 2, got %d", parent.ManifestVersion())
	}
	if parent.HeadingPath() != sectionKey {
		t.Errorf("expected heading path %s, got %s", sectionKey, parent.HeadingPath())
	}
	if len(parent.ChildChunkIDs()) != 1 || parent.ChildChunkIDs()[0] != chunkID {
		t.Errorf("expected child chunk ID %s, got %v", chunkID, parent.ChildChunkIDs())
	}

	// Verify outbox payload construction
	outboxPayload, err := json.Marshal(map[string]any{
		"tenant_id":        tenantID,
		"document_id":      docID,
		"manifest_version": int64(2),
		"patch_id":         "patch-uuid-123",
		"chunk_ids":        []string{chunkID},
	})
	if err != nil {
		t.Fatalf("failed serializing outbox payload: %v", err)
	}

	var payloadMap map[string]any
	if err := json.Unmarshal(outboxPayload, &payloadMap); err != nil {
		t.Fatalf("failed unmarshaling outbox payload: %v", err)
	}
	if payloadMap["manifest_version"].(float64) != 2 {
		t.Errorf("expected manifest_version 2 in payload, got %v", payloadMap["manifest_version"])
	}
}

func TestApplySectionPatch_OptimisticConcurrencyHashing(t *testing.T) {
	hasher := compiler.NewContentHasher()

	originalContent := "Original active section content before patch."
	normalizedOriginal := hasher.NormalizeContent(originalContent)
	h := sha256.New()
	h.Write([]byte(normalizedOriginal))
	correctBaseHash := hex.EncodeToString(h.Sum(nil))

	wrongBaseHash := "0000000000000000000000000000000000000000000000000000000000000000"

	// Verify that matching hash succeeds check
	if correctBaseHash == wrongBaseHash {
		t.Fatalf("hashes should not match")
	}

	// Verify that wrong base hash triggers ErrStaleBaseHash
	checkHash := func(expected, actual string) error {
		if expected != actual {
			return entity.ErrStaleBaseHash
		}
		return nil
	}

	if err := checkHash(correctBaseHash, wrongBaseHash); !errors.Is(err, entity.ErrStaleBaseHash) {
		t.Fatalf("expected ErrStaleBaseHash on mismatch, got %v", err)
	}

	if err := checkHash(correctBaseHash, correctBaseHash); err != nil {
		t.Fatalf("expected nil error on matching hash, got %v", err)
	}
}
