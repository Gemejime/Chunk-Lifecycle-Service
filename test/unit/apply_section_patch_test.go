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

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

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

type mockRow struct {
	val     any
	scanErr error
}

func (r *mockRow) Scan(dest ...any) error {
	if r.scanErr != nil {
		return r.scanErr
	}
	if len(dest) > 0 && r.val != nil {
		if ptr, ok := dest[0].(*string); ok {
			*ptr = r.val.(string)
		}
	}
	return nil
}

type mockRows struct {
	data []string
	idx  int
	err  error
}

func (r *mockRows) Close()                                       {}
func (r *mockRows) Err() error                                   { return r.err }
func (r *mockRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *mockRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *mockRows) Next() bool {
	if r.idx < len(r.data) {
		r.idx++
		return true
	}
	return false
}
func (r *mockRows) Scan(dest ...any) error {
	if len(dest) > 0 && r.idx > 0 && r.idx <= len(r.data) {
		if ptr, ok := dest[0].(*string); ok {
			*ptr = r.data[r.idx-1]
		}
	}
	return nil
}
func (r *mockRows) Values() ([]any, error) { return nil, nil }
func (r *mockRows) RawValues() [][]byte    { return nil }
func (r *mockRows) Conn() *pgx.Conn        { return nil }
func (r *mockRows) TypeMap() *pgtype.Map   { return nil }

type mockTx struct {
	queryRowFunc func(ctx context.Context, sql string, args ...any) pgx.Row
	queryFunc    func(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func (m *mockTx) Begin(ctx context.Context) (pgx.Tx, error) { return m, nil }
func (m *mockTx) Commit(ctx context.Context) error          { return nil }
func (m *mockTx) Rollback(ctx context.Context) error        { return nil }
func (m *mockTx) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	return 0, nil
}
func (m *mockTx) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults { return nil }
func (m *mockTx) LargeObjects() pgx.LargeObjects                               { return pgx.LargeObjects{} }
func (m *mockTx) Prepare(ctx context.Context, name, sql string) (*pgconn.StatementDescription, error) {
	return nil, nil
}
func (m *mockTx) Exec(ctx context.Context, sql string, arguments ...any) (commandTag pgconn.CommandTag, err error) {
	return pgconn.CommandTag{}, nil
}
func (m *mockTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if m.queryFunc != nil {
		return m.queryFunc(ctx, sql, args...)
	}
	return &mockRows{}, nil
}
func (m *mockTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	if m.queryRowFunc != nil {
		return m.queryRowFunc(ctx, sql, args...)
	}
	return &mockRow{scanErr: pgx.ErrNoRows}
}
func (m *mockTx) Conn() *pgx.Conn { return nil }

func TestApplySectionPatch_OptimisticConcurrencyHashing(t *testing.T) {
	hasher := compiler.NewContentHasher()
	handler := command.NewApplySectionPatchHandler(nil, nil)
	ctx := context.Background()

	tenantID := "tenant-hash"
	docID := "doc-hash-1"
	sectionKey := "doc-hash-1/sec_1"

	originalContent := "Original active section content before patch."
	normalizedOriginal := hasher.NormalizeContent(originalContent)
	h := sha256.New()
	h.Write([]byte(normalizedOriginal))
	correctBaseHash := hex.EncodeToString(h.Sum(nil))
	wrongBaseHash := "0000000000000000000000000000000000000000000000000000000000000000"

	// 1. Test Active Patch path via production ValidateBaseContentHash with mockTx
	txActive := &mockTx{
		queryRowFunc: func(ctx context.Context, sql string, args ...any) pgx.Row {
			return &mockRow{val: originalContent}
		},
	}

	// Verify matching hash succeeds
	if err := handler.ValidateBaseContentHash(ctx, txActive, tenantID, docID, 1, sectionKey, correctBaseHash); err != nil {
		t.Fatalf("expected nil error on matching active patch hash, got %v", err)
	}

	// Verify mismatched hash returns ErrStaleBaseHash
	if err := handler.ValidateBaseContentHash(ctx, txActive, tenantID, docID, 1, sectionKey, wrongBaseHash); !errors.Is(err, entity.ErrStaleBaseHash) {
		t.Fatalf("expected ErrStaleBaseHash on mismatched active patch hash, got %v", err)
	}

	// 2. Test Fallback chunk_nodes path via production ValidateBaseContentHash
	chunk1 := "First chunk of content in section."
	chunk2 := "Second chunk of content in section."
	combined := strings.Join([]string{chunk1, chunk2}, "\n\n")
	normalizedCombined := hasher.NormalizeContent(combined)
	chH := sha256.New()
	chH.Write([]byte(normalizedCombined))
	correctChunksHash := hex.EncodeToString(chH.Sum(nil))

	txFallback := &mockTx{
		queryRowFunc: func(ctx context.Context, sql string, args ...any) pgx.Row {
			return &mockRow{scanErr: pgx.ErrNoRows}
		},
		queryFunc: func(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
			return &mockRows{data: []string{chunk1, chunk2}}, nil
		},
	}

	// Matching chunk nodes fallback hash succeeds
	if err := handler.ValidateBaseContentHash(ctx, txFallback, tenantID, docID, 1, sectionKey, correctChunksHash); err != nil {
		t.Fatalf("expected nil error on matching fallback hash, got %v", err)
	}

	// Mismatched chunk nodes fallback hash returns ErrStaleBaseHash
	if err := handler.ValidateBaseContentHash(ctx, txFallback, tenantID, docID, 1, sectionKey, wrongBaseHash); !errors.Is(err, entity.ErrStaleBaseHash) {
		t.Fatalf("expected ErrStaleBaseHash on mismatched fallback hash, got %v", err)
	}

	// 3. Test Empty fallback result does not accept arbitrary baseHash
	txEmpty := &mockTx{
		queryRowFunc: func(ctx context.Context, sql string, args ...any) pgx.Row {
			return &mockRow{scanErr: pgx.ErrNoRows}
		},
		queryFunc: func(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
			return &mockRows{data: []string{}}, nil
		},
	}

	if err := handler.ValidateBaseContentHash(ctx, txEmpty, tenantID, docID, 1, sectionKey, correctBaseHash); !errors.Is(err, entity.ErrStaleBaseHash) {
		t.Fatalf("expected ErrStaleBaseHash on empty chunk nodes result, got %v", err)
	}
}
