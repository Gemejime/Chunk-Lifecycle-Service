package unit

import (
	"context"
	"fmt"
	"testing"

	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/domain/compiler"
	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/domain/entity"
)

func makeTestChildChunk(t *testing.T, tenantID, sectionKey, text string, tokenCount int, chunkIndex int) *entity.ChunkNode {
	t.Helper()
	hasher := compiler.NewContentHasher()
	structuralPath := fmt.Sprintf("%s/para_%d", sectionKey, chunkIndex)
	chunkID := hasher.ComputeChunkID(tenantID, text, structuralPath)

	meta := map[string]string{
		"section_key":  sectionKey,
		"heading_path": sectionKey,
	}

	chunk, err := entity.NewChunkNode(
		chunkID,
		tenantID,
		"",
		text,
		tokenCount,
		structuralPath,
		0,
		int64(len(text)),
		meta,
	)
	if err != nil {
		t.Fatalf("failed to create test child chunk: %v", err)
	}
	return chunk
}

func TestContextCompiler_GroupingWithinBudget(t *testing.T) {
	comp := compiler.NewContextCompiler()
	ctx := context.Background()
	tenantID := "tenant-test"
	docID := "doc-101"
	sectionKey := "doc-101/sec_1"

	// Create 4 child chunks with 250 tokens each = 1,000 tokens total (fits in [800, 1500])
	var chunks []*entity.ChunkNode
	for i := 1; i <= 4; i++ {
		chunks = append(chunks, makeTestChildChunk(t, tenantID, sectionKey, fmt.Sprintf("Child paragraph %d content.", i), 250, i))
	}

	containers, err := comp.Compile(ctx, tenantID, docID, 1, chunks)
	if err != nil {
		t.Fatalf("unexpected compile error: %v", err)
	}

	if len(containers) != 1 {
		t.Fatalf("expected exactly 1 parent container, got %d", len(containers))
	}

	c := containers[0]
	if c.TokenCount() != 1000 {
		t.Errorf("expected token count 1000, got %d", c.TokenCount())
	}
	if len(c.ChildChunkIDs()) != 4 {
		t.Errorf("expected 4 child chunk IDs, got %d", len(c.ChildChunkIDs()))
	}
	if c.TenantID() != tenantID || c.DocumentID() != docID || c.ManifestVersion() != 1 {
		t.Errorf("metadata mismatch in parent container: %+v", c)
	}
}

func TestContextCompiler_UpperBoundSplit(t *testing.T) {
	comp := compiler.NewContextCompiler()
	ctx := context.Background()
	tenantID := "tenant-test"
	docID := "doc-102"
	sectionKey := "doc-102/sec_1"

	// Create 7 child chunks with 250 tokens each = 1,750 tokens total
	// Container 1: 6 chunks * 250 = 1,500 tokens (hits upper budget ceiling)
	// Container 2: 1 chunk * 250 = 250 tokens (tail window at EOF)
	var chunks []*entity.ChunkNode
	for i := 1; i <= 7; i++ {
		chunks = append(chunks, makeTestChildChunk(t, tenantID, sectionKey, fmt.Sprintf("Child paragraph %d with significant text.", i), 250, i))
	}

	containers, err := comp.Compile(ctx, tenantID, docID, 1, chunks)
	if err != nil {
		t.Fatalf("unexpected compile error: %v", err)
	}

	if len(containers) != 2 {
		t.Fatalf("expected 2 parent containers due to 1,500 token ceiling split, got %d", len(containers))
	}

	if containers[0].TokenCount() != 1500 {
		t.Errorf("expected first container token count 1500, got %d", containers[0].TokenCount())
	}
	if len(containers[0].ChildChunkIDs()) != 6 {
		t.Errorf("expected first container to hold 6 chunks, got %d", len(containers[0].ChildChunkIDs()))
	}

	if containers[1].TokenCount() != 250 {
		t.Errorf("expected second container token count 250, got %d", containers[1].TokenCount())
	}
	if len(containers[1].ChildChunkIDs()) != 1 {
		t.Errorf("expected second container to hold 1 chunk, got %d", len(containers[1].ChildChunkIDs()))
	}
}

func TestContextCompiler_StructuralSectionBoundarySplit(t *testing.T) {
	comp := compiler.NewContextCompiler()
	ctx := context.Background()
	tenantID := "tenant-test"
	docID := "doc-103"

	// Section 1: 2 chunks * 200 tokens = 400 tokens (tail window at section boundary)
	// Section 2: 4 chunks * 250 tokens = 1000 tokens
	var chunks []*entity.ChunkNode
	chunks = append(chunks, makeTestChildChunk(t, tenantID, "doc-103/sec_1", "Section 1 Para 1", 200, 1))
	chunks = append(chunks, makeTestChildChunk(t, tenantID, "doc-103/sec_1", "Section 1 Para 2", 200, 2))

	chunks = append(chunks, makeTestChildChunk(t, tenantID, "doc-103/sec_2", "Section 2 Para 1", 250, 1))
	chunks = append(chunks, makeTestChildChunk(t, tenantID, "doc-103/sec_2", "Section 2 Para 2", 250, 2))
	chunks = append(chunks, makeTestChildChunk(t, tenantID, "doc-103/sec_2", "Section 2 Para 3", 250, 3))
	chunks = append(chunks, makeTestChildChunk(t, tenantID, "doc-103/sec_2", "Section 2 Para 4", 250, 4))

	containers, err := comp.Compile(ctx, tenantID, docID, 1, chunks)
	if err != nil {
		t.Fatalf("unexpected compile error: %v", err)
	}

	if len(containers) != 2 {
		t.Fatalf("expected 2 parent containers split at section boundary, got %d", len(containers))
	}

	if containers[0].HeadingPath() != "doc-103/sec_1" {
		t.Errorf("expected heading path doc-103/sec_1, got %s", containers[0].HeadingPath())
	}
	if containers[0].TokenCount() != 400 {
		t.Errorf("expected first container token count 400, got %d", containers[0].TokenCount())
	}

	if containers[1].HeadingPath() != "doc-103/sec_2" {
		t.Errorf("expected heading path doc-103/sec_2, got %s", containers[1].HeadingPath())
	}
	if containers[1].TokenCount() != 1000 {
		t.Errorf("expected second container token count 1000, got %d", containers[1].TokenCount())
	}
}

func TestContextCompiler_TailWindowAllowance(t *testing.T) {
	comp := compiler.NewContextCompiler()
	ctx := context.Background()
	tenantID := "tenant-test"
	docID := "doc-104"

	// Single small chunk (150 tokens) at EOF should emit successfully without ErrTokenBudgetViolation
	chunks := []*entity.ChunkNode{
		makeTestChildChunk(t, tenantID, "doc-104/sec_1", "Short introductory section paragraph.", 150, 1),
	}

	containers, err := comp.Compile(ctx, tenantID, docID, 1, chunks)
	if err != nil {
		t.Fatalf("expected small tail window (< 800 tokens) to succeed, got %v", err)
	}

	if len(containers) != 1 {
		t.Fatalf("expected 1 container, got %d", len(containers))
	}
	if containers[0].TokenCount() != 150 {
		t.Errorf("expected token count 150, got %d", containers[0].TokenCount())
	}
}

func TestContextCompiler_DeterministicParentID(t *testing.T) {
	comp := compiler.NewContextCompiler()
	ctx := context.Background()
	tenantID := "tenant-alpha"
	docID := "doc-deterministic"

	chunks1 := []*entity.ChunkNode{
		makeTestChildChunk(t, tenantID, "doc-deterministic/sec_1", "Consistent text content for testing hashing.", 200, 1),
		makeTestChildChunk(t, tenantID, "doc-deterministic/sec_1", "Second paragraph text for deterministic test.", 250, 2),
	}

	chunks2 := []*entity.ChunkNode{
		makeTestChildChunk(t, tenantID, "doc-deterministic/sec_1", "Consistent text content for testing hashing.", 200, 1),
		makeTestChildChunk(t, tenantID, "doc-deterministic/sec_1", "Second paragraph text for deterministic test.", 250, 2),
	}

	containers1, err := comp.Compile(ctx, tenantID, docID, 1, chunks1)
	if err != nil {
		t.Fatalf("first compilation failed: %v", err)
	}

	containers2, err := comp.Compile(ctx, tenantID, docID, 1, chunks2)
	if err != nil {
		t.Fatalf("second compilation failed: %v", err)
	}

	if containers1[0].ParentID() != containers2[0].ParentID() {
		t.Errorf("expected deterministic ParentIDs to match: %s vs %s",
			containers1[0].ParentID(), containers2[0].ParentID())
	}
	if len(containers1[0].ParentID()) != 64 {
		t.Errorf("expected 64-character hex ParentID, got length %d", len(containers1[0].ParentID()))
	}
}

func BenchmarkContextCompiler(b *testing.B) {
	comp := compiler.NewContextCompiler()
	ctx := context.Background()
	tenantID := "tenant-bench"
	docID := "doc-bench"

	var chunks []*entity.ChunkNode
	for s := 1; s <= 10; s++ {
		secKey := fmt.Sprintf("doc-bench/sec_%d", s)
		for p := 1; p <= 5; p++ {
			hasher := compiler.NewContentHasher()
			structuralPath := fmt.Sprintf("%s/para_%d", secKey, p)
			chunkID := hasher.ComputeChunkID(tenantID, "Benchmark text content for child chunk.", structuralPath)
			node, _ := entity.NewChunkNode(
				chunkID, tenantID, "", "Benchmark text content for child chunk.", 200,
				structuralPath, 0, 50, map[string]string{"section_key": secKey},
			)
			chunks = append(chunks, node)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := comp.Compile(ctx, tenantID, docID, 1, chunks)
		if err != nil {
			b.Fatalf("benchmark compile failed: %v", err)
		}
	}
}
