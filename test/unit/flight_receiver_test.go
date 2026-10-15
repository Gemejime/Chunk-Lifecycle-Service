package unit

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/application/command"
	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/domain/entity"
	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/infrastructure/flight"
)

func TestFlightReceiver_DecodeRecordBatch_Success(t *testing.T) {
	receiver := flight.NewFlightReceiver(nil)

	batch := &flight.ColumnarRecordBatch{
		TenantIDs:         []string{"tenant-alpha", "tenant-alpha"},
		DocumentIDs:       []string{"doc-1", "doc-1"},
		NormalizedContent: []string{"First child chunk content with enough tokens.", "Second child chunk content with enough tokens."},
		StructuralPaths:   []string{"root/sec_1/p_1", "root/sec_1/p_2"},
		TokenCounts:       []int32{150, 200},
		SequenceIndices:   []int64{1, 2},
	}

	chunks, err := receiver.DecodeRecordBatch(batch)
	if err != nil {
		t.Fatalf("unexpected error decoding valid batch: %v", err)
	}

	if len(chunks) != 2 {
		t.Fatalf("expected 2 chunk nodes, got %d", len(chunks))
	}

	if chunks[0].TenantID() != "tenant-alpha" {
		t.Errorf("expected tenant-alpha, got %s", chunks[0].TenantID())
	}
	if chunks[0].TokenCount() != 150 {
		t.Errorf("expected 150 tokens, got %d", chunks[0].TokenCount())
	}
	if len(chunks[0].ChunkID()) != 64 {
		t.Errorf("expected 64-char sha256 chunk id, got %s", chunks[0].ChunkID())
	}
}

func TestFlightReceiver_DecodeRecordBatch_TokenBudgetBounds(t *testing.T) {
	receiver := flight.NewFlightReceiver(nil)

	// Underflow: 50 tokens (< 100)
	batchUnderflow := &flight.ColumnarRecordBatch{
		TenantIDs:         []string{"tenant-alpha"},
		DocumentIDs:       []string{"doc-1"},
		NormalizedContent: []string{"Too short"},
		StructuralPaths:   []string{"root/sec_1"},
		TokenCounts:       []int32{50},
		SequenceIndices:   []int64{1},
	}

	_, err := receiver.DecodeRecordBatch(batchUnderflow)
	if err == nil || !errors.Is(err, entity.ErrTokenBudgetViolation) {
		t.Fatalf("expected ErrTokenBudgetViolation for <100 tokens, got %v", err)
	}

	// Overflow: 350 tokens (> 300)
	batchOverflow := &flight.ColumnarRecordBatch{
		TenantIDs:         []string{"tenant-alpha"},
		DocumentIDs:       []string{"doc-1"},
		NormalizedContent: []string{"Too long"},
		StructuralPaths:   []string{"root/sec_1"},
		TokenCounts:       []int32{350},
		SequenceIndices:   []int64{1},
	}

	_, err = receiver.DecodeRecordBatch(batchOverflow)
	if err == nil || !errors.Is(err, entity.ErrTokenBudgetViolation) {
		t.Fatalf("expected ErrTokenBudgetViolation for >300 tokens, got %v", err)
	}
}

func TestFlightReceiver_MismatchedColumnLengths(t *testing.T) {
	receiver := flight.NewFlightReceiver(nil)

	batch := &flight.ColumnarRecordBatch{
		TenantIDs:         []string{"tenant-alpha", "tenant-alpha"},
		DocumentIDs:       []string{"doc-1"}, // Length 1 vs 2
		NormalizedContent: []string{"Text 1", "Text 2"},
		StructuralPaths:   []string{"sec_1", "sec_2"},
		TokenCounts:       []int32{120, 140},
		SequenceIndices:   []int64{1, 2},
	}

	_, err := receiver.DecodeRecordBatch(batch)
	if err != flight.ErrMismatchedColumnLengths {
		t.Fatalf("expected ErrMismatchedColumnLengths, got %v", err)
	}
}

func TestFlightReceiver_BackpressureSaturation(t *testing.T) {
	ingestHandler := command.NewIngestChunkBatchHandler(nil, nil)
	receiver := flight.NewFlightReceiver(ingestHandler)
	receiver.SetMaxInFlightLimits(0) // Immediately saturate

	batch := &flight.ColumnarRecordBatch{
		TenantIDs:         []string{"tenant-alpha"},
		DocumentIDs:       []string{"doc-1"},
		NormalizedContent: []string{"Valid chunk content text."},
		StructuralPaths:   []string{"root/sec_1"},
		TokenCounts:       []int32{150},
		SequenceIndices:   []int64{1},
	}

	_, err := receiver.IngestRecordBatch(context.Background(), batch, 1)
	if err != flight.ErrBackpressureSaturated {
		t.Fatalf("expected ErrBackpressureSaturated, got %v", err)
	}
}

func TestFlightReceiver_BinaryIPCRoundtrip(t *testing.T) {
	receiver := flight.NewFlightReceiver(nil)

	// Serialize binary buffer matching DecodeIPCStreamMessage expectation
	var buf bytes.Buffer
	rowCount := int32(1)
	_ = binary.Write(&buf, binary.BigEndian, rowCount)

	writeString := func(s string) {
		_ = binary.Write(&buf, binary.BigEndian, int32(len(s)))
		buf.WriteString(s)
	}

	// tenant_id
	writeString("tenant-xyz")
	// document_id
	writeString("doc-99")
	// normalized_content
	writeString("Chunk content from arrow IPC buffer payload.")
	// structural_path
	writeString("doc/chapter_1")
	// token_count
	_ = binary.Write(&buf, binary.BigEndian, int32(180))
	// sequence_index
	_ = binary.Write(&buf, binary.BigEndian, int64(10))

	decodedBatch, err := receiver.DecodeIPCStreamMessage(buf.Bytes())
	if err != nil {
		t.Fatalf("unexpected error decoding IPC stream: %v", err)
	}

	if decodedBatch.NumRows() != 1 {
		t.Fatalf("expected 1 row, got %d", decodedBatch.NumRows())
	}

	if decodedBatch.TenantIDs[0] != "tenant-xyz" {
		t.Errorf("expected tenant-xyz, got %s", decodedBatch.TenantIDs[0])
	}
	if decodedBatch.TokenCounts[0] != 180 {
		t.Errorf("expected 180 tokens, got %d", decodedBatch.TokenCounts[0])
	}

	chunks, err := receiver.DecodeRecordBatch(decodedBatch)
	if err != nil {
		t.Fatalf("unexpected error converting decoded batch: %v", err)
	}
	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk node, got %d", len(chunks))
	}
	if chunks[0].TokenCount() != 180 {
		t.Errorf("expected 180 tokens, got %d", chunks[0].TokenCount())
	}
}

func TestFlightReceiver_IngestRecordBatch_Success(t *testing.T) {
	ingestHandler := command.NewIngestChunkBatchHandler(nil, nil)
	receiver := flight.NewFlightReceiver(ingestHandler)

	batch := &flight.ColumnarRecordBatch{
		TenantIDs:         []string{"tenant-1"},
		DocumentIDs:       []string{"doc-1"},
		NormalizedContent: []string{"Sample content for flight execution test."},
		StructuralPaths:   []string{"root/section_1"},
		TokenCounts:       []int32{220},
		SequenceIndices:   []int64{1},
	}

	res, err := receiver.IngestRecordBatch(context.Background(), batch, 1)
	if err != nil {
		t.Fatalf("unexpected error during IngestRecordBatch: %v", err)
	}

	if !res.Success {
		t.Errorf("expected success true, got false")
	}
	if res.ChunksProcessed != 1 {
		t.Errorf("expected 1 chunk processed, got %d", res.ChunksProcessed)
	}
}
