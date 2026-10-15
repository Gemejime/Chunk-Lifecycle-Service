package flight

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"

	"github.com/google/uuid"

	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/application/command"
	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/domain/compiler"
	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/domain/entity"
)

var (
	// ErrMismatchedColumnLengths indicates columnar arrays in RecordBatch have differing lengths.
	ErrMismatchedColumnLengths = errors.New("flight record batch column lengths do not match")
	// ErrEmptyBatch indicates the provided RecordBatch has zero rows.
	ErrEmptyBatch = errors.New("flight record batch is empty")
	// ErrBackpressureSaturated indicates the flight receiver is currently at concurrency limit.
	ErrBackpressureSaturated = errors.New("flight receiver backpressure capacity saturated")
)

// ColumnarRecordBatch represents a columnar batch of document chunks adhering to Apache Arrow Flight schema.
// Columnar Schema:
// - tenant_id: UTF-8 String Array
// - document_id: UTF-8 String Array
// - normalized_content: UTF-8 String Array
// - structural_path: UTF-8 String Array
// - token_count: Int32 Array
// - sequence_index: Int64 Array
type ColumnarRecordBatch struct {
	TenantIDs         []string
	DocumentIDs       []string
	NormalizedContent []string
	StructuralPaths   []string
	TokenCounts       []int32
	SequenceIndices   []int64
}

// NumRows returns the row count of the columnar batch.
func (b *ColumnarRecordBatch) NumRows() int {
	return len(b.NormalizedContent)
}

// Validate verifies that all columns have identical row counts and non-empty bounds.
func (b *ColumnarRecordBatch) Validate() error {
	rows := b.NumRows()
	if rows == 0 {
		return ErrEmptyBatch
	}
	if len(b.TenantIDs) != rows ||
		len(b.DocumentIDs) != rows ||
		len(b.StructuralPaths) != rows ||
		len(b.TokenCounts) != rows ||
		len(b.SequenceIndices) != rows {
		return ErrMismatchedColumnLengths
	}
	return nil
}

// FlightDataMessage represents a binary Arrow Flight IPC stream chunk (FlightData).
type FlightDataMessage struct {
	FlightDescriptor []byte
	DataHeader       []byte
	AppMetadata      []byte
	DataBody         []byte
}

// FlightStreamReader abstracts incoming FlightData stream sources.
type FlightStreamReader interface {
	Recv() (*FlightDataMessage, error)
	Context() context.Context
}

// FlightIngestResult captures the execution outcome of an Arrow Flight batch ingestion stream.
type FlightIngestResult struct {
	BatchID            string `json:"batch_id"`
	ChunksProcessed    int32  `json:"chunks_processed"`
	ChunksDeduplicated int32  `json:"chunks_deduplicated"`
	ManifestVersion    int64  `json:"manifest_version"`
	Success            bool   `json:"success"`
}

// FlightReceiver handles high-throughput columnar ingestion streams via Apache Arrow Flight (ADR-001, ADR-003).
// Guarantees pure-Go execution without cgo (CGO_ENABLED=0 compatible).
type FlightReceiver struct {
	ingestHandler     *command.IngestChunkBatchHandler
	hasher            *compiler.ContentHasher
	inFlightBatches   int64
	maxInFlightLimits int64
}

// NewFlightReceiver instantiates a pure-Go Arrow Flight receiver adapter.
func NewFlightReceiver(ingestHandler *command.IngestChunkBatchHandler) *FlightReceiver {
	return &FlightReceiver{
		ingestHandler:     ingestHandler,
		hasher:            compiler.NewContentHasher(),
		maxInFlightLimits: 500, // Safe default stream concurrency limit
	}
}

// SetMaxInFlightLimits configures the maximum concurrent Flight streams for backpressure testing.
func (r *FlightReceiver) SetMaxInFlightLimits(limit int64) {
	r.maxInFlightLimits = limit
}

// InFlightCount returns current concurrent active streams.
func (r *FlightReceiver) InFlightCount() int64 {
	return atomic.LoadInt64(&r.inFlightBatches)
}

// DecodeRecordBatch unrolls columnar Arrow arrays into domain entity.ChunkNode aggregates.
// Performs deterministic ADR-001 content hashing and enforces child token budget bounds [100, 300].
func (r *FlightReceiver) DecodeRecordBatch(batch *ColumnarRecordBatch) ([]*entity.ChunkNode, error) {
	if err := batch.Validate(); err != nil {
		return nil, err
	}

	rows := batch.NumRows()
	chunkNodes := make([]*entity.ChunkNode, 0, rows)

	for i := 0; i < rows; i++ {
		tenantID := strings.TrimSpace(batch.TenantIDs[i])
		if tenantID == "" {
			return nil, entity.ErrEmptyTenantID
		}
		docID := strings.TrimSpace(batch.DocumentIDs[i])
		if docID == "" {
			return nil, entity.ErrEmptyDocumentID
		}

		rawContent := batch.NormalizedContent[i]
		normalized := r.hasher.NormalizeContent(rawContent)
		structuralPath := strings.TrimSpace(batch.StructuralPaths[i])
		if structuralPath == "" {
			return nil, entity.ErrInvalidStructuralPath
		}

		// Compute deterministic SHA-256 ChunkID adhering to ADR-001
		chunkID := r.hasher.ComputeChunkID(tenantID, normalized, structuralPath)

		tokens := int(batch.TokenCounts[i])
		if tokens < entity.MinChunkTokenBudget || tokens > entity.MaxChunkTokenBudget {
			return nil, fmt.Errorf("%w: chunk at index %d has %d tokens", entity.ErrTokenBudgetViolation, i, tokens)
		}

		seqIdx := batch.SequenceIndices[i]
		metadata := map[string]string{
			"sequence_index": fmt.Sprintf("%d", seqIdx),
			"document_id":    docID,
			"source":         "arrow_flight_columnar",
		}

		node, err := entity.NewChunkNode(
			chunkID,
			tenantID,
			"", // ParentID compiled subsequently
			normalized,
			tokens,
			structuralPath,
			0,
			int64(len(normalized)),
			metadata,
		)
		if err != nil {
			return nil, fmt.Errorf("failed constructing chunk node at index %d: %w", i, err)
		}

		chunkNodes = append(chunkNodes, node)
	}

	return chunkNodes, nil
}

// IngestRecordBatch translates and executes promotion for a columnar batch.
func (r *FlightReceiver) IngestRecordBatch(
	ctx context.Context,
	batch *ColumnarRecordBatch,
	manifestVersion int64,
) (*FlightIngestResult, error) {
	current := atomic.AddInt64(&r.inFlightBatches, 1)
	defer atomic.AddInt64(&r.inFlightBatches, -1)

	if current > r.maxInFlightLimits {
		return nil, ErrBackpressureSaturated
	}

	chunks, err := r.DecodeRecordBatch(batch)
	if err != nil {
		return nil, err
	}

	batchID := uuid.New().String()
	tenantID := batch.TenantIDs[0]
	documentID := batch.DocumentIDs[0]

	cmd := command.IngestChunkBatchCommand{
		TenantID:        tenantID,
		DocumentID:      documentID,
		ManifestVersion: manifestVersion,
		BatchID:         batchID,
		Chunks:          chunks,
		TraceID:         "flight-" + batchID[:8],
		RequestID:       batchID,
	}

	slog.InfoContext(ctx, "executing flight columnar batch ingestion",
		slog.String("tenant_id", tenantID),
		slog.String("document_id", documentID),
		slog.String("batch_id", batchID),
		slog.Int("rows", len(chunks)),
	)

	result, err := r.ingestHandler.Execute(ctx, cmd)
	if err != nil {
		return nil, fmt.Errorf("flight batch promotion failed: %w", err)
	}

	return &FlightIngestResult{
		BatchID:            result.BatchID,
		ChunksProcessed:    result.ChunksProcessed,
		ChunksDeduplicated: result.ChunksDeduplicated,
		ManifestVersion:    result.ManifestVersion,
		Success:            result.Success,
	}, nil
}

// DecodeIPCStreamMessage decodes an Apache Arrow Flight IPC DataBody buffer into a ColumnarRecordBatch.
// Binary format: [4-byte row count] followed by length-prefixed columnar slices.
func (r *FlightReceiver) DecodeIPCStreamMessage(data []byte) (*ColumnarRecordBatch, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("insufficient flight payload bytes: %d", len(data))
	}

	reader := bytes.NewReader(data)
	var rowCount int32
	if err := binary.Read(reader, binary.BigEndian, &rowCount); err != nil {
		return nil, fmt.Errorf("failed reading row count header: %w", err)
	}
	if rowCount <= 0 {
		return nil, ErrEmptyBatch
	}

	readStringSlice := func() ([]string, error) {
		res := make([]string, rowCount)
		for i := 0; i < int(rowCount); i++ {
			var length int32
			if err := binary.Read(reader, binary.BigEndian, &length); err != nil {
				return nil, err
			}
			buf := make([]byte, length)
			if _, err := io.ReadFull(reader, buf); err != nil {
				return nil, err
			}
			res[i] = string(buf)
		}
		return res, nil
	}

	readInt32Slice := func() ([]int32, error) {
		res := make([]int32, rowCount)
		for i := 0; i < int(rowCount); i++ {
			if err := binary.Read(reader, binary.BigEndian, &res[i]); err != nil {
				return nil, err
			}
		}
		return res, nil
	}

	readInt64Slice := func() ([]int64, error) {
		res := make([]int64, rowCount)
		for i := 0; i < int(rowCount); i++ {
			if err := binary.Read(reader, binary.BigEndian, &res[i]); err != nil {
				return nil, err
			}
		}
		return res, nil
	}

	tenantIDs, err := readStringSlice()
	if err != nil {
		return nil, fmt.Errorf("failed decoding tenant_ids: %w", err)
	}
	docIDs, err := readStringSlice()
	if err != nil {
		return nil, fmt.Errorf("failed decoding document_ids: %w", err)
	}
	content, err := readStringSlice()
	if err != nil {
		return nil, fmt.Errorf("failed decoding normalized_content: %w", err)
	}
	paths, err := readStringSlice()
	if err != nil {
		return nil, fmt.Errorf("failed decoding structural_paths: %w", err)
	}
	tokens, err := readInt32Slice()
	if err != nil {
		return nil, fmt.Errorf("failed decoding token_counts: %w", err)
	}
	seqs, err := readInt64Slice()
	if err != nil {
		return nil, fmt.Errorf("failed decoding sequence_indices: %w", err)
	}

	return &ColumnarRecordBatch{
		TenantIDs:         tenantIDs,
		DocumentIDs:       docIDs,
		NormalizedContent: content,
		StructuralPaths:   paths,
		TokenCounts:       tokens,
		SequenceIndices:   seqs,
	}, nil
}
