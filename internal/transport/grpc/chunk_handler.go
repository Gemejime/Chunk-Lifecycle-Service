package grpc

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync/atomic"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	chunkv1 "github.com/Gemejime/rag-chunk-lifecycle-service/api/proto/v1"
	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/application/command"
	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/application/query"
	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/domain/entity"
	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/transport/grpc/interceptor"
)

var _ chunkv1.ChunkLifecycleServiceServer = (*ChunkHandler)(nil)

type chunkBatchIngestor interface {
	Execute(ctx context.Context, cmd command.IngestChunkBatchCommand) (*command.IngestChunkBatchResult, error)
}

// ChunkHandler implements the gRPC ChunkLifecycleServiceServer interface.
type ChunkHandler struct {
	chunkv1.UnimplementedChunkLifecycleServiceServer
	ingestHandler       *command.IngestChunkBatchHandler
	ingestExecutor      chunkBatchIngestor
	getParentHandler    *query.GetParentContainerHandler
	reconcileCDCHandler *command.ReconcileCDCUpdateHandler
	applyPatchHandler   *command.ApplySectionPatchHandler

	// In-flight backpressure saturation counter
	inFlightStreams    int64
	maxInFlightStreams int64
}

// NewChunkHandler creates a new ChunkHandler instance.
func NewChunkHandler(
	ingestHandler *command.IngestChunkBatchHandler,
	getParentHandler *query.GetParentContainerHandler,
	reconcileCDCHandler *command.ReconcileCDCUpdateHandler,
	applyPatchHandler *command.ApplySectionPatchHandler,
) *ChunkHandler {
	return &ChunkHandler{
		ingestHandler:       ingestHandler,
		ingestExecutor:      ingestHandler,
		getParentHandler:    getParentHandler,
		reconcileCDCHandler: reconcileCDCHandler,
		applyPatchHandler:   applyPatchHandler,
		maxInFlightStreams:  500, // Maximum concurrent streaming ingestion connections
	}
}

// SetIngestExecutorForTest overrides the chunk batch executor for testing error classification.
func (h *ChunkHandler) SetIngestExecutorForTest(executor chunkBatchIngestor) {
	h.ingestExecutor = executor
}

// SetMaxInFlightStreams configures the maximum concurrent streaming ingestion streams for backpressure testing.
func (h *ChunkHandler) SetMaxInFlightStreams(max int64) {
	h.maxInFlightStreams = max
}

// BatchIngestChunks accepts a client-side stream of chunk records and stages them.
// Invariants enforced:
// - Child chunk token count must be between 100 and 300 tokens (codes.InvalidArgument).
// - Tenant isolation is enforced via tenant_id on all records.
// - Under saturation, returns codes.ResourceExhausted backpressure.
func (h *ChunkHandler) BatchIngestChunks(stream chunkv1.ChunkLifecycleService_BatchIngestChunksServer) error {
	current := atomic.AddInt64(&h.inFlightStreams, 1)
	defer atomic.AddInt64(&h.inFlightStreams, -1)

	if current > h.maxInFlightStreams {
		return status.Error(codes.ResourceExhausted, "ingestion queue saturated; backpressure applied")
	}

	var batchID string
	var documentID string
	var tenantID string
	var manifestVersion int64
	var domainChunks []*entity.ChunkNode

	ctxTenant, _ := stream.Context().Value(interceptor.TenantContextKey).(string)
	ctxTenant = strings.TrimSpace(ctxTenant)

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return status.Errorf(codes.Unknown, "failed receiving stream chunk: %v", err)
		}

		if req == nil || req.GetChunk() == nil {
			return status.Error(codes.InvalidArgument, "request and chunk cannot be null")
		}

		c := req.GetChunk()
		cTenant := strings.TrimSpace(c.GetTenantId())
		cDoc := strings.TrimSpace(req.GetDocumentId())

		if ctxTenant != "" && cTenant != "" && cTenant != ctxTenant {
			return status.Errorf(codes.PermissionDenied, "chunk tenant %q does not match authenticated context tenant %q", cTenant, ctxTenant)
		}

		tokens := int(c.GetTokenCount())
		if tokens < entity.MinChunkTokenBudget || tokens > entity.MaxChunkTokenBudget {
			return status.Errorf(codes.InvalidArgument, "chunk %s token count %d violates budget bounds [100, 300]", c.GetChunkId(), tokens)
		}

		if batchID == "" {
			batchID = req.GetBatchId()
			documentID = cDoc
			tenantID = cTenant
			if tenantID == "" && ctxTenant != "" {
				tenantID = ctxTenant
			}
			manifestVersion = req.GetManifestVersion()
		} else {
			if cTenant != "" && tenantID != "" && cTenant != tenantID {
				return status.Errorf(codes.InvalidArgument, "chunk tenant %q differs from first chunk tenant %q", cTenant, tenantID)
			}
			if cDoc != "" && documentID != "" && cDoc != documentID {
				return status.Errorf(codes.InvalidArgument, "chunk document %q differs from first chunk document %q", cDoc, documentID)
			}
		}

		chunkNode, err := entity.NewChunkNode(
			c.GetChunkId(),
			c.GetTenantId(),
			c.GetParentId(),
			c.GetContent(),
			tokens,
			c.GetStructuralPath(),
			c.GetByteOffsetStart(),
			c.GetByteOffsetEnd(),
			c.GetMetadata(),
		)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid chunk node: %v", err)
		}

		domainChunks = append(domainChunks, chunkNode)
	}

	if len(domainChunks) == 0 {
		return status.Error(codes.InvalidArgument, "stream contained zero valid chunk nodes")
	}

	cmd := command.IngestChunkBatchCommand{
		TenantID:        tenantID,
		DocumentID:      documentID,
		ManifestVersion: manifestVersion,
		BatchID:         batchID,
		Chunks:          domainChunks,
	}

	executor := h.ingestExecutor
	if executor == nil {
		executor = h.ingestHandler
	}
	res, err := executor.Execute(stream.Context(), cmd)
	if err != nil {
		if errors.Is(err, entity.ErrTokenBudgetViolation) {
			return status.Errorf(codes.InvalidArgument, "token budget violation: %v", err)
		}
		if errors.Is(err, command.ErrPoolSaturation) {
			return status.Errorf(codes.ResourceExhausted, "resource exhausted: %v", err)
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return status.Errorf(codes.DeadlineExceeded, "batch ingestion deadline exceeded: %v", err)
		}
		return status.Errorf(codes.Internal, "batch ingestion execution failed: %v", err)
	}

	return stream.SendAndClose(&chunkv1.BatchIngestChunksResponse{
		BatchId:            res.BatchID,
		ChunksProcessed:    res.ChunksProcessed,
		ChunksDeduplicated: res.ChunksDeduplicated,
		ManifestVersion:    res.ManifestVersion,
		Success:            res.Success,
	})
}

// GetParentContainer retrieves a compiled parent context container adhering to the sub-8ms P99 target.
func (h *ChunkHandler) GetParentContainer(
	ctx context.Context,
	req *chunkv1.GetParentContainerRequest,
) (*chunkv1.GetParentContainerResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be null")
	}
	tenantID := strings.TrimSpace(req.GetTenantId())
	if tenantID == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id must not be empty")
	}
	parentID := strings.TrimSpace(req.GetParentId())
	if parentID == "" {
		return nil, status.Error(codes.InvalidArgument, "parent_id must not be empty")
	}

	container, err := h.getParentHandler.Execute(ctx, query.GetParentContainerQuery{
		TenantID:        tenantID,
		ParentID:        parentID,
		ManifestVersion: req.GetManifestVersion(),
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed retrieving parent container: %v", err)
	}
	if container == nil {
		return nil, status.Errorf(codes.NotFound, "parent container %s not found", parentID)
	}

	return &chunkv1.GetParentContainerResponse{
		Container: &chunkv1.ParentContainer{
			ParentId:        container.ParentID(),
			TenantId:        container.TenantID(),
			DocumentId:      container.DocumentID(),
			ManifestVersion: container.ManifestVersion(),
			HeadingPath:     container.HeadingPath(),
			Content:         container.Content(),
			TokenCount:      int32(container.TokenCount()),
			ChildChunkIds:   container.ChildChunkIDs(),
		},
	}, nil
}

// ReconcileCDCUpdate evaluates an incoming upstream CDC change against the section patch ledger under ADR-002.
func (h *ChunkHandler) ReconcileCDCUpdate(
	ctx context.Context,
	req *chunkv1.ReconcileCDCUpdateRequest,
) (*chunkv1.ReconcileCDCUpdateResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be null")
	}
	tenantID := strings.TrimSpace(req.GetTenantId())
	if tenantID == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id must not be empty")
	}
	documentID := strings.TrimSpace(req.GetDocumentId())
	if documentID == "" {
		return nil, status.Error(codes.InvalidArgument, "document_id must not be empty")
	}
	sectionKey := strings.TrimSpace(req.GetSectionKey())
	if sectionKey == "" {
		return nil, status.Error(codes.InvalidArgument, "section_key must not be empty")
	}

	res, err := h.reconcileCDCHandler.Execute(ctx, command.ReconcileCDCCommand{
		TenantID:            tenantID,
		DocumentID:          documentID,
		SectionKey:          sectionKey,
		UpstreamContent:     req.GetUpstreamContent(),
		UpstreamContentHash: req.GetUpstreamContentHash(),
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "reconciliation failed: %v", err)
	}

	return &chunkv1.ReconcileCDCUpdateResponse{
		Quarantined:        res.Quarantined,
		PatchId:            res.PatchID,
		Message:            res.Message,
		NewManifestVersion: res.NewManifestVersion,
	}, nil
}

// ApplySectionPatch applies an operator override to an identified section key.
func (h *ChunkHandler) ApplySectionPatch(
	ctx context.Context,
	req *chunkv1.ApplySectionPatchRequest,
) (*chunkv1.ApplySectionPatchResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be null")
	}
	tenantID := strings.TrimSpace(req.GetTenantId())
	if tenantID == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id must not be empty")
	}
	documentID := strings.TrimSpace(req.GetDocumentId())
	if documentID == "" {
		return nil, status.Error(codes.InvalidArgument, "document_id must not be empty")
	}
	sectionKey := strings.TrimSpace(req.GetSectionKey())
	if sectionKey == "" {
		return nil, status.Error(codes.InvalidArgument, "section_key must not be empty")
	}

	res, err := h.applyPatchHandler.Execute(ctx, command.ApplySectionPatchCommand{
		TenantID:         tenantID,
		DocumentID:       documentID,
		SectionKey:       sectionKey,
		BaseContentHash:  req.GetBaseContentHash(),
		PatchedContent:   req.GetPatchedContent(),
		OperatorID:       req.GetOperatorId(),
		ClientMutationID: req.GetClientMutationId(),
	})
	if err != nil {
		if errors.Is(err, entity.ErrStaleBaseHash) {
			return nil, status.Errorf(codes.FailedPrecondition, "optimistic concurrency violation: %v", err)
		}
		if errors.Is(err, entity.ErrEmptySectionKey) || errors.Is(err, entity.ErrEmptyBaseContentHash) {
			return nil, status.Errorf(codes.InvalidArgument, "invalid patch arguments: %v", err)
		}
		return nil, status.Errorf(codes.Internal, "patch application failed: %v", err)
	}

	var statusEnum chunkv1.PatchStatus
	switch res.Status {
	case entity.PatchStatusActive:
		statusEnum = chunkv1.PatchStatus_PATCH_STATUS_ACTIVE
	case entity.PatchStatusStaleQuarantine:
		statusEnum = chunkv1.PatchStatus_PATCH_STATUS_STALE_QUARANTINE
	case entity.PatchStatusSuperseded:
		statusEnum = chunkv1.PatchStatus_PATCH_STATUS_SUPERSEDED
	default:
		statusEnum = chunkv1.PatchStatus_PATCH_STATUS_UNSPECIFIED
	}

	return &chunkv1.ApplySectionPatchResponse{
		PatchId:                 res.PatchID,
		ManifestVersion:         res.ManifestVersion,
		Status:                  statusEnum,
		RecompiledParentIds:     res.RecompiledParentIDs,
		RevectorizationChunkIds: res.RevectorizationChunkIDs,
	}, nil
}
