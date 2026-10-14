package unit

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	chunkv1 "github.com/Gemejime/rag-chunk-lifecycle-service/api/proto/v1"
	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/application/command"
	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/application/query"
	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/domain/entity"
	grpcTransport "github.com/Gemejime/rag-chunk-lifecycle-service/internal/transport/grpc"
	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/transport/grpc/interceptor"
)

type mockIngestStream struct {
	grpc.ServerStream
	ctx      context.Context
	requests []*chunkv1.IngestChunkRequest
	index    int
	response *chunkv1.IngestChunkResponse
}

func (m *mockIngestStream) Context() context.Context {
	return m.ctx
}

func (m *mockIngestStream) Recv() (*chunkv1.IngestChunkRequest, error) {
	if m.index >= len(m.requests) {
		return nil, io.EOF
	}
	req := m.requests[m.index]
	m.index++
	return req, nil
}

func (m *mockIngestStream) SendAndClose(resp *chunkv1.IngestChunkResponse) error {
	m.response = resp
	return nil
}

func TestGRPCHandler_BatchIngestChunks_TokenBudgetBounds(t *testing.T) {
	ingestHandler := command.NewIngestChunkBatchHandler(nil, nil)
	handler := grpcTransport.NewChunkHandler(ingestHandler, nil, nil, nil)

	// Test chunk with 50 tokens (< 100)
	streamUnderflow := &mockIngestStream{
		ctx: context.Background(),
		requests: []*chunkv1.IngestChunkRequest{
			{
				BatchId: "batch-1",
				Chunk: &chunkv1.ChunkNode{
					ChunkId:        strings.Repeat("a", 64),
					TenantId:       "tenant-test",
					Content:        "Short text",
					TokenCount:     50, // underflow
					StructuralPath: "doc/sec_1/para_1",
				},
			},
		},
	}

	err := handler.BatchIngestChunks(streamUnderflow)
	if err == nil {
		t.Fatalf("Expected error for 50 tokens, got nil")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.InvalidArgument {
		t.Fatalf("Expected codes.InvalidArgument, got %s", st.Code())
	}

	// Test chunk with 350 tokens (> 300)
	streamOverflow := &mockIngestStream{
		ctx: context.Background(),
		requests: []*chunkv1.IngestChunkRequest{
			{
				BatchId: "batch-1",
				Chunk: &chunkv1.ChunkNode{
					ChunkId:        strings.Repeat("a", 64),
					TenantId:       "tenant-test",
					Content:        "Long text",
					TokenCount:     350, // overflow
					StructuralPath: "doc/sec_1/para_1",
				},
			},
		},
	}

	err = handler.BatchIngestChunks(streamOverflow)
	if err == nil {
		t.Fatalf("Expected error for 350 tokens, got nil")
	}
	st, _ = status.FromError(err)
	if st.Code() != codes.InvalidArgument {
		t.Fatalf("Expected codes.InvalidArgument, got %s", st.Code())
	}
}

func TestGRPCHandler_BatchIngestChunks_Backpressure(t *testing.T) {
	handler := grpcTransport.NewChunkHandler(nil, nil, nil, nil)
	// Saturate threshold to 0
	handler.SetMaxInFlightStreams(0)

	stream := &mockIngestStream{
		ctx: context.Background(),
	}

	err := handler.BatchIngestChunks(stream)
	if err == nil {
		t.Fatalf("Expected ResourceExhausted error, got nil")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.ResourceExhausted {
		t.Fatalf("Expected codes.ResourceExhausted, got %s", st.Code())
	}
}

func TestGRPCHandler_GetParentContainer(t *testing.T) {
	parentQueryHandler := query.NewGetParentContainerHandler(nil, nil)
	handler := grpcTransport.NewChunkHandler(nil, parentQueryHandler, nil, nil)
	ctx := context.Background()

	// Missing tenant ID
	_, err := handler.GetParentContainer(ctx, &chunkv1.GetParentContainerRequest{
		ParentId: "p-1",
	})
	if err == nil || status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Expected InvalidArgument for missing tenant, got %v", err)
	}

	// Missing parent ID
	_, err = handler.GetParentContainer(ctx, &chunkv1.GetParentContainerRequest{
		TenantId: "tenant-1",
	})
	if err == nil || status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Expected InvalidArgument for missing parent_id, got %v", err)
	}

	// Non-existent parent container returns NotFound
	_, err = handler.GetParentContainer(ctx, &chunkv1.GetParentContainerRequest{
		TenantId: "tenant-1",
		ParentId: "non-existent",
	})
	if err == nil || status.Code(err) != codes.NotFound {
		t.Fatalf("Expected NotFound for missing parent container, got %v", err)
	}
}

func TestGRPCHandler_ReconcileCDCUpdate(t *testing.T) {
	repo := newMockPatchRepo()
	reconcileHandler := command.NewReconcileCDCUpdateHandler(nil, repo, nil)
	handler := grpcTransport.NewChunkHandler(nil, nil, reconcileHandler, nil)
	ctx := context.Background()

	// Test Clean Promotion via gRPC
	resp, err := handler.ReconcileCDCUpdate(ctx, &chunkv1.ReconcileCDCRequest{
		TenantId:            "tenant-grpc",
		DocumentId:          "doc-grpc",
		SectionKey:          "doc-grpc/sec_1",
		UpstreamContent:     "Clean section content text",
		UpstreamContentHash: strings.Repeat("a", 64),
	})
	if err != nil {
		t.Fatalf("ReconcileCDCUpdate failed: %v", err)
	}
	if resp.GetQuarantined() {
		t.Fatalf("Expected Quarantined == false, got true")
	}

	// Test Quarantine via gRPC
	patch, _ := entity.NewSectionPatch(
		"patch-grpc-1",
		"tenant-grpc",
		"doc-grpc",
		"doc-grpc/sec_2",
		strings.Repeat("b", 64),
		"Patched override",
		"op-user",
		time.Now().UTC(),
	)
	_ = repo.Save(ctx, patch)

	resp, err = handler.ReconcileCDCUpdate(ctx, &chunkv1.ReconcileCDCRequest{
		TenantId:            "tenant-grpc",
		DocumentId:          "doc-grpc",
		SectionKey:          "doc-grpc/sec_2",
		UpstreamContent:     "Conflicting upstream text",
		UpstreamContentHash: strings.Repeat("c", 64), // Mismatch
	})
	if err != nil {
		t.Fatalf("ReconcileCDCUpdate failed: %v", err)
	}
	if !resp.GetQuarantined() {
		t.Fatalf("Expected Quarantined == true, got false")
	}
	if resp.GetPatchId() != "patch-grpc-1" {
		t.Fatalf("Expected patchID patch-grpc-1, got %s", resp.GetPatchId())
	}
}

func TestTenantInterceptor_Validation(t *testing.T) {
	interceptorFn := interceptor.TenantUnaryInterceptor()

	// Request without tenant should fail with InvalidArgument
	md := metadata.New(map[string]string{})
	ctx := metadata.NewIncomingContext(context.Background(), md)

	_, err := interceptorFn(ctx, "req", &grpc.UnaryServerInfo{FullMethod: "/chunk.v1.Service/Get"},
		func(ctx context.Context, req any) (any, error) {
			return "ok", nil
		},
	)
	if err == nil || status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Expected codes.InvalidArgument, got %v", err)
	}

	// Request with x-tenant-id metadata header should succeed
	mdWithTenant := metadata.New(map[string]string{"x-tenant-id": "tenant-corp-1"})
	ctxWithTenant := metadata.NewIncomingContext(context.Background(), mdWithTenant)

	res, err := interceptorFn(ctxWithTenant, "req", &grpc.UnaryServerInfo{FullMethod: "/chunk.v1.Service/Get"},
		func(ctx context.Context, req any) (any, error) {
			val := ctx.Value(interceptor.TenantContextKey)
			if val != "tenant-corp-1" {
				t.Fatalf("Expected tenant-corp-1 in context, got %v", val)
			}
			return "ok", nil
		},
	)
	if err != nil || res != "ok" {
		t.Fatalf("Expected interceptor to succeed, got %v", err)
	}
}
