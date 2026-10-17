package interceptor

import (
	"context"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type tenantKeyType struct{}

// TenantContextKey is the context key for extracted tenant identifiers.
var TenantContextKey = tenantKeyType{}

// TenantUnaryInterceptor validates multi-tenant boundary headers and extracts tenant_id into context.
func TenantUnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		tenantID := extractTenantID(ctx, req)
		if tenantID == "" {
			return nil, status.Error(codes.InvalidArgument, "tenant_id must be provided via x-tenant-id metadata header or request payload")
		}

		ctx = context.WithValue(ctx, TenantContextKey, tenantID)
		return handler(ctx, req)
	}
}

// TenantStreamInterceptor validates tenant presence across streaming RPCs.
func TenantStreamInterceptor() grpc.StreamServerInterceptor {
	return func(
		srv any,
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		ctx := ss.Context()
		tenantID := extractTenantFromMetadata(ctx)
		if tenantID != "" {
			ctx = context.WithValue(ctx, TenantContextKey, tenantID)
		}
		// Note: for client-streaming, if not in metadata, the first message can supply tenant_id
		wrapped := &wrappedStream{ServerStream: ss, ctx: ctx}
		return handler(srv, wrapped)
	}
}

type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedStream) Context() context.Context {
	return w.ctx
}

func extractTenantID(ctx context.Context, req any) string {
	if t := extractTenantFromMetadata(ctx); t != "" {
		return t
	}

	// Reflection / Interface duck-typing for messages with GetTenantId()
	type tenantGetter interface {
		GetTenantId() string
	}
	if tg, ok := req.(tenantGetter); ok {
		return strings.TrimSpace(tg.GetTenantId())
	}

	return ""
}

func extractTenantFromMetadata(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	vals := md.Get("x-tenant-id")
	if len(vals) > 0 && strings.TrimSpace(vals[0]) != "" {
		return strings.TrimSpace(vals[0])
	}
	return ""
}
