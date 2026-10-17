package interceptor

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type traceKeyType struct{}
type requestKeyType struct{}

var (
	// TraceContextKey is the context key for extracted or generated distributed trace identifiers.
	TraceContextKey = traceKeyType{}
	// RequestContextKey is the context key for extracted or generated request identifiers.
	RequestContextKey = requestKeyType{}
)

// LoggingUnaryInterceptor logs every unary gRPC request adhering strictly to Secured Logging invariants.
func LoggingUnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		start := time.Now()
		traceID, requestID, tenantID := extractCorrelationMetadata(ctx)

		ctx = context.WithValue(ctx, TraceContextKey, traceID)
		ctx = context.WithValue(ctx, RequestContextKey, requestID)

		logger := slog.Default().With(
			slog.String("trace_id", traceID),
			slog.String("request_id", requestID),
			slog.String("tenant_id", tenantID),
			slog.String("grpc_method", info.FullMethod),
		)

		resp, err := handler(ctx, req)
		duration := time.Since(start)

		st, _ := status.FromError(err)
		logAttrs := []any{
			slog.Duration("latency", duration),
			slog.String("grpc_code", st.Code().String()),
		}

		if err != nil {
			logger.ErrorContext(ctx, "grpc request failed", append(logAttrs, slog.String("error_message", st.Message()))...)
		} else {
			logger.InfoContext(ctx, "grpc request completed", logAttrs...)
		}

		return resp, err
	}
}

// LoggingStreamInterceptor logs streaming RPC connections with correlated metadata.
func LoggingStreamInterceptor() grpc.StreamServerInterceptor {
	return func(
		srv any,
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		start := time.Now()
		ctx := ss.Context()
		traceID, requestID, tenantID := extractCorrelationMetadata(ctx)

		ctx = context.WithValue(ctx, TraceContextKey, traceID)
		ctx = context.WithValue(ctx, RequestContextKey, requestID)

		logger := slog.Default().With(
			slog.String("trace_id", traceID),
			slog.String("request_id", requestID),
			slog.String("tenant_id", tenantID),
			slog.String("grpc_method", info.FullMethod),
		)

		wrapped := &wrappedStream{ServerStream: ss, ctx: ctx}
		err := handler(srv, wrapped)
		duration := time.Since(start)

		st, _ := status.FromError(err)
		logAttrs := []any{
			slog.Duration("latency", duration),
			slog.String("grpc_code", st.Code().String()),
		}

		if err != nil {
			logger.ErrorContext(ctx, "grpc stream terminated with error", append(logAttrs, slog.String("error_message", st.Message()))...)
		} else {
			logger.InfoContext(ctx, "grpc stream completed successfully", logAttrs...)
		}

		return err
	}
}

func extractCorrelationMetadata(ctx context.Context) (traceID, requestID, tenantID string) {
	if tVal, ok := ctx.Value(TenantContextKey).(string); ok {
		tenantID = tVal
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return uuid.New().String(), uuid.New().String(), tenantID
	}

	if traces := md.Get("x-trace-id"); len(traces) > 0 && strings.TrimSpace(traces[0]) != "" {
		traceID = strings.TrimSpace(traces[0])
	} else {
		traceID = uuid.New().String()
	}

	if reqs := md.Get("x-request-id"); len(reqs) > 0 && strings.TrimSpace(reqs[0]) != "" {
		requestID = strings.TrimSpace(reqs[0])
	} else {
		requestID = uuid.New().String()
	}

	if tenantID == "" {
		if tenants := md.Get("x-tenant-id"); len(tenants) > 0 && strings.TrimSpace(tenants[0]) != "" {
			tenantID = strings.TrimSpace(tenants[0])
		}
	}

	return traceID, requestID, tenantID
}
