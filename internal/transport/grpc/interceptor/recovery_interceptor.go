package interceptor

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RecoveryUnaryInterceptor catches panics, emits sanitized structured error logs, and returns codes.Internal.
func RecoveryUnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				stack := string(debug.Stack())
				slog.Default().ErrorContext(ctx, "grpc unary handler recovered from panic",
					slog.String("panic", fmt.Sprintf("%v", r)),
					slog.String("stack_trace", stack),
					slog.String("method", info.FullMethod),
				)
				err = status.Error(codes.Internal, "an unexpected internal server error occurred")
			}
		}()
		return handler(ctx, req)
	}
}

// RecoveryStreamInterceptor catches panics in streaming RPC handlers.
func RecoveryStreamInterceptor() grpc.StreamServerInterceptor {
	return func(
		srv any,
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) (err error) {
		defer func() {
			if r := recover(); r != nil {
				stack := string(debug.Stack())
				slog.Default().ErrorContext(ss.Context(), "grpc stream handler recovered from panic",
					slog.String("panic", fmt.Sprintf("%v", r)),
					slog.String("stack_trace", stack),
					slog.String("method", info.FullMethod),
				)
				err = status.Error(codes.Internal, "an unexpected internal server error occurred")
			}
		}()
		return handler(srv, ss)
	}
}
