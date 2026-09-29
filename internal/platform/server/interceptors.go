package server

import (
	"context"
	"log/slog"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/smallStepGiantLeap/ledger/internal/platform/metrics"
)

// exempt reports whether a method bypasses admission control and draining.
// Health checks must keep answering under overload: shedding them would mark
// the pod not-ready, push its load onto the others, and turn overload into a
// cascade. Reflection is operator tooling.
func exempt(fullMethod string) bool {
	return strings.HasPrefix(fullMethod, "/grpc.health.v1.Health/") ||
		strings.HasPrefix(fullMethod, "/grpc.reflection.")
}

// admission sheds load with RESOURCE_EXHAUSTED once a global in-flight limit
// is reached. Failing fast lets the client back off or go elsewhere; queueing
// would add latency to every call and grow memory without bound.
type admission struct {
	maxUnary, maxStreams int64
	unaryN, streamN      atomic.Int64
	m                    *metrics.Server
}

func newAdmission(maxUnary, maxStreams int, m *metrics.Server) *admission {
	return &admission{maxUnary: int64(maxUnary), maxStreams: int64(maxStreams), m: m}
}

func (a *admission) unary(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if a.maxUnary <= 0 || exempt(info.FullMethod) {
		return handler(ctx, req)
	}
	if a.unaryN.Add(1) > a.maxUnary {
		a.unaryN.Add(-1)
		a.m.Rejected("unary_limit")
		return nil, status.Error(codes.ResourceExhausted, "server at unary concurrency limit")
	}
	defer a.unaryN.Add(-1)
	return handler(ctx, req)
}

func (a *admission) stream(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if a.maxStreams <= 0 || exempt(info.FullMethod) {
		return handler(srv, ss)
	}
	if a.streamN.Add(1) > a.maxStreams {
		a.streamN.Add(-1)
		a.m.Rejected("stream_limit")
		return status.Error(codes.ResourceExhausted, "server at stream concurrency limit")
	}
	defer a.streamN.Add(-1)
	return handler(srv, ss)
}

// capDeadline gives every unary RPC a deadline no later than max from now,
// so a caller that sends no deadline cannot hold a handler forever.
func capDeadline(max time.Duration) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if max <= 0 {
			return handler(ctx, req)
		}
		if dl, ok := ctx.Deadline(); !ok || time.Until(dl) > max {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, max)
			defer cancel()
		}
		return handler(ctx, req)
	}
}

// recoverUnary and recoverStream turn a handler panic into INTERNAL for that
// one RPC instead of killing the process and every other RPC on it.
func recoverUnary(m *metrics.Server) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				err = panicked(m, info.FullMethod, r)
			}
		}()
		return handler(ctx, req)
	}
}

func recoverStream(m *metrics.Server) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = panicked(m, info.FullMethod, r)
			}
		}()
		return handler(srv, ss)
	}
}

func panicked(m *metrics.Server, method string, r any) error {
	m.Panic()
	slog.Error("handler panic", "method", method, "panic", r, "stack", string(debug.Stack()))
	return status.Error(codes.Internal, "internal error")
}
