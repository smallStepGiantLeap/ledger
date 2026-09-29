package server

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var errDraining = errors.New("server draining")

// Drainer ends long-lived streams when the server shuts down, so clients
// reconnect to another replica instead of holding this one until the hard
// Stop. GOAWAY alone cannot do this: it stops new RPCs on a connection but
// lets existing streams run.
//
// After Drain, each stream's context is cancelled at a random point within
// Spread, so reconnects reach the other replicas spread out rather than as
// one burst. A handler that returns when stream.Context() is done drains
// without any code of its own; the client sees UNAVAILABLE and reconnects.
type Drainer struct {
	Spread    time.Duration
	OnDrained func() // optional, for metrics

	ctx  context.Context
	stop context.CancelFunc
}

// NewDrainer returns a Drainer that spreads stream endings over spread.
func NewDrainer(spread time.Duration, onDrained func()) *Drainer {
	ctx, stop := context.WithCancel(context.Background())
	return &Drainer{Spread: spread, OnDrained: onDrained, ctx: ctx, stop: stop}
}

// Drain starts ending streams. Safe to call more than once.
func (d *Drainer) Drain() { d.stop() }

// StreamInterceptor gives each stream a context that the drain cancels. It
// costs nothing until Drain is called: no goroutine per stream.
func (d *Drainer) StreamInterceptor(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if exempt(info.FullMethod) {
		return handler(srv, ss)
	}
	ctx, cancel := context.WithCancelCause(ss.Context())
	defer cancel(nil)
	stop := context.AfterFunc(d.ctx, func() {
		time.AfterFunc(jitter(d.Spread), func() { cancel(errDraining) })
	})
	defer stop()

	err := handler(srv, &drainStream{ServerStream: ss, ctx: ctx})
	if errors.Is(context.Cause(ctx), errDraining) {
		if d.OnDrained != nil {
			d.OnDrained()
		}
		return status.Error(codes.Unavailable, "server draining: reconnect to another replica")
	}
	return err
}

type drainStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *drainStream) Context() context.Context { return s.ctx }

func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	return rand.N(d)
}
