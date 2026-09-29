package server

import (
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// LivenessService is the health key the liveness probe asks about. The drain
// never sets it to NOT_SERVING: a draining process is not a dead one.
const LivenessService = "liveness"

// Shutdown is the drain sequence run on SIGTERM:
//
//  1. readiness ("" and each service) reports NOT_SERVING, so probes and
//     clients doing client-side health checks stop sending new work;
//  2. wait DrainDelay while the pod leaves its endpoints;
//  3. GracefulStop: GOAWAY on every connection, so new RPCs go elsewhere;
//  4. Drain: end open streams, spread over the Drainer's Spread;
//  5. after Timeout, Stop cuts whatever is left.
//
// DrainDelay + Timeout must be less than terminationGracePeriodSeconds, or
// the kubelet's SIGKILL comes first.
type Shutdown struct {
	GRPC          *grpc.Server
	Health        *health.Server
	ReadinessKeys []string
	Drainer       *Drainer
	DrainDelay    time.Duration
	Timeout       time.Duration
	InFlight      func() int64
	Log           *slog.Logger
}

// Run blocks until the server has stopped.
func (s Shutdown) Run() {
	log := s.Log
	if log == nil {
		log = slog.Default()
	}
	log.Info("shutdown: signal received", "inflight_rpcs", s.InFlight())
	for _, k := range s.ReadinessKeys {
		s.Health.SetServingStatus(k, healthpb.HealthCheckResponse_NOT_SERVING)
	}
	log.Info("shutdown: readiness NOT_SERVING")
	time.Sleep(s.DrainDelay)

	// GOAWAY first, then end streams: a client resuming after its stream
	// ends must not be able to reopen it on this server's connection.
	stopped := make(chan struct{})
	go func() { s.GRPC.GracefulStop(); close(stopped) }()
	log.Info("shutdown: GracefulStop started (GOAWAY sent)", "inflight_rpcs", s.InFlight())
	s.Drainer.Drain()

	select {
	case <-stopped:
		log.Info("shutdown: GracefulStop complete")
	case <-time.After(s.Timeout):
		log.Warn("shutdown: timeout, forcing Stop", "inflight_rpcs", s.InFlight())
		s.GRPC.Stop()
		<-stopped
	}
}
