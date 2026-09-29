// Package metrics instruments the gRPC server with the platform's standard
// Prometheus metrics. Every service on the platform exports the same names,
// so one autoscaling query and one dashboard fit all of them.
package metrics

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

// Server holds the server-side metrics.
type Server struct {
	inflight       *prometheus.GaugeVec
	unaryLatency   *prometheus.HistogramVec
	streamDuration *prometheus.HistogramVec
	handled        *prometheus.CounterVec
	rejected       *prometheus.CounterVec
	panics         prometheus.Counter
	drained        prometheus.Counter
	openConns      prometheus.Gauge
	connsTotal     prometheus.Counter

	inflightN atomic.Int64
}

// NewServer registers the metrics, plus Go runtime and process collectors.
func NewServer(reg *prometheus.Registry, version string) *Server {
	m := &Server{
		// The autoscaling signal: sum(vikrant_inflight_rpcs) per service.
		inflight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "vikrant_inflight_rpcs",
			Help: "RPCs currently inside a handler, by type (unary|stream).",
		}, []string{"type"}),
		unaryLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "vikrant_unary_latency_seconds",
			Help:    "Unary handler latency, measured inside the server.",
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 18),
		}, []string{"method", "code"}),
		streamDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "vikrant_stream_duration_seconds",
			Help:    "Lifetime of streaming RPCs.",
			Buckets: []float64{0.1, 0.5, 1, 5, 10, 30, 60, 300, 600, 1800, 3600},
		}, []string{"method", "code"}),
		handled: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "vikrant_rpcs_handled_total",
			Help: "RPCs completed, by method, type and gRPC status code.",
		}, []string{"method", "type", "code"}),
		rejected: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "vikrant_rejected_total",
			Help: "RPCs refused by admission control, by reason.",
		}, []string{"reason"}),
		panics: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "vikrant_panics_total",
			Help: "Handler panics recovered and returned as INTERNAL.",
		}),
		drained: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "vikrant_drained_streams_total",
			Help: "Streams ended with UNAVAILABLE by the shutdown drain, for clients to reconnect elsewhere.",
		}),
		openConns: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "vikrant_open_connections",
			Help: "HTTP/2 connections currently open.",
		}),
		connsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "vikrant_connections_total",
			Help: "HTTP/2 connections accepted since start.",
		}),
	}
	build := prometheus.NewGauge(prometheus.GaugeOpts{
		Name:        "vikrant_build_info",
		Help:        "Always 1; the version label is the running version.",
		ConstLabels: prometheus.Labels{"version": version},
	})
	build.Set(1)
	// Create the in-flight series at 0 so the autoscaler's query has data
	// before the first RPC.
	m.inflight.WithLabelValues("unary")
	m.inflight.WithLabelValues("stream")
	reg.MustRegister(m.inflight, m.unaryLatency, m.streamDuration, m.handled, m.rejected,
		m.panics, m.drained, m.openConns, m.connsTotal, build,
		collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return m
}

// InFlight returns the number of RPCs inside a handler.
func (m *Server) InFlight() int64 { return m.inflightN.Load() }

// Rejected counts an RPC refused by admission control.
func (m *Server) Rejected(reason string) { m.rejected.WithLabelValues(reason).Inc() }

// Panic counts a recovered handler panic.
func (m *Server) Panic() { m.panics.Inc() }

// Drained counts a stream ended by the shutdown drain.
func (m *Server) Drained() { m.drained.Inc() }

// UnaryInterceptor records latency and outcome of unary RPCs.
func (m *Server) UnaryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	g := m.inflight.WithLabelValues("unary")
	g.Inc()
	m.inflightN.Add(1)
	defer func() { g.Dec(); m.inflightN.Add(-1) }()

	start := time.Now()
	resp, err := handler(ctx, req)
	code := status.Code(err).String()
	m.unaryLatency.WithLabelValues(info.FullMethod, code).Observe(time.Since(start).Seconds())
	m.handled.WithLabelValues(info.FullMethod, "unary", code).Inc()
	return resp, err
}

// StreamInterceptor records in-flight count, lifetime and outcome of streams.
func (m *Server) StreamInterceptor(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	g := m.inflight.WithLabelValues("stream")
	g.Inc()
	m.inflightN.Add(1)
	defer func() { g.Dec(); m.inflightN.Add(-1) }()

	start := time.Now()
	err := handler(srv, ss)
	code := status.Code(err).String()
	m.streamDuration.WithLabelValues(info.FullMethod, code).Observe(time.Since(start).Seconds())
	m.handled.WithLabelValues(info.FullMethod, "stream", code).Inc()
	return err
}

// ConnStatsHandler counts transport connections; interceptors see only RPCs.
func (m *Server) ConnStatsHandler() stats.Handler { return connStats{m} }

type connStats struct{ m *Server }

func (connStats) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context   { return ctx }
func (connStats) HandleRPC(context.Context, stats.RPCStats)                         {}
func (connStats) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context { return ctx }

func (c connStats) HandleConn(_ context.Context, s stats.ConnStats) {
	switch s.(type) {
	case *stats.ConnBegin:
		c.m.openConns.Inc()
		c.m.connsTotal.Inc()
	case *stats.ConnEnd:
		c.m.openConns.Dec()
	}
}
