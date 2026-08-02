package opentelemetry

import (
	"context"
	"crypto/tls"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	otelmetric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
)

const (
	transportMetricName = "proxy.outbound.transport.io"
	healthMetricName    = "proxy.outbound.health_check.latency"
	transportScopeName  = "proxy.outbound.transport"
	healthScopeName     = "proxy.outbound.health"
	metricScopeVersion  = "v1alpha1"
)

type healthPoint struct {
	outbound    adapter.OutboundIdentity
	url         string
	latencyMS   int64
	completedAt time.Time
}

type metricSink struct {
	provider  *sdkmetric.MeterProvider
	transport otelmetric.Int64Counter
	health    *healthQueue
}

func newMetricSink(ctx context.Context, config reporterConfig, res *resource.Resource, tlsConfig *tls.Config, tlsConfigured bool) (*metricSink, error) {
	exporterOptions := make([]otlpmetrichttp.Option, 0, 6)
	if config.metricsEndpoint != "" {
		exporterOptions = append(exporterOptions, otlpmetrichttp.WithEndpointURL(config.metricsEndpoint))
	} else if config.endpoint != "" {
		exporterOptions = append(exporterOptions, otlpmetrichttp.WithEndpointURL(signalEndpoint(config.endpoint, "metrics")))
	} else if os.Getenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT") == "" && os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		exporterOptions = append(exporterOptions, otlpmetrichttp.WithEndpointURL(signalEndpoint(defaultEndpoint, "metrics")))
	}
	if len(config.headers) > 0 {
		exporterOptions = append(exporterOptions, otlpmetrichttp.WithHeaders(config.headers))
	}
	if config.timeout > 0 {
		exporterOptions = append(exporterOptions, otlpmetrichttp.WithTimeout(config.timeout))
	}
	switch config.compression {
	case "gzip":
		exporterOptions = append(exporterOptions, otlpmetrichttp.WithCompression(otlpmetrichttp.GzipCompression))
	case "none":
		exporterOptions = append(exporterOptions, otlpmetrichttp.WithCompression(otlpmetrichttp.NoCompression))
	}
	if tlsConfigured {
		exporterOptions = append(exporterOptions, otlpmetrichttp.WithTLSClientConfig(tlsConfig))
	}

	exporter, err := otlpmetrichttp.New(ctx, exporterOptions...)
	if err != nil {
		return nil, err
	}
	health := newHealthQueue(config.metricMaxQueueSize)
	readerOptions := []sdkmetric.PeriodicReaderOption{sdkmetric.WithProducer(health)}
	if config.metricExportInterval > 0 {
		readerOptions = append(readerOptions, sdkmetric.WithInterval(config.metricExportInterval))
	}
	if config.metricExportTimeout > 0 {
		readerOptions = append(readerOptions, sdkmetric.WithTimeout(config.metricExportTimeout))
	}
	reader := sdkmetric.NewPeriodicReader(&acknowledgingMetricExporter{Exporter: exporter, health: health}, readerOptions...)
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithResource(res), sdkmetric.WithReader(reader))
	transport, err := provider.Meter(transportScopeName, otelmetric.WithInstrumentationVersion(metricScopeVersion)).Int64Counter(
		transportMetricName,
		otelmetric.WithUnit("By"),
		otelmetric.WithDescription("Bytes read from or written to an outbound transport socket."),
	)
	if err != nil {
		_ = provider.Shutdown(ctx)
		return nil, err
	}
	return &metricSink{provider: provider, transport: transport, health: health}, nil
}

func (s *metricSink) addTransport(outbound adapter.OutboundIdentity, network string, direction string, bytes int64) {
	if s == nil || bytes <= 0 || outbound.Name == "" {
		return
	}
	s.transport.Add(context.Background(), bytes, otelmetric.WithAttributes(
		attribute.String("proxy.outbound.name", outbound.Name),
		attribute.String("proxy.outbound.type", normalizeType(outbound.Type)),
		attribute.String("network.io.direction", direction),
		attribute.String("network.transport", network),
	))
}

func (s *metricSink) recordHealth(point healthPoint) {
	if s == nil {
		return
	}
	point.outbound.Type = normalizeType(point.outbound.Type)
	s.health.add(point)
}

func (s *metricSink) shutdown(ctx context.Context) error {
	if s == nil {
		return nil
	}
	return s.provider.Shutdown(ctx)
}

type healthQueue struct {
	access sync.Mutex
	max    int
	points []healthPoint
}

func newHealthQueue(maximum int) *healthQueue {
	return &healthQueue{max: maximum}
}

func (q *healthQueue) add(point healthPoint) {
	q.access.Lock()
	defer q.access.Unlock()
	if q.max > 0 && len(q.points) >= q.max {
		copy(q.points, q.points[len(q.points)-q.max+1:])
		q.points = q.points[:q.max-1]
	}
	q.points = append(q.points, point)
}

func (q *healthQueue) Produce(context.Context) ([]metricdata.ScopeMetrics, error) {
	q.access.Lock()
	points := append([]healthPoint(nil), q.points...)
	q.access.Unlock()
	if len(points) == 0 {
		return nil, nil
	}
	sort.SliceStable(points, func(i, j int) bool {
		return points[i].completedAt.Before(points[j].completedAt)
	})
	dataPoints := make([]metricdata.DataPoint[int64], 0, len(points))
	for _, point := range points {
		dataPoints = append(dataPoints, metricdata.DataPoint[int64]{
			Attributes: attribute.NewSet(
				attribute.String("proxy.outbound.name", point.outbound.Name),
				attribute.String("proxy.outbound.type", point.outbound.Type),
				attribute.String("url.full", point.url),
			),
			Time:  point.completedAt,
			Value: point.latencyMS,
		})
	}
	return []metricdata.ScopeMetrics{{
		Scope: instrumentation.Scope{Name: healthScopeName, Version: metricScopeVersion},
		Metrics: []metricdata.Metrics{{
			Name:        healthMetricName,
			Description: "Successful outbound URL test latency.",
			Unit:        "ms",
			Data:        metricdata.Gauge[int64]{DataPoints: dataPoints},
		}},
	}}, nil
}

func (q *healthQueue) acknowledge(metrics *metricdata.ResourceMetrics) {
	var exported []healthPoint
	for _, scope := range metrics.ScopeMetrics {
		for _, metric := range scope.Metrics {
			if metric.Name != healthMetricName {
				continue
			}
			gauge, loaded := metric.Data.(metricdata.Gauge[int64])
			if !loaded {
				continue
			}
			for _, point := range gauge.DataPoints {
				exported = append(exported, healthPoint{
					outbound: adapter.OutboundIdentity{
						Name: attributeString(point.Attributes, "proxy.outbound.name"),
						Type: attributeString(point.Attributes, "proxy.outbound.type"),
					},
					url:         attributeString(point.Attributes, "url.full"),
					latencyMS:   point.Value,
					completedAt: point.Time,
				})
			}
		}
	}
	if len(exported) == 0 {
		return
	}
	q.access.Lock()
	defer q.access.Unlock()
	for _, exportedPoint := range exported {
		for index, queuedPoint := range q.points {
			if queuedPoint == exportedPoint {
				q.points = append(q.points[:index], q.points[index+1:]...)
				break
			}
		}
	}
}

func attributeString(attributes attribute.Set, key string) string {
	value, loaded := attributes.Value(attribute.Key(key))
	if !loaded {
		return ""
	}
	return value.AsString()
}

type acknowledgingMetricExporter struct {
	sdkmetric.Exporter
	health *healthQueue
}

func (e *acknowledgingMetricExporter) Export(ctx context.Context, metrics *metricdata.ResourceMetrics) error {
	err := e.Exporter.Export(ctx, metrics)
	if err == nil {
		e.health.acknowledge(metrics)
	}
	return err
}
