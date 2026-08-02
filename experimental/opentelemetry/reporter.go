package opentelemetry

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	boxLog "github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	otellog "go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	"golang.org/x/net/http/httpguts"
)

const (
	eventName       = "proxy.flow"
	scopeVersion    = "v1alpha2"
	schemaVersion   = "v1alpha2"
	defaultEndpoint = "http://127.0.0.1:4318"
	defaultActive   = time.Minute
	minimumActive   = 10 * time.Second
	maximumActive   = 24 * time.Hour
)

type reporterConfig struct {
	endpoint             string
	logsEndpoint         string
	metricsEndpoint      string
	protocol             string
	headers              map[string]string
	compression          string
	timeout              time.Duration
	activeTimeout        time.Duration
	batchScheduleDelay   time.Duration
	batchExportTimeout   time.Duration
	batchMaxQueueSize    int
	batchMaxExportSize   int
	metricExportInterval time.Duration
	metricExportTimeout  time.Duration
	metricMaxQueueSize   int
	tls                  option.OpenTelemetryTLSOptions
	resourceAttributes   map[string]string
}

type eventSink interface {
	emit(segment flowSegment)
	addTransport(outbound adapter.OutboundIdentity, network string, direction string, bytes int64)
	recordHealth(point healthPoint)
	shutdown(ctx context.Context) error
}

type Reporter struct {
	logger          boxLog.ContextLogger
	outboundManager adapter.OutboundManager
	config          reporterConfig
	sink            eventSink

	access  sync.RWMutex
	flows   map[*Flow]struct{}
	started atomic.Bool
	closed  atomic.Bool
	stop    chan struct{}
	done    chan struct{}
}

func SDKDisabled() bool {
	return strings.EqualFold(os.Getenv("OTEL_SDK_DISABLED"), "true")
}

func New(
	ctx context.Context,
	logger boxLog.ContextLogger,
	options option.OpenTelemetryOptions,
	outboundManager adapter.OutboundManager,
) (*Reporter, error) {
	installDiagnostics(logger)
	config, err := resolveConfig(options)
	if err != nil {
		return nil, err
	}
	if config.tls.InsecureSkipVerify {
		logger.Warn("OpenTelemetry TLS certificate verification is disabled")
	}
	sink, err := newOTelSink(ctx, config)
	if err != nil {
		return nil, err
	}
	return &Reporter{
		logger:          logger,
		outboundManager: outboundManager,
		config:          config,
		sink:            sink,
		flows:           make(map[*Flow]struct{}),
		stop:            make(chan struct{}),
		done:            make(chan struct{}),
	}, nil
}

func (r *Reporter) Name() string { return "opentelemetry" }

func (r *Reporter) ObserveTransport(outbound adapter.OutboundIdentity, network string, direction string, bytes int64) {
	if r == nil || r.closed.Load() || r.sink == nil {
		return
	}
	r.sink.addTransport(outbound, network, direction, bytes)
}

func (r *Reporter) ObserveHealthCheck(outbound adapter.Outbound, url string, latencyMS int64, completedAt time.Time) {
	if r == nil || r.closed.Load() || r.sink == nil || outbound == nil {
		return
	}
	_, _, _, egressName, egressType := r.outbound(outbound)
	if egressName == "" {
		egressName = outbound.Tag()
		egressType = normalizeType(outbound.Type())
	}
	r.sink.recordHealth(healthPoint{
		outbound:    adapter.OutboundIdentity{Name: egressName, Type: egressType},
		url:         url,
		latencyMS:   latencyMS,
		completedAt: completedAt,
	})
}

func (r *Reporter) Start(stage adapter.StartStage) error {
	if stage != adapter.StartStateInitialize || !r.started.CompareAndSwap(false, true) {
		return nil
	}
	go r.run()
	return nil
}

func (r *Reporter) run() {
	ticker := time.NewTicker(r.config.activeTimeout)
	defer ticker.Stop()
	defer close(r.done)
	for {
		select {
		case now := <-ticker.C:
			r.snapshotActive(now)
		case <-r.stop:
			return
		}
	}
}

func (r *Reporter) snapshotActive(now time.Time) {
	r.access.RLock()
	flows := make([]*Flow, 0, len(r.flows))
	for flow := range r.flows {
		flows = append(flows, flow)
	}
	r.access.RUnlock()
	for _, flow := range flows {
		flow.snapshot(now, "active_timeout", false)
	}
}

func (r *Reporter) Close() error {
	if !r.closed.CompareAndSwap(false, true) {
		return nil
	}
	if r.started.Load() {
		close(r.stop)
		<-r.done
	}

	r.access.RLock()
	flows := make([]*Flow, 0, len(r.flows))
	for flow := range r.flows {
		flows = append(flows, flow)
	}
	r.access.RUnlock()
	for _, flow := range flows {
		flow.snapshot(time.Now(), "shutdown", true)
	}

	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout(r.config))
	defer cancel()
	return r.sink.shutdown(ctx)
}

func shutdownTimeout(config reporterConfig) time.Duration {
	timeout := config.batchExportTimeout
	if config.metricExportTimeout > timeout {
		timeout = config.metricExportTimeout
	}
	if timeout > 0 {
		return timeout
	}
	return 30 * time.Second
}

func (r *Reporter) newFlow(metadata FlowMetadata, network string, initialUplink, initialDownlink int64) *Flow {
	if r.closed.Load() {
		return nil
	}
	id, err := uuid.NewV4()
	if err != nil {
		r.logger.Error("create OpenTelemetry flow ID: ", err)
		return nil
	}
	now := time.Now()
	flow := &Flow{
		reporter:     r,
		metadata:     metadata,
		id:           id.String(),
		network:      network,
		segmentStart: now,
		udp:          network == N.NetworkUDP,
	}
	if flow.udp {
		flow.udpUplinkBytes = initialUplink
		flow.udpDownlinkBytes = initialDownlink
	} else {
		flow.uplinkBytes.Store(initialUplink)
		flow.downlinkBytes.Store(initialDownlink)
	}
	r.access.Lock()
	if r.closed.Load() {
		r.access.Unlock()
		return nil
	}
	r.flows[flow] = struct{}{}
	r.access.Unlock()
	return flow
}

func (r *Reporter) remove(flow *Flow) {
	r.access.Lock()
	delete(r.flows, flow)
	r.access.Unlock()
}

func resolveConfig(options option.OpenTelemetryOptions) (reporterConfig, error) {
	config := reporterConfig{
		endpoint:             options.Endpoint,
		logsEndpoint:         options.LogsEndpoint,
		metricsEndpoint:      options.MetricsEndpoint,
		protocol:             options.Protocol,
		headers:              options.Headers,
		compression:          strings.ToLower(options.Compression),
		timeout:              time.Duration(options.Timeout),
		activeTimeout:        time.Duration(options.ActiveTimeout),
		batchScheduleDelay:   time.Duration(options.Batch.ScheduleDelay),
		batchExportTimeout:   time.Duration(options.Batch.ExportTimeout),
		batchMaxQueueSize:    options.Batch.MaxQueueSize,
		batchMaxExportSize:   options.Batch.MaxExportBatchSize,
		metricExportInterval: time.Duration(options.Metrics.ExportInterval),
		metricExportTimeout:  time.Duration(options.Metrics.ExportTimeout),
		metricMaxQueueSize:   options.Metrics.MaxQueueSize,
		tls:                  options.TLS,
		resourceAttributes:   options.ResourceAttributes,
	}
	if config.protocol == "" {
		config.protocol = "http/protobuf"
	}
	if config.protocol != "http/protobuf" {
		return config, fmt.Errorf("unsupported OpenTelemetry protocol %q", config.protocol)
	}
	for _, endpoint := range []string{config.endpoint, config.logsEndpoint, config.metricsEndpoint} {
		if endpoint != "" {
			parsed, err := url.Parse(endpoint)
			if err != nil || parsed.Host == "" || parsed.Scheme != "http" && parsed.Scheme != "https" || parsed.RawQuery != "" || parsed.Fragment != "" {
				return config, fmt.Errorf("invalid OpenTelemetry endpoint %q", endpoint)
			}
		}
	}
	if config.compression != "" && config.compression != "gzip" && config.compression != "none" {
		return config, fmt.Errorf("unsupported OpenTelemetry compression %q", config.compression)
	}
	for name, value := range config.headers {
		if !httpguts.ValidHeaderFieldName(name) {
			return config, fmt.Errorf("invalid OpenTelemetry header name %q", name)
		}
		if !httpguts.ValidHeaderFieldValue(value) {
			return config, fmt.Errorf("invalid OpenTelemetry value for header %q", name)
		}
	}
	if config.timeout < 0 || config.batchScheduleDelay < 0 || config.batchExportTimeout < 0 || config.metricExportInterval < 0 || config.metricExportTimeout < 0 {
		return config, errors.New("OpenTelemetry durations must not be negative")
	}
	if config.activeTimeout == 0 {
		config.activeTimeout = defaultActive
	}
	if config.activeTimeout < minimumActive || config.activeTimeout > maximumActive {
		return config, fmt.Errorf("OpenTelemetry active_timeout must be between %s and %s", minimumActive, maximumActive)
	}
	if config.batchMaxQueueSize < 0 || config.batchMaxExportSize < 0 || config.metricMaxQueueSize < 0 {
		return config, errors.New("OpenTelemetry batch sizes must not be negative")
	}
	if config.batchMaxQueueSize > 0 && config.batchMaxExportSize > config.batchMaxQueueSize {
		return config, errors.New("OpenTelemetry max_export_batch_size must not exceed max_queue_size")
	}
	if (config.tls.ClientCertificate == "") != (config.tls.ClientKey == "") {
		return config, errors.New("OpenTelemetry TLS client_certificate and client_key must be configured together")
	}
	if config.metricMaxQueueSize == 0 {
		config.metricMaxQueueSize = 2048
	}
	return config, nil
}

type otelSink struct {
	logger   otellog.Logger
	provider *sdklog.LoggerProvider
	metrics  *metricSink
}

func newOTelSink(ctx context.Context, config reporterConfig) (*otelSink, error) {
	exporterOptions := make([]otlploghttp.Option, 0, 6)
	if config.logsEndpoint != "" {
		exporterOptions = append(exporterOptions, otlploghttp.WithEndpointURL(config.logsEndpoint))
	} else if config.endpoint != "" {
		exporterOptions = append(exporterOptions, otlploghttp.WithEndpointURL(signalEndpoint(config.endpoint, "logs")))
	} else if os.Getenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT") == "" && os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		exporterOptions = append(exporterOptions, otlploghttp.WithEndpointURL(signalEndpoint(defaultEndpoint, "logs")))
	}
	if len(config.headers) > 0 {
		exporterOptions = append(exporterOptions, otlploghttp.WithHeaders(config.headers))
	}
	if config.timeout > 0 {
		exporterOptions = append(exporterOptions, otlploghttp.WithTimeout(config.timeout))
	}
	switch config.compression {
	case "gzip":
		exporterOptions = append(exporterOptions, otlploghttp.WithCompression(otlploghttp.GzipCompression))
	case "none":
		exporterOptions = append(exporterOptions, otlploghttp.WithCompression(otlploghttp.NoCompression))
	}
	tlsConfig, configured, err := loadTLSConfig(config.tls)
	if err != nil {
		return nil, err
	}
	if configured {
		exporterOptions = append(exporterOptions, otlploghttp.WithTLSClientConfig(tlsConfig))
	}

	exporter, err := otlploghttp.New(ctx, exporterOptions...)
	if err != nil {
		return nil, fmt.Errorf("create OTLP log exporter: %w", err)
	}
	batchOptions := make([]sdklog.BatchProcessorOption, 0, 4)
	if config.batchScheduleDelay > 0 {
		batchOptions = append(batchOptions, sdklog.WithExportInterval(config.batchScheduleDelay))
	}
	if config.batchExportTimeout > 0 {
		batchOptions = append(batchOptions, sdklog.WithExportTimeout(config.batchExportTimeout))
	}
	if config.batchMaxQueueSize > 0 {
		batchOptions = append(batchOptions, sdklog.WithMaxQueueSize(config.batchMaxQueueSize))
	}
	if config.batchMaxExportSize > 0 {
		batchOptions = append(batchOptions, sdklog.WithExportMaxBatchSize(config.batchMaxExportSize))
	}
	processor := sdklog.NewBatchProcessor(exporter, batchOptions...)

	res, err := buildResource(config.resourceAttributes)
	if err != nil {
		_ = exporter.Shutdown(ctx)
		return nil, err
	}
	provider := sdklog.NewLoggerProvider(sdklog.WithResource(res), sdklog.WithProcessor(processor))
	metrics, err := newMetricSink(ctx, config, res, tlsConfig, configured)
	if err != nil {
		_ = provider.Shutdown(ctx)
		return nil, err
	}
	return &otelSink{
		logger:   provider.Logger(eventName, otellog.WithInstrumentationVersion(scopeVersion)),
		provider: provider,
		metrics:  metrics,
	}, nil
}

func signalEndpoint(base string, signal string) string {
	base = strings.TrimRight(base, "/")
	if strings.HasSuffix(base, "/v1/logs") {
		base = strings.TrimSuffix(base, "/v1/logs")
	} else if strings.HasSuffix(base, "/v1/metrics") {
		base = strings.TrimSuffix(base, "/v1/metrics")
	}
	return base + "/v1/" + signal
}

func buildResource(configured map[string]string) (*resource.Resource, error) {
	instanceID, err := uuid.NewV4()
	if err != nil {
		return nil, fmt.Errorf("create service.instance.id: %w", err)
	}
	base := resource.NewSchemaless(
		attribute.String("service.name", "sing-box"),
		attribute.String("service.version", C.Version),
		attribute.String("service.instance.id", instanceID.String()),
	)
	res, err := resource.Merge(resource.Default(), base)
	if err != nil {
		return nil, fmt.Errorf("merge default OpenTelemetry resource: %w", err)
	}
	res, err = resource.Merge(res, resource.Environment())
	if err != nil {
		return nil, fmt.Errorf("merge OpenTelemetry environment resource: %w", err)
	}
	keys := make([]string, 0, len(configured))
	for key := range configured {
		if key != "proxy.flow.schema.version" {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	attributes := make([]attribute.KeyValue, 0, len(keys))
	for _, key := range keys {
		attributes = append(attributes, attribute.String(key, configured[key]))
	}
	res, err = resource.Merge(res, resource.NewSchemaless(attributes...))
	if err != nil {
		return nil, fmt.Errorf("merge configured OpenTelemetry resource: %w", err)
	}
	res, err = resource.Merge(res, resource.NewSchemaless(attribute.String("proxy.flow.schema.version", schemaVersion)))
	if err != nil {
		return nil, fmt.Errorf("set proxy flow schema resource: %w", err)
	}
	return res, nil
}

func loadTLSConfig(options option.OpenTelemetryTLSOptions) (*tls.Config, bool, error) {
	configured := options.CACertificate != "" || options.ClientCertificate != "" || options.ClientKey != "" || options.InsecureSkipVerify
	if !configured {
		return nil, false, nil
	}
	config := &tls.Config{InsecureSkipVerify: options.InsecureSkipVerify} //nolint:gosec
	if options.CACertificate != "" {
		pemData, err := os.ReadFile(options.CACertificate)
		if err != nil {
			return nil, false, fmt.Errorf("read OpenTelemetry CA certificate: %w", err)
		}
		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(pemData) {
			return nil, false, errors.New("OpenTelemetry CA certificate contains no certificate")
		}
		config.RootCAs = pool
	}
	if options.ClientCertificate != "" {
		certificate, err := tls.LoadX509KeyPair(options.ClientCertificate, options.ClientKey)
		if err != nil {
			return nil, false, fmt.Errorf("load OpenTelemetry client certificate: %w", err)
		}
		config.Certificates = []tls.Certificate{certificate}
	}
	return config, true, nil
}

func (s *otelSink) emit(segment flowSegment) {
	var record otellog.Record
	record.SetEventName(eventName)
	record.SetTimestamp(segment.start)
	record.SetObservedTimestamp(segment.end)
	record.SetBody(otellog.StringValue(""))
	record.AddAttributes(segment.attributes()...)
	s.logger.Emit(context.Background(), record)
}

func (s *otelSink) addTransport(outbound adapter.OutboundIdentity, network string, direction string, bytes int64) {
	s.metrics.addTransport(outbound, network, direction, bytes)
}

func (s *otelSink) recordHealth(point healthPoint) {
	s.metrics.recordHealth(point)
}

func (s *otelSink) shutdown(ctx context.Context) error {
	return errors.Join(s.provider.Shutdown(ctx), s.metrics.shutdown(ctx))
}

type FlowMetadata struct {
	clientAddress      string
	clientPort         int64
	destinationAddress string
	destinationPort    int64
	networkType        string
	inboundName        string
	inboundType        string
	outboundName       string
	outboundType       string
	outboundChain      []string
	egressName         string
	egressType         string
	ruleMatched        bool
	ruleType           string
	ruleValue          string
	ruleAction         string
	originalAddress    string
	originalPort       int64
	resolvedAddresses  []string
	protocolName       string
	processPID         int64
	processName        string
	processPath        string
	processOwner       string
	processUID         int64
	androidPackages    []string
}

type Flow struct {
	reporter *Reporter
	metadata FlowMetadata
	id       string
	network  string
	udp      bool

	uplinkBytes   atomic.Int64
	downlinkBytes atomic.Int64

	udpAccess          sync.Mutex
	udpUplinkBytes     int64
	udpDownlinkBytes   int64
	udpUplinkPackets   int64
	udpDownlinkPackets int64

	selectionAccess sync.RWMutex
	selections      map[string]adapter.OutboundIdentity
	selectedLeaf    adapter.OutboundIdentity

	segmentAccess sync.RWMutex
	segmentStart  time.Time
	sequence      int64
	closed        bool
}

func (f *Flow) addUplink(bytes int64, packet bool) {
	if f == nil || bytes < 0 {
		return
	}
	f.segmentAccess.RLock()
	defer f.segmentAccess.RUnlock()
	if f.closed {
		return
	}
	if f.udp {
		f.udpAccess.Lock()
		f.udpUplinkBytes += bytes
		if packet {
			f.udpUplinkPackets++
		}
		f.udpAccess.Unlock()
		return
	}
	if bytes > 0 {
		f.uplinkBytes.Add(bytes)
	}
}

func (f *Flow) addDownlink(bytes int64, packet bool) {
	if f == nil || bytes < 0 {
		return
	}
	f.segmentAccess.RLock()
	defer f.segmentAccess.RUnlock()
	if f.closed {
		return
	}
	if f.udp {
		f.udpAccess.Lock()
		f.udpDownlinkBytes += bytes
		if packet {
			f.udpDownlinkPackets++
		}
		f.udpAccess.Unlock()
		return
	}
	if bytes > 0 {
		f.downlinkBytes.Add(bytes)
	}
}

func (f *Flow) RecordOutboundSelection(parent adapter.OutboundIdentity, selected adapter.OutboundIdentity) {
	if f == nil || parent.Name == "" || selected.Name == "" {
		return
	}
	f.selectionAccess.Lock()
	if f.selections == nil {
		f.selections = make(map[string]adapter.OutboundIdentity)
	}
	f.selections[parent.Name] = selected
	f.selectionAccess.Unlock()
}

func (f *Flow) RecordOutboundLeaf(outbound adapter.OutboundIdentity) {
	if f == nil || outbound.Name == "" {
		return
	}
	f.selectionAccess.Lock()
	f.selectedLeaf = outbound
	f.selectionAccess.Unlock()
}

func (f *Flow) resolvedMetadata() FlowMetadata {
	metadata := f.metadata
	f.selectionAccess.RLock()
	defer f.selectionAccess.RUnlock()
	if len(f.selections) == 0 {
		metadata.outboundChain = append([]string(nil), metadata.outboundChain...)
		if f.selectedLeaf.Name != "" {
			leaf := f.selectedLeaf
			if len(metadata.outboundChain) == 0 {
				metadata.outboundChain = append(metadata.outboundChain, leaf.Name)
			} else if metadata.outboundChain[len(metadata.outboundChain)-1] != leaf.Name {
				if metadata.egressName != "" && metadata.outboundChain[len(metadata.outboundChain)-1] == metadata.egressName {
					metadata.outboundChain[len(metadata.outboundChain)-1] = leaf.Name
				} else {
					metadata.outboundChain = append(metadata.outboundChain, leaf.Name)
				}
			}
			metadata.egressName = leaf.Name
			metadata.egressType = normalizeType(leaf.Type)
		}
		return metadata
	}
	current := adapter.OutboundIdentity{Name: metadata.outboundName, Type: metadata.outboundType}
	chain := make([]string, 0, len(f.selections)+2)
	seen := make(map[string]struct{}, len(f.selections)+2)
	for current.Name != "" && len(chain) < 32 {
		if _, loaded := seen[current.Name]; loaded {
			break
		}
		seen[current.Name] = struct{}{}
		chain = append(chain, current.Name)
		next, loaded := f.selections[current.Name]
		if !loaded {
			break
		}
		current = next
	}
	leaf := f.selectedLeaf
	if leaf.Name != "" {
		if len(chain) == 0 || chain[len(chain)-1] != leaf.Name {
			chain = append(chain, leaf.Name)
		}
		current = leaf
	}
	metadata.outboundChain = chain
	metadata.egressName = current.Name
	metadata.egressType = normalizeType(current.Type)
	return metadata
}

func (f *Flow) snapshot(end time.Time, reason string, closeFlow bool) {
	if f == nil {
		return
	}
	f.segmentAccess.Lock()
	if f.closed {
		f.segmentAccess.Unlock()
		return
	}
	if closeFlow {
		f.closed = true
	}
	start := f.segmentStart
	f.segmentStart = end
	var uplinkBytes, downlinkBytes, uplinkPackets, downlinkPackets int64
	if f.udp {
		f.udpAccess.Lock()
		uplinkBytes, f.udpUplinkBytes = f.udpUplinkBytes, 0
		downlinkBytes, f.udpDownlinkBytes = f.udpDownlinkBytes, 0
		uplinkPackets, f.udpUplinkPackets = f.udpUplinkPackets, 0
		downlinkPackets, f.udpDownlinkPackets = f.udpDownlinkPackets, 0
		f.udpAccess.Unlock()
	} else {
		uplinkBytes = f.uplinkBytes.Swap(0)
		downlinkBytes = f.downlinkBytes.Swap(0)
	}
	if uplinkBytes == 0 && downlinkBytes == 0 && uplinkPackets == 0 && downlinkPackets == 0 {
		f.segmentAccess.Unlock()
		if closeFlow {
			f.reporter.remove(f)
		}
		return
	}
	f.sequence++
	sequence := f.sequence
	f.reporter.sink.emit(flowSegment{
		metadata: f.resolvedMetadata(), id: f.id, network: f.network, udp: f.udp,
		start: start, end: end,
		uplinkBytes: uplinkBytes, downlinkBytes: downlinkBytes,
		uplinkDatagrams: uplinkPackets, downlinkDatagrams: downlinkPackets,
		sequence: sequence, reason: reason,
	})
	f.segmentAccess.Unlock()
	if closeFlow {
		f.reporter.remove(f)
	}
}

type flowSegment struct {
	metadata          FlowMetadata
	id                string
	network           string
	udp               bool
	start             time.Time
	end               time.Time
	uplinkBytes       int64
	downlinkBytes     int64
	uplinkDatagrams   int64
	downlinkDatagrams int64
	sequence          int64
	reason            string
}

func (s flowSegment) attributes() []otellog.KeyValue {
	metadata := s.metadata
	attributes := make([]otellog.KeyValue, 0, 40)
	appendString := func(key, value string) {
		if value != "" {
			attributes = append(attributes, otellog.String(key, value))
		}
	}
	appendInt := func(key string, value int64) {
		if value != 0 {
			attributes = append(attributes, otellog.Int64(key, value))
		}
	}
	appendStrings := func(key string, values []string) {
		if len(values) == 0 {
			return
		}
		converted := make([]otellog.Value, 0, len(values))
		for _, value := range values {
			if value != "" {
				converted = append(converted, otellog.StringValue(value))
			}
		}
		if len(converted) > 0 {
			attributes = append(attributes, otellog.Slice(key, converted...))
		}
	}
	appendString("client.address", metadata.clientAddress)
	appendInt("client.port", metadata.clientPort)
	appendString("server.address", metadata.destinationAddress)
	appendInt("server.port", metadata.destinationPort)
	appendString("network.transport", s.network)
	appendString("network.type", metadata.networkType)
	attributes = append(attributes,
		otellog.Int64("proxy.flow.payload.uplink.bytes", s.uplinkBytes),
		otellog.Int64("proxy.flow.payload.downlink.bytes", s.downlinkBytes),
		otellog.String("proxy.flow.id", s.id),
		otellog.Int64("proxy.flow.segment.sequence", s.sequence),
		otellog.String("proxy.flow.end_reason", s.reason),
	)
	if s.udp {
		attributes = append(attributes,
			otellog.Int64("proxy.flow.payload.uplink.datagrams", s.uplinkDatagrams),
			otellog.Int64("proxy.flow.payload.downlink.datagrams", s.downlinkDatagrams),
		)
	}
	appendString("proxy.inbound.name", metadata.inboundName)
	appendString("proxy.inbound.type", metadata.inboundType)
	appendString("proxy.outbound.name", metadata.outboundName)
	appendString("proxy.outbound.type", metadata.outboundType)
	appendStrings("proxy.outbound.chain", metadata.outboundChain)
	appendString("proxy.outbound.egress.name", metadata.egressName)
	appendString("proxy.outbound.egress.type", metadata.egressType)
	attributes = append(attributes, otellog.Bool("proxy.rule.matched", metadata.ruleMatched))
	appendString("proxy.rule.type", metadata.ruleType)
	appendString("proxy.rule.value", metadata.ruleValue)
	appendString("proxy.rule.action", metadata.ruleAction)
	appendString("proxy.destination.original.address", metadata.originalAddress)
	appendInt("proxy.destination.original.port", metadata.originalPort)
	appendStrings("proxy.destination.resolved_addresses", metadata.resolvedAddresses)
	appendString("network.protocol.name", metadata.protocolName)
	appendInt("process.pid", metadata.processPID)
	appendString("process.executable.name", metadata.processName)
	appendString("process.executable.path", metadata.processPath)
	appendString("process.owner", metadata.processOwner)
	if metadata.processUID >= 0 {
		attributes = append(attributes, otellog.Int64("process.real_user.id", metadata.processUID))
	}
	appendStrings("proxy.client.android.package_names", metadata.androidPackages)
	return attributes
}

func socksAddress(address M.Socksaddr) (string, int64) {
	if address.Fqdn != "" {
		return address.Fqdn, int64(address.Port)
	}
	if address.Addr.IsValid() {
		return address.Addr.String(), int64(address.Port)
	}
	return "", int64(address.Port)
}

func sameEndpoint(address string, port int64, otherAddress string, otherPort int64) bool {
	return address == otherAddress && port == otherPort
}
