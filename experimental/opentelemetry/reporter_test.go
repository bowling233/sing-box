package opentelemetry

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/option"
	M "github.com/sagernet/sing/common/metadata"
	otellog "go.opentelemetry.io/otel/log"
	collectorlogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectormetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/protobuf/proto"
)

type captureSink struct {
	access   sync.Mutex
	segments []flowSegment
}

func (s *captureSink) emit(segment flowSegment) {
	s.access.Lock()
	s.segments = append(s.segments, segment)
	s.access.Unlock()
}

func (s *captureSink) addTransport(adapter.OutboundIdentity, string, string, int64) {}

func (s *captureSink) recordHealth(healthPoint) {}

func (s *captureSink) shutdown(context.Context) error { return nil }

func (s *captureSink) snapshot() []flowSegment {
	s.access.Lock()
	defer s.access.Unlock()
	return append([]flowSegment(nil), s.segments...)
}

func TestCanonicalFlowContract(t *testing.T) {
	start := time.Unix(1, 0)
	end := time.Unix(61, 0)
	metadata := FlowMetadata{
		clientAddress:      "192.0.2.10",
		clientPort:         52341,
		destinationAddress: "example.com",
		destinationPort:    443,
		inboundName:        "tun-in",
		inboundType:        "tun",
		outboundName:       "auto-select",
		outboundType:       "url_test",
		outboundChain:      []string{"auto-select", "node-a"},
		egressName:         "node-a",
		ruleMatched:        true,
		ruleType:           "domain_suffix",
		ruleValue:          "example.com",
		ruleAction:         "auto-select",
		resolvedAddresses:  []string{"203.0.113.8"},
		protocolName:       "tls",
		processName:        "curl",
		processPath:        "/usr/bin/curl",
		processUID:         1000,
	}
	segment := flowSegment{
		metadata: metadata, id: "00000000-0000-4000-8000-000000000001", network: "tcp",
		start: start, end: end, uplinkBytes: 18432, downlinkBytes: 2048,
		sequence: 1, reason: "active_timeout",
	}
	assertGolden(t, segment)
}

func TestDirectionAndSegmentAccounting(t *testing.T) {
	sink := new(captureSink)
	reporter := &Reporter{sink: sink, flows: make(map[*Flow]struct{})}
	start := time.Unix(10, 0)
	end := time.Unix(20, 0)
	flow := &Flow{
		reporter: reporter,
		metadata: FlowMetadata{
			clientAddress: "192.0.2.1", clientPort: 1000,
			destinationAddress: "198.51.100.2", destinationPort: 443,
			processUID: -1,
		},
		id: "flow", network: "tcp", segmentStart: start,
	}
	reporter.flows[flow] = struct{}{}
	flow.addUplink(7, false)
	flow.addDownlink(11, false)
	flow.snapshot(end, "closed", true)
	if len(sink.segments) != 1 {
		t.Fatalf("got %d segments, want 1", len(sink.segments))
	}
	attributes := attributesMap(sink.segments[0].attributes())
	if attributes["client.address"] != "192.0.2.1" || attributes["server.address"] != "198.51.100.2" {
		t.Fatalf("unexpected endpoints: %#v", attributes)
	}
	if attributes["proxy.flow.payload.uplink.bytes"] != int64(7) || attributes["proxy.flow.payload.downlink.bytes"] != int64(11) {
		t.Fatalf("unexpected counters: %#v", attributes)
	}
	if _, exists := attributes["proxy.flow.payload.uplink.datagrams"]; exists {
		t.Fatal("TCP segment contains datagram counters")
	}
}

func TestZeroLengthUDPDatagram(t *testing.T) {
	sink := new(captureSink)
	reporter := &Reporter{sink: sink, flows: make(map[*Flow]struct{})}
	flow := &Flow{reporter: reporter, metadata: FlowMetadata{processUID: -1}, id: "udp", network: "udp", udp: true, segmentStart: time.Unix(1, 0)}
	flow.addUplink(0, true)
	flow.snapshot(time.Unix(2, 0), "closed", true)
	if len(sink.segments) != 1 {
		t.Fatalf("got %d segments, want 1", len(sink.segments))
	}
	attributes := attributesMap(sink.segments[0].attributes())
	if attributes["proxy.flow.payload.uplink.bytes"] != int64(0) || attributes["proxy.flow.payload.downlink.bytes"] != int64(0) || attributes["proxy.flow.payload.uplink.datagrams"] != int64(1) || attributes["proxy.flow.payload.downlink.datagrams"] != int64(0) {
		t.Fatalf("unexpected UDP counters: %#v", attributes)
	}
}

func TestConfigurationValidation(t *testing.T) {
	if normalized := normalizeType("URLTest"); normalized != "url_test" {
		t.Fatalf("normalize URLTest: got %q", normalized)
	}
	_, err := resolveConfig(option.OpenTelemetryOptions{Protocol: "grpc"})
	if err == nil {
		t.Fatal("expected unsupported protocol error")
	}
	_, err = resolveConfig(option.OpenTelemetryOptions{Batch: option.OpenTelemetryBatchOptions{MaxQueueSize: 1, MaxExportBatchSize: 2}})
	if err == nil {
		t.Fatal("expected invalid batch sizes error")
	}
	_, err = resolveConfig(option.OpenTelemetryOptions{Headers: map[string]string{"invalid header": "value"}})
	if err == nil {
		t.Fatal("expected invalid header error")
	}
}

func TestMetadataDoesNotTreatInboundListenerAsOriginalDestination(t *testing.T) {
	reporter := new(Reporter)
	metadata := adapter.InboundContext{
		Source:            M.SocksaddrFrom(netip.MustParseAddr("127.0.0.1"), 50000),
		Destination:       M.SocksaddrFrom(netip.MustParseAddr("127.0.0.1"), 18080),
		OriginDestination: M.SocksaddrFrom(netip.MustParseAddr("127.0.0.1"), 17890),
		User:              "alice",
	}
	flowMetadata := reporter.metadata(metadata, nil, nil)
	if flowMetadata.originalAddress != "" || flowMetadata.originalPort != 0 {
		t.Fatalf("inbound listener leaked as original destination: %#v", flowMetadata)
	}
	metadata.RouteOriginalDestination = M.SocksaddrFrom(netip.MustParseAddr("192.0.2.1"), 443)
	flowMetadata = reporter.metadata(metadata, nil, nil)
	if flowMetadata.originalAddress != "192.0.2.1" || flowMetadata.originalPort != 443 {
		t.Fatalf("route original destination missing: %#v", flowMetadata)
	}
}

func TestFlowUsesSuccessfulOutboundSelection(t *testing.T) {
	flow := &Flow{metadata: FlowMetadata{
		outboundName:  "select",
		outboundType:  "selector",
		outboundChain: []string{"select", "stale-node"},
		egressName:    "stale-node",
		egressType:    "vless",
	}}
	flow.RecordOutboundSelection(
		adapter.OutboundIdentity{Name: "select", Type: "selector"},
		adapter.OutboundIdentity{Name: "auto", Type: "urltest"},
	)
	flow.RecordOutboundSelection(
		adapter.OutboundIdentity{Name: "auto", Type: "urltest"},
		adapter.OutboundIdentity{Name: "node-b", Type: "Hysteria2"},
	)
	flow.RecordOutboundLeaf(adapter.OutboundIdentity{Name: "node-b", Type: "Hysteria2"})
	metadata := flow.resolvedMetadata()
	if !reflect.DeepEqual(metadata.outboundChain, []string{"select", "auto", "node-b"}) || metadata.egressName != "node-b" || metadata.egressType != "hysteria2" {
		t.Fatalf("unexpected successful selection: %#v", metadata)
	}
}

func TestFlowLeafUpdatePreservesPrematchedChain(t *testing.T) {
	flow := &Flow{metadata: FlowMetadata{
		outboundName:  "select",
		outboundType:  "selector",
		outboundChain: []string{"select", "auto", "node-a"},
		egressName:    "node-a",
		egressType:    "vless",
	}}
	flow.RecordOutboundLeaf(adapter.OutboundIdentity{Name: "node-a", Type: "VLESS"})
	metadata := flow.resolvedMetadata()
	if !reflect.DeepEqual(metadata.outboundChain, []string{"select", "auto", "node-a"}) || metadata.egressName != "node-a" || metadata.egressType != "vless" {
		t.Fatalf("unexpected prematched chain: %#v", metadata)
	}

	flow.RecordOutboundLeaf(adapter.OutboundIdentity{Name: "node-b", Type: "Hysteria2"})
	metadata = flow.resolvedMetadata()
	if !reflect.DeepEqual(metadata.outboundChain, []string{"select", "auto", "node-b"}) || metadata.egressName != "node-b" || metadata.egressType != "hysteria2" {
		t.Fatalf("unexpected updated leaf: %#v", metadata)
	}
}

func TestActiveSnapshotCloseRace(t *testing.T) {
	for iteration := 0; iteration < 100; iteration++ {
		sink := new(captureSink)
		reporter := &Reporter{sink: sink, flows: make(map[*Flow]struct{})}
		flow := &Flow{
			reporter: reporter,
			metadata: FlowMetadata{processUID: -1},
			id:       "flow", network: "tcp", segmentStart: time.Unix(1, 0),
		}
		reporter.flows[flow] = struct{}{}
		flow.addUplink(1, false)

		start := make(chan struct{})
		var wait sync.WaitGroup
		wait.Add(2)
		go func() {
			defer wait.Done()
			<-start
			flow.snapshot(time.Unix(2, 0), "active_timeout", false)
		}()
		go func() {
			defer wait.Done()
			<-start
			flow.snapshot(time.Unix(3, 0), "closed", true)
		}()
		close(start)
		wait.Wait()

		segments := sink.snapshot()
		var total int64
		for _, segment := range segments {
			total += segment.uplinkBytes + segment.downlinkBytes
		}
		if total != 1 {
			t.Fatalf("iteration %d: got %d bytes across %#v", iteration, total, segments)
		}
		flow.addUplink(1, false)
		flow.snapshot(time.Unix(4, 0), "active_timeout", false)
		if len(sink.snapshot()) != len(segments) {
			t.Fatalf("iteration %d: closed flow accepted a late counter", iteration)
		}
	}
}

func TestOTLPHTTPExport(t *testing.T) {
	requests := make(chan *collectorlogspb.ExportLogsServiceRequest, 1)
	metricRequests := make(chan *collectormetricspb.ExportMetricsServiceRequest, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/v1/metrics" {
			body, err := io.ReadAll(request.Body)
			if err != nil {
				t.Error(err)
				writer.WriteHeader(http.StatusBadRequest)
				return
			}
			exportRequest := new(collectormetricspb.ExportMetricsServiceRequest)
			if err = proto.Unmarshal(body, exportRequest); err != nil {
				t.Error(err)
				writer.WriteHeader(http.StatusBadRequest)
				return
			}
			metricRequests <- exportRequest
			writer.Header().Set("Content-Type", "application/x-protobuf")
			writer.WriteHeader(http.StatusOK)
			return
		}
		if request.URL.Path != "/v1/logs" {
			t.Errorf("unexpected path %q", request.URL.Path)
		}
		if request.Header.Get("X-Test-Token") != "present" {
			t.Error("configured OTLP header missing")
		}
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Error(err)
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		exportRequest := new(collectorlogspb.ExportLogsServiceRequest)
		if err = proto.Unmarshal(body, exportRequest); err != nil {
			t.Error(err)
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		requests <- exportRequest
		writer.Header().Set("Content-Type", "application/x-protobuf")
		writer.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	sink, err := newOTelSink(context.Background(), reporterConfig{
		endpoint:           server.URL,
		headers:            map[string]string{"X-Test-Token": "present"},
		metricMaxQueueSize: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	sink.emit(flowSegment{
		metadata: FlowMetadata{clientAddress: "192.0.2.1", destinationAddress: "example.com", processUID: -1},
		id:       "flow-id", network: "tcp", start: time.Unix(1, 0), end: time.Unix(2, 0), uplinkBytes: 5, sequence: 1, reason: "closed",
	})
	sink.addTransport(adapter.OutboundIdentity{Name: "node-a", Type: "VLESS"}, "tcp", "transmit", 12)
	sink.recordHealth(healthPoint{outbound: adapter.OutboundIdentity{Name: "node-a", Type: "VLESS"}, url: "https://example.com/204", latencyMS: 37, completedAt: time.Unix(3, 4)})
	sink.recordHealth(healthPoint{outbound: adapter.OutboundIdentity{Name: "node-a", Type: "VLESS"}, url: "https://example.com/204", latencyMS: 41, completedAt: time.Unix(5, 6)})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err = sink.shutdown(ctx); err != nil {
		t.Fatal(err)
	}

	select {
	case exportRequest := <-requests:
		if len(exportRequest.ResourceLogs) != 1 || len(exportRequest.ResourceLogs[0].ScopeLogs) != 1 || len(exportRequest.ResourceLogs[0].ScopeLogs[0].LogRecords) != 1 {
			t.Fatalf("unexpected OTLP shape: %#v", exportRequest)
		}
		record := exportRequest.ResourceLogs[0].ScopeLogs[0].LogRecords[0]
		if record.EventName != eventName || record.TimeUnixNano != uint64(time.Unix(1, 0).UnixNano()) || record.Body.GetStringValue() != "" {
			t.Fatalf("unexpected OTLP record: %#v", record)
		}
		if resourceAttribute(exportRequest, "service.name") != "sing-box" || resourceAttribute(exportRequest, "proxy.flow.schema.version") != schemaVersion {
			t.Fatalf("unexpected resource: %#v", exportRequest.ResourceLogs[0].Resource)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for OTLP request")
	}

	select {
	case exportRequest := <-metricRequests:
		transport := findMetric(exportRequest, transportMetricName)
		if transport == nil || transport.Unit != "By" || !transport.GetSum().IsMonotonic || len(transport.GetSum().DataPoints) != 1 || transport.GetSum().DataPoints[0].GetAsInt() != 12 {
			t.Fatalf("unexpected transport metric: %#v", transport)
		}
		health := findMetric(exportRequest, healthMetricName)
		if health == nil || health.Unit != "ms" || len(health.GetGauge().DataPoints) != 2 {
			t.Fatalf("unexpected health metric: %#v", health)
		}
		if health.GetGauge().DataPoints[0].TimeUnixNano != uint64(time.Unix(3, 4).UnixNano()) || health.GetGauge().DataPoints[0].GetAsInt() != 37 || health.GetGauge().DataPoints[1].TimeUnixNano != uint64(time.Unix(5, 6).UnixNano()) || health.GetGauge().DataPoints[1].GetAsInt() != 41 {
			t.Fatalf("health points lost precision or ordering: %#v", health.GetGauge().DataPoints)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for OTLP metrics request")
	}
}

func findMetric(request *collectormetricspb.ExportMetricsServiceRequest, name string) *metricspb.Metric {
	for _, resourceMetrics := range request.ResourceMetrics {
		for _, scopeMetrics := range resourceMetrics.ScopeMetrics {
			for _, metric := range scopeMetrics.Metrics {
				if metric.Name == name {
					return metric
				}
			}
		}
	}
	return nil
}

func resourceAttribute(request *collectorlogspb.ExportLogsServiceRequest, key string) string {
	for _, attribute := range request.ResourceLogs[0].Resource.Attributes {
		if attribute.Key == key {
			return attribute.Value.GetStringValue()
		}
	}
	return ""
}

func assertGolden(t *testing.T, segment flowSegment) {
	t.Helper()
	actual := map[string]any{
		"event_name":                   eventName,
		"timestamp_unix_nano":          segment.start.UnixNano(),
		"observed_timestamp_unix_nano": segment.end.UnixNano(),
		"attributes":                   attributesMap(segment.attributes()),
	}
	actualJSON, err := json.Marshal(actual)
	if err != nil {
		t.Fatal(err)
	}
	expectedJSON, err := os.ReadFile("testdata/proxy_flow_golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var actualValue, expectedValue any
	if err = json.Unmarshal(actualJSON, &actualValue); err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(expectedJSON, &expectedValue); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(actualValue, expectedValue) {
		t.Fatalf("contract mismatch\nactual: %s\nexpected: %s", actualJSON, expectedJSON)
	}
}

func attributesMap(attributes []otellog.KeyValue) map[string]any {
	result := make(map[string]any, len(attributes))
	for _, attribute := range attributes {
		result[attribute.Key] = logValue(attribute.Value)
	}
	return result
}

func logValue(value otellog.Value) any {
	switch value.Kind() {
	case otellog.KindString:
		return value.AsString()
	case otellog.KindInt64:
		return value.AsInt64()
	case otellog.KindBool:
		return value.AsBool()
	case otellog.KindSlice:
		values := value.AsSlice()
		result := make([]any, 0, len(values))
		for _, item := range values {
			result = append(result, logValue(item))
		}
		return result
	default:
		return value.String()
	}
}
