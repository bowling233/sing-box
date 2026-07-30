# OpenTelemetry observability

The OpenTelemetry reporter exports three native signals through OTLP/HTTP
protobuf:

- bidirectional per-flow payload records in `proxy.flow` logs;
- actual outbound socket traffic in the monotonic
  `proxy.outbound.transport.io` counter;
- every successful URL test as an individual
  `proxy.outbound.health_check.latency` Gauge point.

Flow records include routing-rule and already available process metadata. The
client identity is represented by its original network endpoint. The reporter
uses the route process information already collected by sing-box.

### Structure

```json
{
  "enabled": true,
  "endpoint": "http://127.0.0.1:4318",
  "logs_endpoint": "",
  "metrics_endpoint": "",
  "protocol": "http/protobuf",
  "headers": {},
  "compression": "gzip",
  "timeout": "10s",
  "active_timeout": "60s",
  "batch": {
    "schedule_delay": "1s",
    "export_timeout": "30s",
    "max_queue_size": 2048,
    "max_export_batch_size": 512
  },
  "metrics": {
    "export_interval": "60s",
    "export_timeout": "30s",
    "max_queue_size": 2048
  },
  "tls": {
    "ca_certificate": "",
    "client_certificate": "",
    "client_key": "",
    "insecure_skip_verify": false
  },
  "resource_attributes": {
    "deployment.environment.name": "home",
    "service.instance.id": "proxy-node-a"
  }
}
```

### Fields

#### enabled

Enable OpenTelemetry export. Disabled by default.
`OTEL_SDK_DISABLED=true` also disables the reporter.

#### endpoint

Collector base URL. `/v1/logs` and `/v1/metrics` are appended for their
respective signals. An URL ending in either signal path is also accepted and
the sibling signal URL is derived automatically. The default is
`http://127.0.0.1:4318`.

#### logs_endpoint

Complete OTLP Logs URL. When set, it overrides `endpoint` for logs.

#### metrics_endpoint

Complete OTLP Metrics URL. When set, it overrides `endpoint` for metrics.

#### protocol

Only `http/protobuf` is supported.

#### headers

Additional OTLP HTTP headers shared by both signals.

#### compression

Empty, `none`, or `gzip` for both signals.

#### timeout

Timeout for one exporter HTTP request.

#### active_timeout

Interval for incremental long-lived-flow records. The default is `60s`; valid
values range from `10s` through `24h`. A segment with traffic contains both
uplink and downlink payload counters, including zero for the empty direction.

#### batch

Batch log processor schedule delay, export timeout, queue size, and export
batch size. `max_export_batch_size` cannot exceed `max_queue_size`.

#### metrics

Periodic metric export interval and timeout. `max_queue_size` bounds pending
URL-test points while retaining their original completion timestamps.

`proxy.outbound.transport.io` counts bytes at the actual leaf outbound's outer
socket. It includes proxy handshakes, encryption, protocol framing, QUIC/TLS,
and multiplex transport traffic. The attributes identify the outbound,
transport, and `transmit` or `receive` direction.

`proxy.outbound.health_check.latency` uses integer milliseconds. Each
successful URL test is retained as a separate Gauge point with the actual
completion time, outbound identity, and `url.full`.

#### tls

Optional custom server CA, mTLS client certificate/key pair, and certificate
verification override. `insecure_skip_verify` should only be used for testing.

#### resource_attributes

Additional string-valued OpenTelemetry Resource attributes. The reporter sets
`proxy.flow.schema.version=v1alpha2`. A configured
`service.instance.id` remains stable across process restarts.

The reporter honors standard `OTEL_SERVICE_NAME`,
`OTEL_RESOURCE_ATTRIBUTES`, `OTEL_EXPORTER_OTLP_*`,
`OTEL_EXPORTER_OTLP_LOGS_*`, `OTEL_EXPORTER_OTLP_METRICS_*`, `OTEL_BLRP_*`,
`OTEL_METRIC_EXPORT_INTERVAL`, and `OTEL_METRIC_EXPORT_TIMEOUT` environment
variables. The configuration block must still set `enabled: true`.
