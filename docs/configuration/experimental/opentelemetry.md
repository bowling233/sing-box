# OpenTelemetry Traffic

The OpenTelemetry traffic reporter exports native per-flow payload counters as
OTLP/HTTP protobuf log records. It does not depend on the Clash API.

Each TCP connection or UDP association is split into `uplink` and `downlink`
`proxy.flow` records. Long-lived flows emit incremental records at the active
timeout. TCP packet counts are omitted; UDP records include datagram counts.
Known routing-rule and process attributes are always included. Inbound
authentication users are never exported. Enabling this reporter does not enable
additional process lookup; use the existing route process settings when needed.

### Structure

```json
{
  "enabled": true,
  "endpoint": "http://127.0.0.1:4318/v1/logs",
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
  "tls": {
    "ca_certificate": "",
    "client_certificate": "",
    "client_key": "",
    "insecure_skip_verify": false
  },
  "resource_attributes": {
    "deployment.environment.name": "home"
  }
}
```

### Fields

#### enabled

Enable traffic export. Disabled by default. `OTEL_SDK_DISABLED=true` also
disables the reporter.

#### endpoint

Complete OTLP logs URL. The default is
`http://127.0.0.1:4318/v1/logs`. Standard signal-specific and general OTLP
environment variables are used when this field is empty.

#### protocol

Only `http/protobuf` is supported.

#### headers

Additional OTLP HTTP headers.

#### compression

Empty, `none`, or `gzip`.

#### timeout

Timeout for one export request. The exporter default is `10s`.

#### active_timeout

Interval for incremental long-lived-flow records. The default is `60s`; valid
values range from `10s` through `24h`.

#### batch

Batch log record processor settings. Defaults are `1s`, `30s`, `2048`, and
`512`, respectively. `max_export_batch_size` cannot exceed `max_queue_size`.

#### tls

Optional custom server CA, mTLS client certificate/key pair, and certificate
verification override. `insecure_skip_verify` should only be used for testing.

#### resource_attributes

Additional string-valued OpenTelemetry resource attributes. The reporter
always sets `proxy.flow.schema.version`; it cannot be overridden.

The reporter also honors standard `OTEL_SERVICE_NAME`,
`OTEL_RESOURCE_ATTRIBUTES`, `OTEL_EXPORTER_OTLP_*`,
`OTEL_EXPORTER_OTLP_LOGS_*`, and `OTEL_BLRP_*` environment variables. The
configuration block must still have `enabled: true`.
