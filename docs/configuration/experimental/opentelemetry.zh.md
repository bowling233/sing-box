# OpenTelemetry 可观测性

OpenTelemetry 上报器通过 OTLP/HTTP protobuf 导出三类原生信号：

- `proxy.flow` 日志中的逐 flow 双向 payload 记录；
- 单调递增 Counter `proxy.outbound.transport.io` 中的真实出口 socket 流量；
- Gauge `proxy.outbound.health_check.latency` 中逐次保留的成功 URLTest 结果。

Flow 记录包含路由规则和路由阶段已有的进程信息，客户端身份使用其原始网络端点表示。
上报器直接读取 sing-box 已获得的进程信息。

### 结构

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

### 字段

#### enabled

启用 OpenTelemetry 上报，默认关闭。`OTEL_SDK_DISABLED=true` 也会关闭本功能。

#### endpoint

Collector 基础 URL。Logs 与 Metrics 分别在其后添加 `/v1/logs` 和
`/v1/metrics`。以任一信号路径结尾的 URL 也可直接使用，上报器会自动推导另一个
信号的 URL。默认值为 `http://127.0.0.1:4318`。

#### logs_endpoint

完整的 OTLP Logs URL。设置后覆盖 Logs 使用的 `endpoint`。

#### metrics_endpoint

完整的 OTLP Metrics URL。设置后覆盖 Metrics 使用的 `endpoint`。

#### protocol

目前支持 `http/protobuf`。

#### headers

两个信号共用的附加 OTLP HTTP headers。

#### compression

两个信号共用的压缩方式，可设为空、`none` 或 `gzip`。

#### timeout

单次 exporter HTTP 请求的超时。

#### active_timeout

长连接增量记录周期，默认为 `60s`，有效范围为 `10s` 至 `24h`。有流量的
segment 在同一条记录中同时包含上下行 payload 计数，空方向的值为零。

#### batch

Logs batch processor 的调度间隔、导出超时、队列长度与单批长度。
`max_export_batch_size` 不得大于 `max_queue_size`。

#### metrics

Metrics 的周期导出间隔和超时。`max_queue_size` 限制待上报 URLTest 点的数量，
每个点保留原始检测完成时间。

`proxy.outbound.transport.io` 在真实叶子 outbound 的外层 socket 计数，覆盖代理握手、
加密、协议封装、QUIC/TLS 和 multiplex 传输流量。属性标识出口、传输协议以及
`transmit` 或 `receive` 方向。

`proxy.outbound.health_check.latency` 使用整数毫秒。每次成功 URLTest 都保留为独立
Gauge 点，包含实际完成时间、出口身份和 `url.full`。

#### tls

自定义服务端 CA、mTLS 客户端证书/私钥和证书校验设置。
`insecure_skip_verify` 适用于测试环境。

#### resource_attributes

附加的字符串 OpenTelemetry Resource 属性。上报器设置
`proxy.flow.schema.version=v1alpha2`。显式配置 `service.instance.id` 可使节点标识在
进程重启后保持稳定。

上报器支持标准的 `OTEL_SERVICE_NAME`、`OTEL_RESOURCE_ATTRIBUTES`、
`OTEL_EXPORTER_OTLP_*`、`OTEL_EXPORTER_OTLP_LOGS_*`、
`OTEL_EXPORTER_OTLP_METRICS_*`、`OTEL_BLRP_*`、
`OTEL_METRIC_EXPORT_INTERVAL` 和 `OTEL_METRIC_EXPORT_TIMEOUT` 环境变量，配置块中仍须
设置 `enabled: true`。
