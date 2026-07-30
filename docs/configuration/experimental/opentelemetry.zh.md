# OpenTelemetry 流量上报

OpenTelemetry 流量上报器把数据平面内原生统计的逐流 payload 流量以
OTLP/HTTP protobuf LogRecord 导出，不依赖 Clash API。

每个 TCP 连接或 UDP 会话会拆成 `uplink`、`downlink` 两个方向的
`proxy.flow` 记录；长连接按 active timeout 上报增量。TCP 不输出无法准确获得的包数，
UDP 输出 datagram 数。已获得的规则和进程属性总是记录，入站认证用户永不记录。
启用本功能不会额外开启进程查询；需要时请使用现有的路由进程查询设置。

### 结构

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

### 字段

#### enabled

启用流量上报，默认关闭。`OTEL_SDK_DISABLED=true` 也会禁用本功能。

#### endpoint

完整的 OTLP Logs URL，默认为 `http://127.0.0.1:4318/v1/logs`。字段为空时使用
标准的 signal-specific 或通用 OTLP 环境变量。

#### protocol

目前仅支持 `http/protobuf`。

#### headers

附加的 OTLP HTTP headers。

#### compression

可设为空、`none` 或 `gzip`。

#### timeout

单次导出请求超时；exporter 默认值为 `10s`。

#### active_timeout

长连接增量记录周期，默认为 `60s`，有效范围为 `10s` 至 `24h`。

#### batch

批处理参数依次默认为 `1s`、`30s`、`2048`、`512`。
`max_export_batch_size` 不得大于 `max_queue_size`。

#### tls

可配置自定义服务端 CA、mTLS 客户端证书/私钥和跳过证书校验。
`insecure_skip_verify` 仅应在测试中使用。

#### resource_attributes

附加的字符串 OpenTelemetry Resource 属性。上报器始终设置且不允许覆盖
`proxy.flow.schema.version`。

上报器也支持标准的 `OTEL_SERVICE_NAME`、`OTEL_RESOURCE_ATTRIBUTES`、
`OTEL_EXPORTER_OTLP_*`、`OTEL_EXPORTER_OTLP_LOGS_*` 和 `OTEL_BLRP_*`
环境变量，但仍须在配置块中显式设置 `enabled: true`。
