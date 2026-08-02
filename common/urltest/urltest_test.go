package urltest

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/sagernet/sing/service"
)

type testOutbound struct {
	address string
}

func (*testOutbound) Type() string           { return "test" }
func (*testOutbound) Tag() string            { return "node-a" }
func (*testOutbound) Network() []string      { return []string{"tcp"} }
func (*testOutbound) Dependencies() []string { return nil }
func (o *testOutbound) DialContext(ctx context.Context, network string, _ M.Socksaddr) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(ctx, network, o.address)
}
func (*testOutbound) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, net.ErrClosed
}

type healthObservation struct {
	outbound    adapter.Outbound
	url         string
	latencyMS   int64
	completedAt time.Time
}

type telemetryCapture struct {
	access sync.Mutex
	point  *healthObservation
}

func (*telemetryCapture) ObserveTransport(adapter.OutboundIdentity, string, string, int64) {}

func (c *telemetryCapture) ObserveHealthCheck(outbound adapter.Outbound, url string, latencyMS int64, completedAt time.Time) {
	c.access.Lock()
	c.point = &healthObservation{outbound, url, latencyMS, completedAt}
	c.access.Unlock()
}

func TestURLTestRecordsSuccessfulResult(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	serverAddress := server.Listener.Addr().String()
	outbound := &testOutbound{address: serverAddress}
	capture := new(telemetryCapture)
	ctx := service.ContextWithDefaultRegistry(context.Background())
	service.MustRegister[adapter.OutboundTelemetry](ctx, capture)
	startedAt := time.Now()
	delay, err := URLTest(ctx, server.URL, outbound)
	if err != nil {
		t.Fatal(err)
	}
	capture.access.Lock()
	point := capture.point
	capture.access.Unlock()
	if point == nil {
		t.Fatal("successful URL test was not observed")
	}
	if point.outbound != outbound || point.url != server.URL || point.latencyMS != int64(delay) || point.completedAt.Before(startedAt) {
		t.Fatalf("unexpected health point: %#v", point)
	}
}
