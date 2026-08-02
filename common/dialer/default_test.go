package dialer

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"
)

type transportObservation struct {
	outbound  adapter.OutboundIdentity
	network   string
	direction string
	bytes     int64
}

type telemetryCapture struct {
	access       sync.Mutex
	observations []transportObservation
}

func (c *telemetryCapture) ObserveTransport(outbound adapter.OutboundIdentity, network string, direction string, bytes int64) {
	c.access.Lock()
	c.observations = append(c.observations, transportObservation{outbound, network, direction, bytes})
	c.access.Unlock()
}

func (*telemetryCapture) ObserveHealthCheck(adapter.Outbound, string, int64, time.Time) {}

func (c *telemetryCapture) total(network string, direction string) int64 {
	c.access.Lock()
	defer c.access.Unlock()
	var total int64
	for _, observation := range c.observations {
		if observation.network == network && observation.direction == direction {
			total += observation.bytes
		}
	}
	return total
}

type selectionCapture struct {
	leaf adapter.OutboundIdentity
}

func (*selectionCapture) RecordOutboundSelection(adapter.OutboundIdentity, adapter.OutboundIdentity) {
}

func (c *selectionCapture) RecordOutboundLeaf(outbound adapter.OutboundIdentity) {
	c.leaf = outbound
}

func TestTransportCounterConn(t *testing.T) {
	telemetry := new(telemetryCapture)
	identity := adapter.OutboundIdentity{Name: "node-a", Type: "vless"}
	dialer := &DefaultDialer{telemetry: telemetry, outboundIdentity: identity}
	local, remote := net.Pipe()
	defer remote.Close()
	recorder := new(selectionCapture)
	ctx := adapter.ContextWithOutboundSelectionRecorder(context.Background(), recorder)
	tracked, err := dialer.trackConn(ctx, "tcp", M.Socksaddr{}, local, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tracked.Close()

	go func() { _, _ = remote.Write([]byte("abc")) }()
	buffer := make([]byte, 8)
	if n, readErr := tracked.Read(buffer); readErr != nil || n != 3 {
		t.Fatalf("read = (%d, %v)", n, readErr)
	}
	writeDone := make(chan error, 1)
	go func() {
		readBuffer := make([]byte, 8)
		_, readErr := remote.Read(readBuffer)
		writeDone <- readErr
	}()
	if n, writeErr := tracked.Write([]byte("four")); writeErr != nil || n != 4 {
		t.Fatalf("write = (%d, %v)", n, writeErr)
	}
	if err = <-writeDone; err != nil {
		t.Fatal(err)
	}
	if telemetry.total("tcp", "receive") != 3 || telemetry.total("tcp", "transmit") != 4 {
		t.Fatalf("unexpected observations: %#v", telemetry.observations)
	}
	if recorder.leaf != identity {
		t.Fatalf("leaf = %#v", recorder.leaf)
	}
}

func TestTransportCounterPacketConn(t *testing.T) {
	telemetry := new(telemetryCapture)
	identity := adapter.OutboundIdentity{Name: "node-b", Type: "hysteria2"}
	dialer := &DefaultDialer{telemetry: telemetry, outboundIdentity: identity}
	server, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	client, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tracked, err := dialer.trackPacketConn(context.Background(), M.Socksaddr{}, client, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tracked.Close()

	if _, err = server.WriteTo([]byte("abc"), tracked.LocalAddr()); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 8)
	if n, _, readErr := tracked.ReadFrom(buffer); readErr != nil || n != 3 {
		t.Fatalf("read = (%d, %v)", n, readErr)
	}
	if n, writeErr := tracked.WriteTo([]byte("four"), server.LocalAddr()); writeErr != nil || n != 4 {
		t.Fatalf("write = (%d, %v)", n, writeErr)
	}
	if telemetry.total("udp", "receive") != 3 || telemetry.total("udp", "transmit") != 4 {
		t.Fatalf("unexpected observations: %#v", telemetry.observations)
	}
}
