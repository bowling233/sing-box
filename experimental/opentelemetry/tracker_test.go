package opentelemetry

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing/common/buf"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

func TestRoutedFlowLifecycle(t *testing.T) {
	sink := new(captureSink)
	reporter := &Reporter{sink: sink, flows: make(map[*Flow]struct{})}
	tracker := reporter.RoutedFlow(context.Background(), adapter.InboundContext{Network: N.NetworkUDP}, nil, nil)
	if tracker == nil {
		t.Fatal("missing flow tracker")
	}
	tracker.CountForward(0)
	tracker.CountReverse(5)
	tracker.CloseFlow(0)
	tracker.CountReverse(1)
	segments := sink.snapshot()
	if len(segments) != 1 || segments[0].uplinkDatagrams != 1 || segments[0].uplinkBytes != 0 || segments[0].downlinkDatagrams != 1 || segments[0].downlinkBytes != 5 {
		t.Fatalf("unexpected TUN flow segment: %#v", segments)
	}
}

type scriptedRead struct {
	data []byte
	err  error
}

type scriptedWrite struct {
	n   int
	err error
}

type scriptedConn struct {
	reads  []scriptedRead
	writes []scriptedWrite
}

func (c *scriptedConn) Read(buffer []byte) (int, error) {
	if len(c.reads) == 0 {
		return 0, io.EOF
	}
	result := c.reads[0]
	c.reads = c.reads[1:]
	return copy(buffer, result.data), result.err
}

func (c *scriptedConn) Write(buffer []byte) (int, error) {
	if len(c.writes) == 0 {
		return len(buffer), nil
	}
	result := c.writes[0]
	c.writes = c.writes[1:]
	if result.n < 0 || result.n > len(buffer) {
		result.n = len(buffer)
	}
	return result.n, result.err
}

func (c *scriptedConn) Close() error                     { return nil }
func (c *scriptedConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (c *scriptedConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (c *scriptedConn) SetDeadline(time.Time) error      { return nil }
func (c *scriptedConn) SetReadDeadline(time.Time) error  { return nil }
func (c *scriptedConn) SetWriteDeadline(time.Time) error { return nil }

func TestTrackedConnCountsAllIOPaths(t *testing.T) {
	sink := new(captureSink)
	reporter := &Reporter{sink: sink, flows: make(map[*Flow]struct{})}
	flow := &Flow{
		reporter: reporter,
		metadata: FlowMetadata{processUID: -1},
		id:       "tcp", network: "tcp", segmentStart: time.Unix(1, 0),
	}
	reporter.flows[flow] = struct{}{}
	conn := &scriptedConn{
		reads: []scriptedRead{
			{data: []byte("abc"), err: io.EOF},
			{data: []byte("def")},
		},
		writes: []scriptedWrite{
			{n: 2, err: io.ErrShortWrite},
			{n: -1},
		},
	}
	tracked := newTrackedConn(conn, flow)

	readBuffer := make([]byte, 8)
	if n, err := tracked.Read(readBuffer); n != 3 || err != io.EOF {
		t.Fatalf("Read returned (%d, %v)", n, err)
	}
	if n, err := tracked.Write([]byte("four")); n != 2 || err != io.ErrShortWrite {
		t.Fatalf("Write returned (%d, %v)", n, err)
	}
	buffer := buf.New()
	defer buffer.Release()
	if err := tracked.ReadBuffer(buffer); err != nil || buffer.Len() != 3 {
		t.Fatalf("ReadBuffer returned len=%d, err=%v", buffer.Len(), err)
	}
	if err := tracked.WriteBuffer(buf.As([]byte("data"))); err != nil {
		t.Fatal(err)
	}

	reader := tracked.ExtendedConn.(interface {
		UnwrapReader() (io.Reader, []N.CountFunc)
	})
	_, readCallbacks := reader.UnwrapReader()
	readCallbacks[0](7)
	writer := tracked.ExtendedConn.(interface {
		UnwrapWriter() (io.Writer, []N.CountFunc)
	})
	_, writeCallbacks := writer.UnwrapWriter()
	writeCallbacks[0](11)
	readUpstream, outerReadCallbacks := N.UnwrapCountReader(tracked, nil)
	if readUpstream != tracked || len(outerReadCallbacks) != 0 {
		t.Fatalf("unexpected reader unwrap: upstream=%T callbacks=%d", readUpstream, len(outerReadCallbacks))
	}
	writeUpstream, outerWriteCallbacks := N.UnwrapCountWriter(tracked, nil)
	if writeUpstream != tracked || len(outerWriteCallbacks) != 0 {
		t.Fatalf("unexpected writer unwrap: upstream=%T callbacks=%d", writeUpstream, len(outerWriteCallbacks))
	}
	if err := tracked.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tracked.Close(); err != nil {
		t.Fatal(err)
	}

	var uplink, downlink int64
	for _, segment := range sink.snapshot() {
		uplink += segment.uplinkBytes
		downlink += segment.downlinkBytes
	}
	if uplink != 13 || downlink != 17 {
		t.Fatalf("got uplink=%d downlink=%d", uplink, downlink)
	}
}

type packetRead struct {
	data []byte
	err  error
}

type scriptedPacketConn struct {
	reads    []packetRead
	writeErr []error
}

func (c *scriptedPacketConn) ReadPacket(buffer *buf.Buffer) (M.Socksaddr, error) {
	if len(c.reads) == 0 {
		return M.Socksaddr{}, io.EOF
	}
	result := c.reads[0]
	c.reads = c.reads[1:]
	_, _ = buffer.Write(result.data)
	return M.Socksaddr{}, result.err
}

func (c *scriptedPacketConn) WritePacket(*buf.Buffer, M.Socksaddr) error {
	if len(c.writeErr) == 0 {
		return nil
	}
	err := c.writeErr[0]
	c.writeErr = c.writeErr[1:]
	return err
}

func (c *scriptedPacketConn) Close() error                     { return nil }
func (c *scriptedPacketConn) LocalAddr() net.Addr              { return &net.UDPAddr{} }
func (c *scriptedPacketConn) SetDeadline(time.Time) error      { return nil }
func (c *scriptedPacketConn) SetReadDeadline(time.Time) error  { return nil }
func (c *scriptedPacketConn) SetWriteDeadline(time.Time) error { return nil }

func TestTrackedPacketConnCountsSuccessfulDatagrams(t *testing.T) {
	sink := new(captureSink)
	reporter := &Reporter{sink: sink, flows: make(map[*Flow]struct{})}
	flow := &Flow{
		reporter: reporter,
		metadata: FlowMetadata{processUID: -1},
		id:       "udp", network: "udp", udp: true, segmentStart: time.Unix(1, 0),
	}
	reporter.flows[flow] = struct{}{}
	conn := &scriptedPacketConn{
		reads:    []packetRead{{}, {data: []byte("hello")}, {data: []byte{1}, err: io.ErrUnexpectedEOF}},
		writeErr: []error{nil, nil, io.ErrClosedPipe},
	}
	tracked := newTrackedPacketConn(conn, flow)

	readBuffer := buf.NewPacket()
	defer readBuffer.Release()
	if _, err := tracked.ReadPacket(readBuffer); err != nil {
		t.Fatal(err)
	}
	successfulRead := buf.NewPacket()
	defer successfulRead.Release()
	if _, err := tracked.ReadPacket(successfulRead); err != nil {
		t.Fatalf("ReadPacket error = %v", err)
	}
	failedRead := buf.NewPacket()
	defer failedRead.Release()
	if _, err := tracked.ReadPacket(failedRead); err != io.ErrUnexpectedEOF {
		t.Fatalf("ReadPacket error = %v", err)
	}
	if err := tracked.WritePacket(buf.As(nil), M.Socksaddr{}); err != nil {
		t.Fatal(err)
	}
	if err := tracked.WritePacket(buf.As([]byte("success")), M.Socksaddr{}); err != nil {
		t.Fatal(err)
	}
	if err := tracked.WritePacket(buf.As([]byte{1}), M.Socksaddr{}); err != io.ErrClosedPipe {
		t.Fatalf("WritePacket error = %v", err)
	}
	readUpstream, readCallbacks := N.UnwrapCountPacketReader(tracked, nil)
	if readUpstream != tracked || len(readCallbacks) != 0 {
		t.Fatalf("unexpected reader unwrap: upstream=%T callbacks=%d", readUpstream, len(readCallbacks))
	}
	writeUpstream, writeCallbacks := N.UnwrapCountPacketWriter(tracked, nil)
	if writeUpstream != tracked || len(writeCallbacks) != 0 {
		t.Fatalf("unexpected writer unwrap: upstream=%T callbacks=%d", writeUpstream, len(writeCallbacks))
	}
	if err := tracked.Close(); err != nil {
		t.Fatal(err)
	}

	segments := sink.snapshot()
	if len(segments) != 1 {
		t.Fatalf("got %d segments: %#v", len(segments), segments)
	}
	segment := segments[0]
	if segment.uplinkBytes != 5 || segment.uplinkDatagrams != 2 || segment.downlinkBytes != 7 || segment.downlinkDatagrams != 2 {
		t.Fatalf("unexpected UDP segment: %#v", segment)
	}
}

type closeUnblocksReadConn struct {
	readStarted chan struct{}
	closed      chan struct{}
	startOnce   sync.Once
	closeOnce   sync.Once
}

func (c *closeUnblocksReadConn) Read(buffer []byte) (int, error) {
	c.startOnce.Do(func() { close(c.readStarted) })
	<-c.closed
	return copy(buffer, "abc"), io.EOF
}

func (c *closeUnblocksReadConn) Write(buffer []byte) (int, error) { return len(buffer), nil }
func (c *closeUnblocksReadConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return nil
}
func (c *closeUnblocksReadConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (c *closeUnblocksReadConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (c *closeUnblocksReadConn) SetDeadline(time.Time) error      { return nil }
func (c *closeUnblocksReadConn) SetReadDeadline(time.Time) error  { return nil }
func (c *closeUnblocksReadConn) SetWriteDeadline(time.Time) error { return nil }

func TestTrackedConnCloseWaitsForInFlightCount(t *testing.T) {
	sink := new(captureSink)
	reporter := &Reporter{sink: sink, flows: make(map[*Flow]struct{})}
	flow := &Flow{
		reporter: reporter,
		metadata: FlowMetadata{processUID: -1},
		id:       "tcp-close", network: "tcp", segmentStart: time.Unix(1, 0),
	}
	reporter.flows[flow] = struct{}{}
	upstream := &closeUnblocksReadConn{
		readStarted: make(chan struct{}),
		closed:      make(chan struct{}),
	}
	tracked := newTrackedConn(upstream, flow)

	type readResult struct {
		n   int
		err error
	}
	readDone := make(chan readResult, 1)
	go func() {
		buffer := make([]byte, 8)
		n, err := tracked.Read(buffer)
		readDone <- readResult{n: n, err: err}
	}()
	<-upstream.readStarted
	closeDone := make(chan error, 1)
	go func() { closeDone <- tracked.Close() }()

	select {
	case result := <-readDone:
		if result.n != 3 || result.err != io.EOF {
			t.Fatalf("Read returned (%d, %v)", result.n, result.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Read did not unblock after Close")
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not wait for the in-flight read")
	}

	segments := sink.snapshot()
	if len(segments) != 1 || segments[0].uplinkBytes != 3 || segments[0].downlinkBytes != 0 {
		t.Fatalf("unexpected final segments: %#v", segments)
	}
}
