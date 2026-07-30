package opentelemetry

import (
	"context"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing/common/buf"
	"github.com/sagernet/sing/common/bufio"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

func (r *Reporter) RoutedConnection(_ context.Context, conn net.Conn, metadata adapter.InboundContext, matchedRule adapter.Rule, matchedOutbound adapter.Outbound) net.Conn {
	flow := r.newFlow(r.metadata(metadata, matchedRule, matchedOutbound), N.NetworkTCP, 0, 0)
	if flow == nil {
		return conn
	}
	return newTrackedConn(conn, flow)
}

func newTrackedConn(conn net.Conn, flow *Flow) *trackedConn {
	counter := bufio.NewCounterConn(conn, []N.CountFunc{func(n int64) {
		flow.addUplink(n, false)
	}}, []N.CountFunc{func(n int64) {
		flow.addDownlink(n, false)
	}})
	return &trackedConn{ExtendedConn: counter, flow: flow}
}

func (r *Reporter) RoutedPacketConnection(_ context.Context, conn N.PacketConn, metadata adapter.InboundContext, matchedRule adapter.Rule, matchedOutbound adapter.Outbound) N.PacketConn {
	flow := r.newFlow(r.metadata(metadata, matchedRule, matchedOutbound), N.NetworkUDP, 0, 0)
	if flow == nil {
		return conn
	}
	return newTrackedPacketConn(conn, flow)
}

func newTrackedPacketConn(conn N.PacketConn, flow *Flow) *trackedPacketConn {
	return &trackedPacketConn{PacketConn: conn, flow: flow}
}

type trackedConn struct {
	N.ExtendedConn
	flow            *Flow
	once            sync.Once
	operationAccess sync.RWMutex
	closed          atomic.Bool
}

func (c *trackedConn) Read(buffer []byte) (int, error) {
	c.operationAccess.RLock()
	defer c.operationAccess.RUnlock()
	if c.closed.Load() {
		return 0, net.ErrClosed
	}
	return c.ExtendedConn.Read(buffer)
}

func (c *trackedConn) ReadBuffer(buffer *buf.Buffer) error {
	c.operationAccess.RLock()
	defer c.operationAccess.RUnlock()
	if c.closed.Load() {
		return net.ErrClosed
	}
	return c.ExtendedConn.ReadBuffer(buffer)
}

func (c *trackedConn) Write(buffer []byte) (int, error) {
	c.operationAccess.RLock()
	defer c.operationAccess.RUnlock()
	if c.closed.Load() {
		return 0, net.ErrClosed
	}
	return c.ExtendedConn.Write(buffer)
}

func (c *trackedConn) WriteBuffer(buffer *buf.Buffer) error {
	c.operationAccess.RLock()
	defer c.operationAccess.RUnlock()
	if c.closed.Load() {
		return net.ErrClosed
	}
	return c.ExtendedConn.WriteBuffer(buffer)
}

func (c *trackedConn) Close() (err error) {
	c.once.Do(func() {
		c.closed.Store(true)
		err = c.ExtendedConn.Close()
		c.operationAccess.Lock()
		c.flow.snapshotNow("closed")
		c.operationAccess.Unlock()
	})
	return
}

func (c *trackedConn) Upstream() any           { return c.ExtendedConn }
func (c *trackedConn) ReaderReplaceable() bool { return false }
func (c *trackedConn) WriterReplaceable() bool { return false }

type trackedPacketConn struct {
	N.PacketConn
	flow            *Flow
	once            sync.Once
	operationAccess sync.RWMutex
	closed          atomic.Bool
}

func (c *trackedPacketConn) ReadPacket(buffer *buf.Buffer) (destination M.Socksaddr, err error) {
	c.operationAccess.RLock()
	defer c.operationAccess.RUnlock()
	if c.closed.Load() {
		return destination, net.ErrClosed
	}
	destination, err = c.PacketConn.ReadPacket(buffer)
	if err == nil {
		c.flow.addUplink(int64(buffer.Len()), true)
	}
	return
}

func (c *trackedPacketConn) WritePacket(buffer *buf.Buffer, destination M.Socksaddr) error {
	c.operationAccess.RLock()
	defer c.operationAccess.RUnlock()
	if c.closed.Load() {
		return net.ErrClosed
	}
	length := int64(buffer.Len())
	err := c.PacketConn.WritePacket(buffer, destination)
	if err == nil {
		c.flow.addDownlink(length, true)
	}
	return err
}

func (c *trackedPacketConn) Close() (err error) {
	c.once.Do(func() {
		c.closed.Store(true)
		err = c.PacketConn.Close()
		c.operationAccess.Lock()
		c.flow.snapshotNow("closed")
		c.operationAccess.Unlock()
	})
	return
}

func (c *trackedPacketConn) Upstream() any           { return c.PacketConn }
func (c *trackedPacketConn) ReaderReplaceable() bool { return false }
func (c *trackedPacketConn) WriterReplaceable() bool { return false }

func (f *Flow) snapshotNow(reason string) {
	f.snapshot(timeNow(), reason, true)
}

var timeNow = func() time.Time { return time.Now() }

func (r *Reporter) metadata(metadata adapter.InboundContext, matchedRule adapter.Rule, matchedOutbound adapter.Outbound) FlowMetadata {
	clientAddress, clientPort := socksAddress(metadata.Source)
	destinationAddress, destinationPort := socksAddress(metadata.Destination)
	if metadata.Domain != "" {
		destinationAddress = metadata.Domain
	}
	result := FlowMetadata{
		clientAddress:      clientAddress,
		clientPort:         clientPort,
		destinationAddress: destinationAddress,
		destinationPort:    destinationPort,
		inboundName:        metadata.Inbound,
		inboundType:        normalizeType(metadata.InboundType),
		protocolName:       normalizeType(metadata.Protocol),
		processUID:         -1,
	}
	if metadata.Source.Addr.IsValid() && metadata.Domain == "" && metadata.Destination.Addr.IsValid() {
		if metadata.Source.Addr.Is4() == metadata.Destination.Addr.Is4() {
			if metadata.Source.Addr.Is4() {
				result.networkType = "ipv4"
			} else {
				result.networkType = "ipv6"
			}
		}
	}
	original := metadata.RouteOriginalDestination
	if original.IsValid() {
		originalAddress, originalPort := socksAddress(original)
		if !sameEndpoint(destinationAddress, destinationPort, originalAddress, originalPort) {
			result.originalAddress = originalAddress
			result.originalPort = originalPort
		}
	}
	if len(metadata.DestinationAddresses) > 0 {
		result.resolvedAddresses = make([]string, 0, len(metadata.DestinationAddresses))
		for _, address := range metadata.DestinationAddresses {
			if address.IsValid() {
				result.resolvedAddresses = append(result.resolvedAddresses, address.String())
			}
		}
	}
	if process := metadata.ProcessInfo; process != nil {
		result.processPID = int64(process.ProcessID)
		result.processPath = process.ProcessPath
		if process.ProcessPath != "" {
			result.processName = filepath.Base(process.ProcessPath)
		}
		result.processOwner = process.UserName
		result.processUID = int64(process.UserId)
		result.androidPackages = append([]string(nil), process.AndroidPackageNames...)
	}
	result.outboundName, result.outboundType, result.outboundChain, result.egressName, result.egressType = r.outbound(matchedOutbound)
	if matchedRule != nil {
		result.ruleMatched = true
		result.ruleType = normalizeType(matchedRule.Type())
		result.ruleValue = matchedRule.String()
		result.ruleAction = result.outboundName
	}
	return result
}

func (r *Reporter) outbound(matched adapter.Outbound) (name, outboundType string, chain []string, egressName, egressType string) {
	if matched == nil {
		if r.outboundManager == nil {
			return
		}
		matched = r.outboundManager.Default()
	}
	if matched == nil {
		return
	}
	name = matched.Tag()
	outboundType = normalizeType(matched.Type())
	next := name
	seen := make(map[string]struct{})
	for len(chain) < 32 && next != "" {
		if _, exists := seen[next]; exists {
			break
		}
		seen[next] = struct{}{}
		outbound, loaded := r.outboundManager.Outbound(next)
		if !loaded {
			break
		}
		chain = append(chain, outbound.Tag())
		egressName = outbound.Tag()
		egressType = normalizeType(outbound.Type())
		group, isGroup := outbound.(adapter.OutboundGroup)
		if !isGroup {
			break
		}
		next = group.Now()
	}
	return
}

func normalizeType(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	characters := []rune(value)
	var builder strings.Builder
	lastUnderscore := false
	for index, character := range characters {
		if character == '-' || character == ' ' || character == '/' || character == '.' {
			if builder.Len() > 0 && !lastUnderscore {
				builder.WriteByte('_')
				lastUnderscore = true
			}
			continue
		}
		if character >= 'A' && character <= 'Z' {
			previousLower := index > 0 && characters[index-1] >= 'a' && characters[index-1] <= 'z'
			previousDigit := index > 0 && characters[index-1] >= '0' && characters[index-1] <= '9'
			nextLower := index+1 < len(characters) && characters[index+1] >= 'a' && characters[index+1] <= 'z'
			previousUpper := index > 0 && characters[index-1] >= 'A' && characters[index-1] <= 'Z'
			if builder.Len() > 0 && !lastUnderscore && (previousLower || previousDigit || previousUpper && nextLower) {
				builder.WriteByte('_')
			}
			builder.WriteRune(character + ('a' - 'A'))
			lastUnderscore = false
			continue
		}
		builder.WriteRune(character)
		lastUnderscore = false
	}
	return strings.Trim(builder.String(), "_")
}
