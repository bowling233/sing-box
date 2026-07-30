package adapter

import (
	"context"
	"time"
)

type OutboundIdentity struct {
	Name string
	Type string
}

func IdentityOf(outbound Outbound) OutboundIdentity {
	if outbound == nil {
		return OutboundIdentity{}
	}
	return OutboundIdentity{Name: outbound.Tag(), Type: outbound.Type()}
}

type OutboundTelemetry interface {
	ObserveTransport(outbound OutboundIdentity, network string, direction string, bytes int64)
	ObserveHealthCheck(outbound Outbound, url string, latencyMS int64, completedAt time.Time)
}

type OutboundSelectionRecorder interface {
	RecordOutboundSelection(parent OutboundIdentity, selected OutboundIdentity)
	RecordOutboundLeaf(outbound OutboundIdentity)
}

type outboundIdentityContextKey struct{}
type outboundSelectionRecorderContextKey struct{}

func ContextWithOutboundIdentity(ctx context.Context, identity OutboundIdentity) context.Context {
	return context.WithValue(ctx, outboundIdentityContextKey{}, identity)
}

func OutboundIdentityFromContext(ctx context.Context) (OutboundIdentity, bool) {
	identity, loaded := ctx.Value(outboundIdentityContextKey{}).(OutboundIdentity)
	return identity, loaded && identity.Name != ""
}

func ContextWithOutboundSelectionRecorder(ctx context.Context, recorder OutboundSelectionRecorder) context.Context {
	if recorder == nil {
		return ctx
	}
	return context.WithValue(ctx, outboundSelectionRecorderContextKey{}, recorder)
}

func OutboundSelectionRecorderFromContext(ctx context.Context) OutboundSelectionRecorder {
	recorder, _ := ctx.Value(outboundSelectionRecorderContextKey{}).(OutboundSelectionRecorder)
	return recorder
}

func RecordOutboundSelection(ctx context.Context, parent OutboundIdentity, selected OutboundIdentity) {
	recorder := OutboundSelectionRecorderFromContext(ctx)
	if recorder != nil {
		recorder.RecordOutboundSelection(parent, selected)
	}
}

func RecordOutboundLeaf(ctx context.Context, outbound OutboundIdentity) {
	recorder := OutboundSelectionRecorderFromContext(ctx)
	if recorder != nil {
		recorder.RecordOutboundLeaf(outbound)
	}
}
