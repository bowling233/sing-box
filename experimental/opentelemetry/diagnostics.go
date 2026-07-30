package opentelemetry

import (
	"fmt"
	"strings"
	"sync"
	"time"

	boxLog "github.com/sagernet/sing-box/log"

	"github.com/go-logr/logr"
	"go.opentelemetry.io/otel"
)

const diagnosticInterval = 30 * time.Second

type diagnosticState struct {
	access     sync.Mutex
	last       map[string]time.Time
	suppressed map[string]int
}

type diagnosticSink struct {
	logger boxLog.ContextLogger
	state  *diagnosticState
	name   string
	values []any
}

func installDiagnostics(logger boxLog.ContextLogger) {
	otel.SetLogger(logr.New(&diagnosticSink{
		logger: logger,
		state: &diagnosticState{
			last:       make(map[string]time.Time),
			suppressed: make(map[string]int),
		},
	}))
}

func (s *diagnosticSink) Init(logr.RuntimeInfo) {}

func (s *diagnosticSink) Enabled(level int) bool { return level <= 1 }

func (s *diagnosticSink) Info(level int, message string, values ...any) {
	// sdk/log v0.14 emits this once even when its internal dropped count is zero.
	if level > 1 || message == "limit reached: dropping log Record attributes" {
		return
	}
	if suppressed, ok := s.allow("warn:" + message); ok {
		s.logger.Warn("OpenTelemetry: ", s.message(message, suppressed, values))
	}
}

func (s *diagnosticSink) Error(err error, message string, values ...any) {
	if suppressed, ok := s.allow("error:" + message); ok {
		s.logger.Error("OpenTelemetry: ", s.message(message, suppressed, values), ": ", err)
	}
}

func (s *diagnosticSink) WithValues(values ...any) logr.LogSink {
	clone := *s
	clone.values = append(append([]any(nil), s.values...), values...)
	return &clone
}

func (s *diagnosticSink) WithName(name string) logr.LogSink {
	clone := *s
	if clone.name == "" {
		clone.name = name
	} else if name != "" {
		clone.name += "/" + name
	}
	return &clone
}

func (s *diagnosticSink) allow(key string) (int, bool) {
	now := time.Now()
	s.state.access.Lock()
	defer s.state.access.Unlock()
	if last := s.state.last[key]; !last.IsZero() && now.Sub(last) < diagnosticInterval {
		s.state.suppressed[key]++
		return 0, false
	}
	suppressed := s.state.suppressed[key]
	s.state.suppressed[key] = 0
	s.state.last[key] = now
	return suppressed, true
}

func (s *diagnosticSink) message(message string, suppressed int, values []any) string {
	var builder strings.Builder
	if s.name != "" {
		builder.WriteString(s.name)
		builder.WriteString(": ")
	}
	builder.WriteString(message)
	writeDiagnosticValues(&builder, s.values)
	writeDiagnosticValues(&builder, values)
	if suppressed > 0 {
		fmt.Fprintf(&builder, " (suppressed %d similar messages)", suppressed)
	}
	return builder.String()
}

func writeDiagnosticValues(builder *strings.Builder, values []any) {
	for index := 0; index+1 < len(values); index += 2 {
		key, ok := values[index].(string)
		if !ok || !safeDiagnosticKey(key) {
			continue
		}
		fmt.Fprintf(builder, " %s=%v", key, values[index+1])
	}
}

func safeDiagnosticKey(key string) bool {
	switch strings.ToLower(key) {
	case "count", "dropped", "rejected", "status", "status_code":
		return true
	default:
		return false
	}
}
