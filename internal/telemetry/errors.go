package telemetry

import (
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// MarkError records only a stable Go error type and a caller-owned safe
// description. Raw error messages may contain URLs or connection details and
// must not be exported to Tempo.
func MarkError(span trace.Span, err error, safeDescription string) {
	span.SetAttributes(attribute.String("error.type", fmt.Sprintf("%T", err)))
	span.SetStatus(codes.Error, safeDescription)
}
