package otelenv

import (
	"strings"

	"go.opentelemetry.io/otel/attribute"
)

// Value of OTEL_RESOURCE_ATTRIBUTES for key value list.
func Value(values ...attribute.KeyValue) string {
	var parts []string
	for _, kv := range values {
		parts = append(parts, string(kv.Key)+"="+kv.Value.AsString())
	}
	return strings.Join(parts, ",")
}
