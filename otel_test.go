package foundation

import "testing"

func TestGetTracingEndpointPrefersTracesEndpoint(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", " http://localhost:4318/v1/traces ")

	if got := getTracingEndpoint(); got != "http://localhost:4318/v1/traces" {
		t.Fatalf("expected traces endpoint to win, got %q", got)
	}
}

func TestGetTracingEndpointFallsBackToGenericEndpoint(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", " http://localhost:4317 ")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")

	if got := getTracingEndpoint(); got != "http://localhost:4317" {
		t.Fatalf("expected generic endpoint, got %q", got)
	}
}

func TestGetTracingTransportPrefersExplicitProtocol(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")

	if got := getTracingTransport("http://localhost:4317"); got != otlpTransportHTTP {
		t.Fatalf("expected http transport, got %q", got)
	}
}

func TestGetTracingTransportInfersHTTPFromPort4318(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "")

	if got := getTracingTransport("http://localhost:4318"); got != otlpTransportHTTP {
		t.Fatalf("expected http transport, got %q", got)
	}
}

func TestGetTracingTransportInfersHTTPFromTracesPath(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "")

	if got := getTracingTransport("http://localhost:9999/v1/traces"); got != otlpTransportHTTP {
		t.Fatalf("expected http transport, got %q", got)
	}
}

func TestGetTracingTransportDefaultsToGRPC(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "")

	if got := getTracingTransport("http://localhost:4317"); got != otlpTransportGRPC {
		t.Fatalf("expected grpc transport, got %q", got)
	}
}
