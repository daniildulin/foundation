package gateway

import (
	"net/http"
	"net/http/httptest"
	"testing"

	gwruntime "github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc/metadata"
)

func TestProbeGrpcMetadataAlias(t *testing.T) {
	mux := gwruntime.NewServeMux(
		gwruntime.WithIncomingHeaderMatcher(IncomingHeaderMatcher),
		gwruntime.WithOutgoingHeaderMatcher(OutgoingHeaderMatcher),
	)

	// Simulate what the gateway does: strip identity headers, then (optionally)
	// no auth details middleware at all.
	req := httptest.NewRequest(http.MethodPost, "/x", nil)
	req.Header.Set("X-User-Id", "11111111-1111-1111-1111-111111111111")
	req.Header.Set("X-Authenticated", "true")
	req.Header.Set("Grpc-Metadata-X-User-Id", "22222222-2222-2222-2222-222222222222")
	req.Header.Set("Grpc-Metadata-X-Authenticated", "true")
	req.Header.Set("Grpc-Metadata-X-Scope", "admin")

	var strippedHandler http.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, err := gwruntime.AnnotateIncomingContext(r.Context(), mux, r, "/svc/Method")
		if err != nil {
			t.Fatalf("annotate: %v", err)
		}
		md, _ := metadata.FromIncomingContext(ctx)
		t.Logf("metadata: %#v", md)
	})
	strippedHandler = StripClientAuthenticationHeaders(strippedHandler)

	strippedHandler.ServeHTTP(httptest.NewRecorder(), req)
}

func TestProbeMatcher(t *testing.T) {
	for _, k := range []string{
		"Grpc-Metadata-X-User-Id",
		"Grpc-Metadata-X-Authenticated",
		"grpc-metadata-x-user-id",
		"X-User-Id",
	} {
		h, ok := IncomingHeaderMatcher(k)
		t.Logf("%-32s -> %q %v", k, h, ok)
	}
}
