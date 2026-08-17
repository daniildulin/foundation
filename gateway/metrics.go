package gateway

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/metadata"

	fmetrics "github.com/foundation-go/foundation/metrics"
)

// routeHolder carries the matched route pattern from grpc-gateway's routing
// back out to the metrics middleware, which wraps the mux and therefore runs
// before any routing has happened.
type routeHolder struct {
	route string
}

type routeHolderKey struct{}

// unmatchedRoute labels requests that never matched a route: 404s, and the
// endpoints served by middleware such as swagger.
const unmatchedRoute = "unmatched"

// RouteAnnotator records the route pattern grpc-gateway matched, so that HTTP
// metrics can be labelled by route rather than by request path.
//
// Labelling by path would put `/v1/chats/1` and `/v1/chats/2` in separate time
// series and let any client mint new ones at will; labelling by route keeps the
// cardinality bounded by the API surface.
//
// Foundation installs it as a grpc-gateway metadata annotator. It adds no
// metadata of its own.
func RouteAnnotator(ctx context.Context, req *http.Request) metadata.MD {
	holder, ok := req.Context().Value(routeHolderKey{}).(*routeHolder)
	if !ok {
		return nil
	}

	if pattern, found := runtime.HTTPPathPattern(ctx); found {
		holder.route = pattern
	}

	return nil
}

// WithMetrics records the request rate, error rate and duration of every
// request — the three numbers any investigation starts from, and none of which
// the framework used to expose.
func WithMetrics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()

		holder := &routeHolder{route: unmatchedRoute}
		r = r.WithContext(context.WithValue(r.Context(), routeHolderKey{}, holder))

		lrw := NewLoggingResponseWriter(w)

		fmetrics.HTTPRequestsInFlight.Inc()
		defer fmetrics.HTTPRequestsInFlight.Dec()

		next.ServeHTTP(wrapResponseWriter(lrw, w), r)

		// The annotator has run by now, on this goroutine, so reading the
		// holder needs no synchronisation.
		route := holder.route

		// The server span was opened before routing, so it could only be named
		// after the method. Now that the route is known, say so — a trace list
		// of bare "GET" is not much use.
		if span := trace.SpanFromContext(r.Context()); span.IsRecording() {
			span.SetName(r.Method + " " + route)
			span.SetAttributes(
				attribute.String("http.route", route),
				attribute.Int("http.status_code", lrw.Status()),
			)
		}

		fmetrics.HTTPRequests.
			WithLabelValues(r.Method, route, strconv.Itoa(lrw.statusCode)).
			Inc()

		fmetrics.HTTPRequestDuration.
			WithLabelValues(r.Method, route).
			Observe(time.Since(started).Seconds())
	})
}
