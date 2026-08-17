package grpc

import (
	"context"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	fctx "github.com/foundation-go/foundation/context"
)

// LoggingUnaryInterceptor returns a gRPC unary interceptor that logs all incoming gRPC calls.
// It logs the method details, request, response, and any potential errors.
func LoggingUnaryInterceptor(log *logrus.Entry) func(context.Context, interface{}, *grpc.UnaryServerInfo, grpc.UnaryHandler) (interface{}, error) {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		// Enhance the log with request-related fields.
		//
		// N.B.: this MUST NOT assign back to `log` — that variable is captured
		// by the closure and shared by every in-flight call, so reassigning it
		// is a data race and leaks fields between concurrent calls.
		callLog := log.WithFields(logrus.Fields{
			"method":         info.FullMethod,
			"correlation_id": fctx.GetCorrelationID(ctx),
		})

		callLog.Info("Call started")
		callLog.WithField("request", req).Debug("Request")

		// Add logger to context
		ctx = fctx.WithLogger(ctx, callLog)

		// Call handler
		resp, err = handler(ctx, req)

		// Process handling error if any
		if err != nil {
			callLog.WithError(err).Error("Call failed")
			return nil, err
		}

		callLog.WithField("response", resp).Debug("Response")
		callLog.Info("Call finished")

		return resp, nil
	}
}
