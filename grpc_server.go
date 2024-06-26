package foundation

import (
	"context"
	"fmt"
	"net"

	"github.com/getsentry/sentry-go"
	"google.golang.org/grpc"

	fg "github.com/foundation-go/foundation/grpc"
)

// GRPCServer represents a gRPC server mode Foundation service.
type GRPCServer struct {
	*Service

	Options *GRPCServerOptions
}

// InitGRPCServer initializes a new Foundation service in gRPC Server mode.
func InitGRPCServer(name string) *GRPCServer {
	return &GRPCServer{
		Service: Init(name),
	}
}

// GRPCServerOptions are the options to start a Foundation service in gRPC Server mode.
type GRPCServerOptions struct {
	// RegisterFunc is a function that registers the gRPC server implementation.
	RegisterFunc func(s *grpc.Server)

	// GRPCServerOptions are the gRPC server options to use.
	GRPCServerOptions []grpc.ServerOption

	// StartComponentsOptions are the options to start the components.
	StartComponentsOptions []StartComponentsOption
}

func NewGRPCServerOptions() *GRPCServerOptions {
	return &GRPCServerOptions{}
}

// Start initializes the Foundation service in gRPC server mode.
func (s *GRPCServer) Start(opts *GRPCServerOptions) {
	s.Options = opts

	s.Service.Start(&StartOptions{
		ModeName:               "grpc",
		StartComponentsOptions: s.Options.StartComponentsOptions,
		ServiceFunc:            s.ServiceFunc,
	})
}

func (s *GRPCServer) ServiceFunc(ctx context.Context) error {
	// Default interceptors
	//
	// N.B.: Interceptors are executed in the order they are defined.
	defaultInterceptors := []grpc.UnaryServerInterceptor{
		fg.MetadataUnaryInterceptor,
		fg.FoundationErrorToStatusUnaryInterceptor,
		fg.LoggingUnaryInterceptor(s.Logger),
	}

	// Construct the default server options
	defaultOptions := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(defaultInterceptors...),
	}

	// Prepend the default server options in front of the application-defined ones
	serverOptions := append(defaultOptions, s.Options.GRPCServerOptions...)

	// TLS
	if s.Config.GRPC.TLSDir != "" {
		s.Logger.Debugf("gRPC mTLS is enabled, loading certificates from %s", s.Config.GRPC.TLSDir)

		tlsConfig, err := fg.NewTLSConfig(s.Config.GRPC.TLSDir)
		if err != nil {
			return fmt.Errorf("failed to load TLS config: %w", err)
		}

		serverOptions = append(serverOptions, grpc.Creds(tlsConfig))
	} else if IsProductionEnv() {
		s.Logger.Warn("mTLS for gRPC server is not configured, it is strongly recommended to use mTLS in production")
	}

	// Start the server
	listener := s.acquireListener()
	server := grpc.NewServer(serverOptions...)

	s.Options.RegisterFunc(server)

	go func() {
		if err := server.Serve(listener); err != nil {
			err = fmt.Errorf("failed to start server: %w", err)
			sentry.CaptureException(err)
			s.Logger.Fatal(err)
		}
	}()

	<-ctx.Done()

	// Gracefully stop the server
	server.GracefulStop()

	return nil
}

func (s *Service) acquireListener() net.Listener {
	port := GetEnvOrInt("PORT", 51051)
	listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
	if err != nil {
		err = fmt.Errorf("failed to listen port %d: %w", port, err)
		sentry.CaptureException(err)
		s.Logger.Fatal(err)
	}

	s.Logger.Infof("Listening on http://0.0.0.0:%d", port)

	return listener
}
