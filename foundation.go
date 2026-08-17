package foundation

import (
	"context"
	"fmt"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"

	fjobs "github.com/foundation-go/foundation/jobs"
	fkafka "github.com/foundation-go/foundation/kafka"
	fpg "github.com/foundation-go/foundation/postgresql"
	fredis "github.com/foundation-go/foundation/redis"
	fsentry "github.com/foundation-go/foundation/sentry"
)

const Version = "0.2.1"

// DefaultShutdownTimeout is how long the service spends draining work in flight
// before stopping its components.
const DefaultShutdownTimeout = 30 * time.Second

// Service represents a single microservice - part of the bigger Foundation-based application, which implements
// an isolated domain of the application logic.
type Service struct {
	Name       string
	Config     *Config
	Components []Component
	ModeName   string
	cancelFunc context.CancelFunc

	// draining is set once shutdown begins, so that the readiness probe can
	// report the service as unavailable before anything is torn down.
	draining atomic.Bool

	Logger *logrus.Entry
}

// Config represents the configuration of a Service.
type Config struct {
	Database     *DatabaseConfig
	EventsWorker *EventsWorkerConfig
	GRPC         *GRPCConfig
	Kafka        *KafkaConfig
	Metrics      *MetricsConfig
	Outbox       *OutboxConfig
	Redis        *RedisConfig
	Sentry       *SentryConfig
	JobsEnqueuer *JobsEnqueuerConfig

	// ShutdownTimeout bounds how long the service spends draining work in
	// flight before it stops its components. Set it comfortably below the
	// supervisor's own grace period (Kubernetes' terminationGracePeriodSeconds,
	// for instance), so that the service finishes on its own terms.
	ShutdownTimeout time.Duration

	// DrainDelay is how long the service keeps serving after it has started
	// failing its readiness probe, before it begins shutting down. It gives
	// load balancers time to notice and stop routing new requests here.
	// Default: 0, i.e. shut down immediately.
	DrainDelay time.Duration

	// HealthCheckTimeout bounds a single readiness probe.
	HealthCheckTimeout time.Duration
}

// DatabaseConfig represents the configuration of a PostgreSQL database.
type DatabaseConfig struct {
	Enabled bool
	Pool    int
	URL     string
}

// EventsWorkerConfig represents the configuration of an event bus.
type EventsWorkerConfig struct {
	// ErrorsTopic is the name of the Kafka topic to which errors from the
	// events worker handlers should be published.
	ErrorsTopic string

	// DeliverErrors determines whether errors from events worker handlers
	// should be published to the errors topic (and thus, delivered
	// to originator, aka user) or not.
	DeliverErrors bool
}

// GRPCConfig represents the configuration of a gRPC server.
type GRPCConfig struct {
	TLSDir string
}

// KafkaConfig represents the configuration of a Kafka client.
type KafkaConfig struct {
	Brokers  []string
	SASL     *KafkaSASLConfig
	Consumer *KafkaConsumerConfig
	Producer *KafkaProducerConfig
	TLSDir   string
}

// KafkaSASLConfig represents the configuration of a Kafka consumer.
type KafkaSASLConfig struct {
	Username string
	Password string
	Protocol string
}

// KafkaConsumerConfig represents the configuration of a Kafka consumer.
type KafkaConsumerConfig struct {
	Enabled bool
	Topics  []string
}

// KafkaProducerConfig represents the configuration of a Kafka producer.
type KafkaProducerConfig struct {
	Enabled      bool
	BatchSize    int
	BatchTimeout int
}

// MetricsConfig represents the configuration of a metrics server.
type MetricsConfig struct {
	Enabled bool
	Port    int
}

// SentryConfig represents the configuration of a Sentry client.
type SentryConfig struct {
	DSN     string
	Enabled bool
	// Environment separates events coming from development, test and
	// production. Defaults to FOUNDATION_ENV.
	Environment string
	// Release is reported with every event. Defaults to SENTRY_RELEASE.
	Release string
	// FlushTimeout is how long the service waits for buffered events to reach
	// Sentry before exiting.
	FlushTimeout time.Duration
}

// OutboxConfig represents the configuration of an outbox.
type OutboxConfig struct {
	Enabled bool
}

// RedisConfig represents the configuration of a Redis client.
type RedisConfig struct {
	Enabled bool
	URL     string
}

// JobsEnqueuerConfig represents the configuration of a jobs enqueuer.
type JobsEnqueuerConfig struct {
	Enabled   bool
	URL       string
	Pool      int
	Namespace string
}

// NewConfig returns a new Config with values populated from environment variables.
func NewConfig() *Config {
	return &Config{
		Database: &DatabaseConfig{
			Enabled: len(GetEnvOrString("DATABASE_URL", "")) > 0,
			Pool:    GetEnvOrInt("DATABASE_POOL", 5),
			URL:     GetEnvOrString("DATABASE_URL", ""),
		},
		EventsWorker: &EventsWorkerConfig{
			ErrorsTopic:   GetEnvOrString("EVENTS_WORKER_ERRORS_TOPIC", "foundation.events_worker.errors"),
			DeliverErrors: GetEnvOrBool("EVENTS_WORKER_DELIVER_ERRORS", true),
		},
		GRPC: &GRPCConfig{
			TLSDir: GetEnvOrString("GRPC_TLS_DIR", ""),
		},
		Kafka: &KafkaConfig{
			Brokers: strings.Split(GetEnvOrString("KAFKA_BROKERS", ""), ","),
			SASL: &KafkaSASLConfig{
				Username: GetEnvOrString("KAFKA_SASL_USERNAME", ""),
				Password: GetEnvOrString("KAFKA_SASL_PASSWORD", ""),
				Protocol: GetEnvOrString("KAFKA_SASL_PROTOCOL", ""),
			},
			Consumer: &KafkaConsumerConfig{
				Enabled: false,
				Topics:  nil,
			},
			Producer: &KafkaProducerConfig{
				Enabled:      false,
				BatchSize:    GetEnvOrInt("KAFKA_PRODUCER_BATCH_SIZE", 1),
				BatchTimeout: GetEnvOrInt("KAFKA_PRODUCER_BATCH_TIMEOUT", 1),
			},
			TLSDir: GetEnvOrString("KAFKA_TLS_DIR", ""),
		},
		Metrics: &MetricsConfig{
			Enabled: GetEnvOrBool("METRICS_ENABLED", true),
			Port:    GetEnvOrInt("METRICS_PORT", 51077),
		},
		Outbox: &OutboxConfig{
			Enabled: false,
		},
		Redis: &RedisConfig{
			Enabled: len(GetEnvOrString("REDIS_URL", "")) > 0,
			URL:     GetEnvOrString("REDIS_URL", ""),
		},
		Sentry: &SentryConfig{
			DSN:          GetEnvOrString("SENTRY_DSN", ""),
			Enabled:      len(GetEnvOrString("SENTRY_DSN", "")) > 0,
			Environment:  GetEnvOrString("SENTRY_ENVIRONMENT", string(FoundationEnv())),
			Release:      GetEnvOrString("SENTRY_RELEASE", ""),
			FlushTimeout: GetEnvOrDuration("SENTRY_FLUSH_TIMEOUT", fsentry.DefaultFlushTimeout),
		},
		JobsEnqueuer: &JobsEnqueuerConfig{
			Enabled:   false,
			URL:       GetEnvOrString("REDIS_URL", ""),
			Pool:      GetEnvOrInt("REDIS_POOL", 5),
			Namespace: GetEnvOrString("REDIS_NAMESPACE", ""),
		},
		ShutdownTimeout:    GetEnvOrDuration("SHUTDOWN_TIMEOUT", DefaultShutdownTimeout),
		DrainDelay:         GetEnvOrDuration("DRAIN_DELAY", 0),
		HealthCheckTimeout: GetEnvOrDuration("HEALTH_CHECK_TIMEOUT", DefaultHealthCheckTimeout),
	}
}

// shutdownTimeout returns the configured shutdown budget, falling back to the
// default for services constructed without a Config.
func (s *Service) shutdownTimeout() time.Duration {
	if s.Config != nil && s.Config.ShutdownTimeout > 0 {
		return s.Config.ShutdownTimeout
	}

	return DefaultShutdownTimeout
}

// Init initializes the Foundation service.
func Init(name string) *Service {
	return &Service{
		Name:   name,
		Config: NewConfig(),
		Logger: initLogger(name),
	}
}

// StartComponentsOption is an option to `StartComponents`.
type StartComponentsOption func(*Service)

// WithKafkaConsumer sets the Kafka consumer enabled flag.
func WithKafkaConsumer() StartComponentsOption {
	return func(s *Service) {
		s.Config.Kafka.Consumer.Enabled = true
	}
}

// WithKafkaProducer sets the Kafka producer enabled flag.
func WithKafkaProducer() StartComponentsOption {
	return func(s *Service) {
		s.Config.Kafka.Producer.Enabled = true
	}
}

// WithKafkaConsumerTopics sets the Kafka consumer topics.
func WithKafkaConsumerTopics(topics ...string) StartComponentsOption {
	return func(s *Service) {
		s.Config.Kafka.Consumer.Topics = topics
	}
}

// WithOutbox sets the outbox enabled flag.
func WithOutbox() StartComponentsOption {
	return func(s *Service) {
		s.Config.Outbox.Enabled = true
	}
}

// WithRedis sets the redis enabled flag.
func WithRedis() StartComponentsOption {
	return func(s *Service) {
		s.Config.Redis.Enabled = true
	}
}

// WithJobsEnqueuer sets the jobs enqueuer enabled flag.
func WithJobsEnqueuer() StartComponentsOption {
	return func(s *Service) {
		s.Config.JobsEnqueuer.Enabled = true
	}
}

func (s *Service) addSystemComponents() error {
	// Remove user-defined components in order to add system components first.
	existedComponents := s.Components
	s.Components = []Component{}

	// Sentry
	if s.Config.Sentry.Enabled {
		s.Components = append(s.Components, fsentry.NewComponent(
			s.Config.Sentry.DSN,
			fsentry.WithEnvironment(s.Config.Sentry.Environment),
			fsentry.WithRelease(s.Config.Sentry.Release),
			fsentry.WithFlushTimeout(s.Config.Sentry.FlushTimeout),
		))
	}

	// PostgreSQL
	if s.Config.Database.Enabled {
		s.Components = append(s.Components, fpg.NewComponent(
			fpg.WithDatabaseURL(s.Config.Database.URL),
			fpg.WithLogger(s.Logger),
			fpg.WithPoolSize(s.Config.Database.Pool),
		))
	}

	// Kafka consumer
	if s.Config.Kafka.Consumer.Enabled {
		consumerComponents := make([]fkafka.ConsumerComponentOption, 5, 6)
		consumerComponents[0] = fkafka.WithConsumerAppName(s.Name)
		consumerComponents[1] = fkafka.WithConsumerBrokers(s.Config.Kafka.Brokers)
		consumerComponents[2] = fkafka.WithConsumerLogger(s.Logger)
		consumerComponents[3] = fkafka.WithConsumerTLSDir(s.Config.Kafka.TLSDir)
		consumerComponents[4] = fkafka.WithConsumerTopics(s.Config.Kafka.Consumer.Topics)

		if s.Config.Kafka.SASL.Username != "" && s.Config.Kafka.SASL.Password != "" {
			saslComponent, err := fkafka.WithSASLMechanism(s.Config.Kafka.SASL.Protocol, s.Config.Kafka.SASL.Username, s.Config.Kafka.SASL.Password)
			if err != nil {
				return err
			}
			consumerComponents = append(consumerComponents, saslComponent)
		}

		s.Components = append(s.Components, fkafka.NewConsumerComponent(consumerComponents...))
	}

	// Kafka producer
	if s.Config.Kafka.Producer.Enabled {
		producerComponents := make([]fkafka.ProducerComponentOption, 5, 6)
		producerComponents[0] = fkafka.WithProducerBrokers(s.Config.Kafka.Brokers)
		producerComponents[1] = fkafka.WithProducerLogger(s.Logger)
		producerComponents[2] = fkafka.WithProducerTLSDir(s.Config.Kafka.TLSDir)
		producerComponents[3] = fkafka.WithProducerBatchSize(s.Config.Kafka.Producer.BatchSize)
		producerComponents[4] = fkafka.WithProducerBatchTimeout(time.Duration(s.Config.Kafka.Producer.BatchTimeout) * time.Second)

		if s.Config.Kafka.SASL.Username != "" && s.Config.Kafka.SASL.Password != "" {
			producerSASLComponent, err := fkafka.WithProducerSASLMechanism(s.Config.Kafka.SASL.Protocol, s.Config.Kafka.SASL.Username, s.Config.Kafka.SASL.Password)
			if err != nil {
				return err
			}
			producerComponents = append(producerComponents, producerSASLComponent)
		}

		s.Components = append(s.Components, fkafka.NewProducerComponent(producerComponents...))
	}

	// Metrics server
	if s.Config.Metrics.Enabled {
		s.Components = append(s.Components, NewMetricsServerComponent(
			WithMetricsServerHealthHandler(s.healthHandler),
			WithMetricsServerLivenessHandler(s.livenessHandler),
			WithMetricsServerReadinessHandler(s.readinessHandler),
			WithMetricsServerLogger(s.Logger),
			WithMetricsServerPort(s.Config.Metrics.Port),
		))
	}

	// Redis
	if s.Config.Redis.Enabled {
		s.Components = append(s.Components, fredis.NewComponent(
			fredis.WithLogger(s.Logger),
			fredis.WithURL(s.Config.Redis.URL),
		))
	}

	if s.Config.JobsEnqueuer.Enabled {
		redisPool, err := BuildRedisPool(s.Config.JobsEnqueuer.URL, s.Config.JobsEnqueuer.Pool)
		if err != nil {
			return fmt.Errorf("failed to initialize redis pool: %w", err)
		}

		s.Components = append(s.Components, fjobs.NewComponent(
			fjobs.WithLogger(s.Logger),
			fjobs.WithRedisPool(redisPool),
			fjobs.WithNamespace(s.Config.JobsEnqueuer.Namespace),
		))
	}

	// Add user-defined components back
	s.Components = append(s.Components, existedComponents...)

	return nil
}

// StartComponents starts the default Foundation service components.
func (s *Service) StartComponents(opts ...StartComponentsOption) error {
	// Apply options
	for _, opt := range opts {
		opt(s)
	}

	if err := s.addSystemComponents(); err != nil {
		return err
	}

	s.Logger.Info("Starting components:")

	started := make([]Component, 0, len(s.Components))

	for _, component := range s.Components {
		s.Logger.Infof(" - %s", component.Name())

		if err := component.Start(); err != nil {
			// Unwind what is already running. Otherwise a service that fails to
			// start halfway through leaves database connections, Kafka readers
			// and listening sockets behind it.
			s.stopComponents(started)

			return fmt.Errorf("%s: %w", component.Name(), err)
		}

		started = append(started, component)
	}

	return nil
}

// Shutdown asks the service to begin a graceful shutdown, as if it had received
// SIGTERM. It is safe to call before Start and to call more than once.
func (s *Service) Shutdown() {
	if s.cancelFunc != nil {
		s.cancelFunc()
	}
}

// StopComponents stops the default Foundation service components.
//
// It is bounded by Config.ShutdownTimeout: a component that refuses to stop
// must not keep the process alive until the supervisor kills it, because then
// nothing after this point — flushing Sentry, for one — gets to run.
func (s *Service) StopComponents() {
	done := make(chan struct{})

	go func() {
		defer close(done)

		s.stopComponents(s.Components)
	}()

	timeout := s.shutdownTimeout()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-done:
	case <-timer.C:
		s.Logger.Errorf("Components did not stop within %s; exiting anyway", timeout)
	}
}

// stopComponents stops the given components in reverse order, so that
// dependents go down before their dependencies.
func (s *Service) stopComponents(components []Component) {
	if len(components) == 0 {
		return
	}

	s.Logger.Info("Stopping components:")

	for i := len(components) - 1; i >= 0; i-- {
		s.stopComponent(components[i])
	}
}

// stopComponent stops a single component, containing both errors and panics:
// one misbehaving component must not prevent the rest from shutting down.
func (s *Service) stopComponent(component Component) {
	name := component.Name()

	defer func() {
		if r := recover(); r != nil {
			s.CaptureError(
				fmt.Errorf("panic while stopping component `%s`: %v", name, r),
				"",
			)
		}
	}()

	s.Logger.Infof(" - %s", name)

	if err := component.Stop(); err != nil {
		s.CaptureError(err, fmt.Sprintf("failed to stop component `%s`", name))
	}
}

type StartOptions struct {
	ModeName               string
	StartComponentsOptions []StartComponentsOption
	ServiceFunc            func(ctx context.Context) error
}

// Start runs the Foundation service.
func (s *Service) Start(opts *StartOptions) {
	s.ModeName = opts.ModeName

	// Set running mode to logger
	s.Logger = s.Logger.WithField("mode", s.ModeName)

	// Log application startup
	s.logStartup()

	// Start common components
	if err := s.StartComponents(opts.StartComponentsOptions...); err != nil {
		s.Fatal(err, "failed to start components")
	}

	signalCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	s.cancelFunc = stop

	// The service context lags the signal by the drain delay. Readiness starts
	// failing the moment the signal arrives, but servers keep accepting for a
	// little longer, so that load balancers notice before connections are
	// refused rather than after.
	serviceCtx, stopServing := context.WithCancel(context.WithoutCancel(signalCtx))
	defer stopServing()

	go func() {
		<-signalCtx.Done()

		s.draining.Store(true)

		if delay := s.drainDelay(); delay > 0 {
			s.Logger.Infof("Draining for %s before shutting down", delay)
			time.Sleep(delay)
		}

		stopServing()
	}()

	// Run the actual service code
	if err := opts.ServiceFunc(serviceCtx); err != nil {
		s.Fatal(err, "failed to start service")
	}

	<-serviceCtx.Done()
	s.Logger.Println("Shutting down service...")

	s.StopComponents()

	s.Logger.Println("Service gracefully stopped")
}
