package postgresql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	ComponentName = "postgresql"

	// DefaultConnectTimeout bounds the connectivity check performed at startup.
	DefaultConnectTimeout = 10 * time.Second

	// DefaultHealthTimeout bounds a health check made without a context.
	DefaultHealthTimeout = 2 * time.Second
)

type Component struct {
	Connection *pgxpool.Pool

	databaseURL    string
	poolSize       int
	logger         *logrus.Entry
	tracingEnabled bool

	registerPoolStatsOnce sync.Once
}

// ComponentOption is an option to `PostgreSQLComponent`.
type ComponentOption func(*Component)

// WithLogger sets the logger for the PostgreSQL component.
func WithLogger(logger *logrus.Entry) ComponentOption {
	return func(c *Component) {
		c.logger = logger.WithField("component", c.Name())
	}
}

// WithDatabaseURL sets the database URL for the PostgreSQL component.
func WithDatabaseURL(databaseURL string) ComponentOption {
	return func(c *Component) {
		c.databaseURL = databaseURL
	}
}

// WithPoolSize sets the pool size for the PostgreSQL component.
func WithPoolSize(poolSize int) ComponentOption {
	return func(c *Component) {
		c.poolSize = poolSize
	}
}

// WithTracing controls whether queries emit OpenTelemetry spans and duration
// metrics. Enabled by default.
func WithTracing(enabled bool) ComponentOption {
	return func(c *Component) {
		c.tracingEnabled = enabled
	}
}

func NewComponent(opts ...ComponentOption) *Component {
	c := &Component{tracingEnabled: true}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

// log returns the component logger, or a usable default when the component was
// built without one.
func (c *Component) log() *logrus.Entry {
	if c.logger == nil {
		return logrus.NewEntry(logrus.StandardLogger()).WithField("component", c.Name())
	}

	return c.logger
}

// Start implements the Component interface.
func (c *Component) Start() error {
	config, err := pgxpool.ParseConfig(c.databaseURL)
	if err != nil {
		return err
	}

	if c.poolSize <= 0 {
		return fmt.Errorf("pool size must be positive, got %d", c.poolSize)
	}

	config.MaxConns = int32(c.poolSize) //nolint:gosec // guarded above and bounded by configuration

	if c.tracingEnabled {
		config.ConnConfig.Tracer = QueryTracer{}
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return err
	}

	// Bound the connectivity check: an unreachable database would otherwise
	// hang startup indefinitely, with no log line explaining why.
	ctx, cancel := context.WithTimeout(context.Background(), DefaultConnectTimeout)
	defer cancel()

	if err = pool.Ping(ctx); err != nil {
		pool.Close()

		return fmt.Errorf("failed to reach the database within %s: %w", DefaultConnectTimeout, err)
	}

	c.Connection = pool

	// Registering once per component: a second Start would otherwise fail on a
	// duplicate collector, and there is nothing to be gained from that.
	c.registerPoolStatsOnce.Do(func() {
		if err := prometheus.Register(poolStatsCollector{component: c}); err != nil {
			c.log().Warnf("Failed to register the connection pool collector: %v", err)
		}
	})

	return nil
}

// Stop implements the Component interface.
func (c *Component) Stop() error {
	if c.Connection == nil {
		return nil
	}

	c.log().Info("Disconnecting from PostgreSQL...")

	c.Connection.Close()

	return nil
}

// Health implements the Component interface.
func (c *Component) Health() error {
	ctx, cancel := context.WithTimeout(context.Background(), DefaultHealthTimeout)
	defer cancel()

	return c.HealthContext(ctx)
}

// HealthContext implements the HealthCheckerContext interface.
func (c *Component) HealthContext(ctx context.Context) error {
	if c.Connection == nil {
		return errors.New("connection is not initialized")
	}

	return c.Connection.Ping(ctx)
}

// Name implements the Component interface.
func (c *Component) Name() string {
	return ComponentName
}

func NewNullTimeFromPbTimestamp(timestamp *timestamppb.Timestamp) sql.NullTime {
	result := sql.NullTime{}

	if timestamp != nil && (timestamp.GetSeconds() > 0 || timestamp.GetNanos() > 0) {
		_ = result.Scan(timestamp.AsTime())
	}

	return result
}

func NewNullInt32(num *int32) sql.NullInt32 {
	result := sql.NullInt32{}

	if num != nil {
		_ = result.Scan(*num)
	}

	return result
}

func NewNullInt64(num *int64) sql.NullInt64 {
	result := sql.NullInt64{}

	if num != nil {
		_ = result.Scan(*num)
	}

	return result
}

func NewNullString(str *string) sql.NullString {
	result := sql.NullString{}

	if str != nil {
		_ = result.Scan(*str)
	}

	return result
}

func NewNullUUID(uuidStr *string) uuid.NullUUID {
	result := uuid.NullUUID{}

	if uuidStr != nil {
		_ = result.Scan(*uuidStr)
	}

	return result
}
