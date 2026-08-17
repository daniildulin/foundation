package foundation

import (
	"fmt"
	"strings"
	"time"

	"github.com/gomodule/redigo/redis"
)

const (
	// redisPoolIdleTimeout closes connections that have been idle longer than
	// this. Without it the pool hands out sockets a server-side timeout or a
	// NAT table has already dropped.
	redisPoolIdleTimeout = 5 * time.Minute

	// redisPoolMaxConnLifetime bounds how long a single connection is reused.
	redisPoolMaxConnLifetime = time.Hour

	// redisPoolHealthCheckAfter is how stale a connection may be before it is
	// verified with a PING on checkout.
	redisPoolHealthCheckAfter = time.Minute

	// redisDialTimeout bounds establishing a connection.
	redisDialTimeout = 5 * time.Second
)

// AddSuffix adds a suffix to a string, if it doesn't already have it.
func AddSuffix(s string, suffix string) string {
	if s == "" {
		return suffix
	}

	if strings.HasSuffix(s, suffix) {
		return s
	}

	return fmt.Sprintf("%s-%s", s, suffix)
}

// BuildRedisPool returns a redigo connection pool for the given Redis URL.
//
// It dials through redis.DialURL, which understands the whole URL: the database
// number in the path, credentials, and the `rediss://` scheme for TLS. The
// previous implementation reassembled `host:port` by hand, so the database
// number and TLS were silently dropped and a URL without an explicit port
// produced an address ending in a bare colon.
func BuildRedisPool(url string, poolSize int) (*redis.Pool, error) {
	if strings.TrimSpace(url) == "" {
		return nil, fmt.Errorf("redis URL is empty: set REDIS_URL")
	}

	if poolSize <= 0 {
		poolSize = DefaultRedisPoolSize
	}

	pool := &redis.Pool{
		MaxActive:       poolSize,
		MaxIdle:         poolSize,
		Wait:            true,
		IdleTimeout:     redisPoolIdleTimeout,
		MaxConnLifetime: redisPoolMaxConnLifetime,
		Dial: func() (redis.Conn, error) {
			return redis.DialURL(url,
				redis.DialConnectTimeout(redisDialTimeout),
				redis.DialReadTimeout(redisDialTimeout),
				redis.DialWriteTimeout(redisDialTimeout),
			)
		},
		TestOnBorrow: func(c redis.Conn, lastUsed time.Time) error {
			if time.Since(lastUsed) < redisPoolHealthCheckAfter {
				return nil
			}

			_, err := c.Do("PING")

			return err
		},
	}

	// Fail here rather than on the first command, so a malformed URL is a
	// startup error with a clear cause.
	conn, err := pool.Dial()
	if err != nil {
		pool.Close() //nolint:errcheck // nothing was borrowed

		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}
	conn.Close() //nolint:errcheck // returning the probe connection

	return pool, nil
}
