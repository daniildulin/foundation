package foundation

import (
	"errors"
	"fmt"
	"time"

	ferr "github.com/foundation-go/foundation/errors"
	fsentry "github.com/foundation-go/foundation/sentry"
	"github.com/getsentry/sentry-go"
)

// sentryFlushTimeout returns how long the service is willing to wait for a
// Sentry report to be delivered before terminating.
func (s *Service) sentryFlushTimeout() time.Duration {
	if s.Config != nil && s.Config.Sentry != nil && s.Config.Sentry.FlushTimeout > 0 {
		return s.Config.Sentry.FlushTimeout
	}

	return fsentry.DefaultFlushTimeout
}

// wrapError prefixes err with msg, keeping the wrapped error inspectable.
func wrapError(err error, msg string) error {
	switch {
	case err == nil && msg == "":
		return errors.New("unknown error")
	case err == nil:
		return errors.New(msg)
	case msg == "":
		return err
	default:
		return fmt.Errorf("%s: %w", msg, err)
	}
}

// CaptureError logs err and reports it to Sentry without terminating.
func (s *Service) CaptureError(err error, msg string) {
	err = wrapError(err, msg)

	s.Logger.Error(err)
	sentry.CaptureException(err)
}

// Fatal reports err to Sentry, waits for the report to be delivered, logs it
// and terminates the process.
//
// Always use this instead of `sentry.CaptureException` followed by
// `Logger.Fatal`: the Sentry transport is asynchronous and `Logger.Fatal` calls
// `os.Exit`, so that pairing drops exactly the errors worth knowing about — the
// ones that stop the service from running.
func (s *Service) Fatal(err error, msg string) {
	err = wrapError(err, msg)

	fsentry.CaptureAndFlush(err, s.sentryFlushTimeout())

	s.Logger.Fatal(err)
}

func (s *Service) HandleError(err ferr.FoundationError, prefix string) {
	// Log internal errors
	var internalError *ferr.InternalError
	if errors.As(err, &internalError) {
		if prefix != "" {
			s.Logger.Errorf("%s: %s", prefix, err.Error())
		} else {
			s.Logger.Error(err.Error())
		}

		sentry.CaptureException(err)
	}
}
