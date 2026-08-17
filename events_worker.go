package foundation

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	fctx "github.com/foundation-go/foundation/context"
	ferr "github.com/foundation-go/foundation/errors"
	fkafka "github.com/foundation-go/foundation/kafka"
	fmetrics "github.com/foundation-go/foundation/metrics"
	"github.com/jackc/pgx/v5"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

type EventsWorker struct {
	*SpinWorker

	protoNamesToMessages map[string]proto.Message
}

// EventHandler represents an event handler
type EventHandler interface {
	Handle(context.Context, *Event, proto.Message) ([]*Event, ferr.FoundationError)
}

// ErrorHandlingStrategy defines the EventsWorker behavior when errors occur while handle event
type ErrorHandlingStrategy int

const (
	// Default strategy: commit the message and skip the event
	IgnoreError ErrorHandlingStrategy = iota

	// ShutdownOnError stops the worker on error
	ShutdownOnError

	// RetryOnError retries to handle event on error. TODO: implement RetryOnError.
	// RetryOnError
)

// EventsWorkerOptions represents the options for starting an events worker
type EventsWorkerOptions struct {
	Handlers               map[proto.Message][]EventHandler
	Topics                 []string
	ModeName               string
	ErrorHandlingStrategy  ErrorHandlingStrategy
	StartComponentsOptions []StartComponentsOption
}

func InitEventsWorker(name string) *EventsWorker {
	return &EventsWorker{
		SpinWorker: InitSpinWorker(name),
	}
}

func (opts *EventsWorkerOptions) GetTopics() []string {
	// If topics are specified in the options, use them
	if len(opts.Topics) > 0 {
		return opts.Topics
	}

	// Otherwise, build topics from events we're handling
	topics := []string{}

	if len(opts.Handlers) == 0 {
		return nil
	}

	for protoMsg := range opts.Handlers {
		protoName := ProtoToName(protoMsg)
		// Collect service names from event message names
		// project.service.SomeEvent -> project.service
		topic := protoName[:strings.LastIndex(protoName, ".")]

		if topic != "" {
			// Add topic to the list if it's not already there
			found := false
			for _, t := range topics {
				if t == topic {
					found = true
					break
				}
			}

			if !found {
				topics = append(topics, topic)
			}
		}
	}

	// Sort topics for consistency
	sort.Strings(topics)

	return topics
}

func (opts *EventsWorkerOptions) ProtoNamesToMessages() map[string]proto.Message {
	protoNamesToMessages := make(map[string]proto.Message)

	for msg := range opts.Handlers {
		protoNamesToMessages[ProtoToName(msg)] = msg
	}

	return protoNamesToMessages
}

// Start runs the worker that handles events
func (w *EventsWorker) Start(opts *EventsWorkerOptions) {
	w.protoNamesToMessages = opts.ProtoNamesToMessages()

	wOpts := NewSpinWorkerOptions()
	wOpts.ModeName = opts.ModeName
	wOpts.ProcessFunc = w.newProcessEventFunc(opts.Handlers, opts.ErrorHandlingStrategy)
	wOpts.StartComponentsOptions = append(opts.StartComponentsOptions,
		WithKafkaConsumer(),
		WithKafkaConsumerTopics(opts.GetTopics()...),
	)

	w.SpinWorker.Start(wOpts)
}

func newEventFromKafkaMessage(msg *kafka.Message) *Event {
	headers := make(map[string]string)
	for _, header := range msg.Headers {
		headers[header.Key] = string(header.Value)
	}

	return &Event{
		Topic:     msg.Topic,
		Key:       string(msg.Key),
		Payload:   msg.Value,
		ProtoName: headers[fkafka.HeaderProtoName],
		Headers:   headers,
		CreatedAt: msg.Time,
	}
}

func (w *EventsWorker) newProcessEventFunc(
	handlers map[proto.Message][]EventHandler,
	errorMode ErrorHandlingStrategy,
) func(ctx context.Context) ferr.FoundationError {
	return func(ctx context.Context) ferr.FoundationError {
		msg, err := w.GetKafkaConsumer().FetchMessage(ctx)
		if err != nil {
			// A cancelled context is how a fetch ends during a normal
			// shutdown, not a failure worth reporting.
			if errors.Is(err, context.Canceled) || errors.Is(err, io.EOF) {
				return nil
			}

			return ferr.NewInternalError(err, "failed to read message from Kafka")
		}

		shouldCommit, handleErr := w.handleMessage(ctx, msg, handlers, errorMode)

		if shouldCommit {
			if commitErr := w.CommitMessage(ctx, msg); commitErr != nil {
				return commitErr
			}
		}

		return handleErr
	}
}

// handleMessage processes a single fetched Kafka message and reports whether
// its offset should be committed.
//
// It is deliberately separate from fetching and committing so that the
// decisions below can be tested without a broker.
func (w *EventsWorker) handleMessage(
	ctx context.Context,
	msg kafka.Message,
	handlers map[proto.Message][]EventHandler,
	errorMode ErrorHandlingStrategy,
) (bool, ferr.FoundationError) {
	event := newEventFromKafkaMessage(&msg)

	// Enrich the context with the correlation ID of the incoming event, so
	// that everything published while handling it — including the error
	// event below — carries the same ID.
	ctx = fctx.WithCorrelationID(ctx, event.Headers[fkafka.HeaderCorrelationID])

	log := w.Logger.WithFields(map[string]interface{}{
		"correlation_id": event.Headers[fkafka.HeaderCorrelationID],
		"event":          event.ProtoName,
		"topic":          msg.Topic,
		"partition":      msg.Partition,
		"offset":         msg.Offset,
	})
	log.Info("Received event")

	if !event.CreatedAt.IsZero() {
		fmetrics.EventLag.WithLabelValues(msg.Topic).Observe(time.Since(event.CreatedAt).Seconds())
	}

	templateProtoMsg, ok := w.protoNamesToMessages[event.ProtoName]
	if !ok {
		// A worker subscribes to whole topics, so most of what it reads may be
		// of types it does not handle. Those offsets still have to be
		// committed: skipping the commit leaves the committed offset behind
		// the read position, so the consumer lag grows without bound and every
		// restart replays the tail of the topic.
		log.Debugf("Skipping event without handlers: `%s`", event.ProtoName)
		fmetrics.EventsProcessed.
			WithLabelValues(msg.Topic, event.ProtoName, fmetrics.ResultSkipped).
			Inc()

		return true, nil
	}

	protoMsg := proto.Clone(templateProtoMsg)
	if err := proto.Unmarshal(event.Payload, protoMsg); err != nil {
		// The coordinates go into the error itself, not just the log fields:
		// this error is reported once, by the caller, and whoever reads it in
		// Sentry needs to know which offset to re-drive.
		unmarshalErr := ferr.NewInternalError(err, fmt.Sprintf(
			"failed to unmarshal payload of `%s` at %s/%d offset %d",
			event.ProtoName, msg.Topic, msg.Partition, msg.Offset,
		))

		if errorMode == ShutdownOnError {
			log.Error("Cannot unmarshal event, shutting down")
			w.Shutdown()

			return false, unmarshalErr
		}

		fmetrics.EventsProcessed.
			WithLabelValues(msg.Topic, event.ProtoName, fmetrics.ResultError).
			Inc()

		// A payload that cannot be parsed now will not become parsable later,
		// so commit and move on rather than blocking the partition forever.
		// The error is returned rather than reported here: the worker loop
		// reports whatever it gets back, and reporting in both places sent two
		// Sentry events per message, doubling the volume during exactly the
		// poison-message storm where the count matters.
		return true, unmarshalErr
	}

	var handleErr ferr.FoundationError

	started := time.Now()

	defer func() {
		fmetrics.EventProcessingDuration.
			WithLabelValues(event.ProtoName).
			Observe(time.Since(started).Seconds())

		fmetrics.EventsProcessed.
			WithLabelValues(msg.Topic, event.ProtoName, fmetrics.ResultOf(handleErr)).
			Inc()
	}()

	for _, handler := range handlers[templateProtoMsg] {
		handlerLog := log.WithField("handler", fmt.Sprintf("%T", handler))
		handlerLog.Info("Processing event")

		handleErr = w.processEvent(ctx, handler, event, protoMsg)
		if handleErr != nil {
			handlerLog.WithError(handleErr).Errorf("Failed to process event `%s`", event.ProtoName)

			// We publish the error event to the error topic for further delivery to the user via WebSocket.
			if event.Headers[fkafka.HeaderOriginatorID] != "" {
				if err := w.NewAndPublishEvent(
					ctx, handleErr.MarshalProto(), event.Headers[fkafka.HeaderOriginatorID], nil, nil,
				); err != nil {
					return false, err
				}
			}

			// We just stop all the subsequent handlers from processing the event if one of them failed.
			//
			// TODO: Consider adding a configuration option to allow the user to choose whether to stop after
			// specific handler failed or not. It would require to add ability to return multiple errors from
			// this function.
			break
		}

		handlerLog.Info("Event processed successfully")
	}

	if handleErr != nil && errorMode == ShutdownOnError {
		log.Errorf("Cannot process event: %v", handleErr)
		w.Shutdown()

		return false, handleErr
	}

	return true, handleErr
}

func (w *EventsWorker) processEvent(ctx context.Context, handler EventHandler, event *Event, msg proto.Message) ferr.FoundationError {
	var (
		tx         pgx.Tx
		needCommit bool
		err        error
	)

	if w.Config.Database.Enabled {
		tx, err = w.GetPostgreSQL().Begin(ctx)
		if err != nil {
			return ferr.NewInternalError(err, "failed to begin transaction")
		}
		defer tx.Rollback(ctx) // nolint:errcheck
		needCommit = true

		// Add transaction to context
		ctx = fctx.WithTX(ctx, tx)
	}

	// Handle event
	events, handleErr := handler.Handle(ctx, event, msg)
	if handleErr != nil {
		return handleErr
	}

	// Publish outgoing events
	for _, e := range events {
		if publishErr := w.PublishEvent(ctx, e, tx); publishErr != nil {
			return publishErr
		}
	}

	if needCommit {
		// Commit transaction
		if err = tx.Commit(ctx); err != nil {
			return ferr.NewInternalError(err, "failed to commit transaction")
		}
	}

	return nil
}

const (
	// commitMessageAttempts is how many times a Kafka offset commit is tried
	// before giving up.
	commitMessageAttempts = 3

	// commitMessageBaseDelay is the first backoff step between commit attempts;
	// it doubles on every retry.
	commitMessageBaseDelay = 250 * time.Millisecond
)

// CommitMessage commits a Kafka message using the service's KafkaConsumer,
// retrying a few times with exponential backoff. The wait between attempts is
// interruptible, so a shutdown does not have to sit through it.
func (s *Service) CommitMessage(ctx context.Context, msg kafka.Message) ferr.FoundationError {
	var lastErr error

	for attempt := 0; attempt < commitMessageAttempts; attempt++ {
		if attempt > 0 {
			delay := commitMessageBaseDelay << (attempt - 1)

			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ferr.NewInternalError(lastErr, "failed to commit message before shutdown")
			case <-timer.C:
			}
		}

		if err := s.GetKafkaConsumer().CommitMessages(ctx, msg); err != nil {
			lastErr = err
			continue
		}

		return nil
	}

	return ferr.NewInternalError(lastErr, "failed to commit message")
}
