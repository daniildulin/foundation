package foundation

import (
	"context"
	"io"
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	ferr "github.com/foundation-go/foundation/errors"
	ferrpb "github.com/foundation-go/foundation/errors/proto"
	fkafka "github.com/foundation-go/foundation/kafka"
)

// recordingHandler captures the events it is given and optionally fails.
type recordingHandler struct {
	calls int
	err   ferr.FoundationError
}

func (h *recordingHandler) Handle(context.Context, *Event, proto.Message) ([]*Event, ferr.FoundationError) {
	h.calls++

	return nil, h.err
}

func newTestEventsWorker(handlers map[proto.Message][]EventHandler) (*EventsWorker, *bool) {
	logger := logrus.New()
	logger.SetOutput(io.Discard)

	shutdownCalled := false

	svc := &Service{
		Name: "test",
		Config: &Config{
			Database: &DatabaseConfig{},
			Outbox:   &OutboxConfig{},
			Sentry:   &SentryConfig{},
		},
		Logger:     logrus.NewEntry(logger),
		cancelFunc: func() { shutdownCalled = true },
	}

	opts := &EventsWorkerOptions{Handlers: handlers}

	return &EventsWorker{
		SpinWorker:           &SpinWorker{Service: svc},
		protoNamesToMessages: opts.ProtoNamesToMessages(),
	}, &shutdownCalled
}

func newTestMessage(t *testing.T, msg proto.Message, headers map[string]string) kafka.Message {
	t.Helper()

	payload, err := proto.Marshal(msg)
	require.NoError(t, err)

	return newRawTestMessage(payload, ProtoToName(msg), headers)
}

func newRawTestMessage(payload []byte, protoName string, headers map[string]string) kafka.Message {
	kafkaHeaders := []kafka.Header{{Key: fkafka.HeaderProtoName, Value: []byte(protoName)}}
	for k, v := range headers {
		kafkaHeaders = append(kafkaHeaders, kafka.Header{Key: k, Value: []byte(v)})
	}

	return kafka.Message{
		Topic:     "foundation.errors",
		Partition: 0,
		Offset:    42,
		Value:     payload,
		Headers:   kafkaHeaders,
	}
}

func TestHandleMessageCommitsHandledEvent(t *testing.T) {
	handler := &recordingHandler{}
	template := &ferrpb.NotFoundError{}

	worker, shutdown := newTestEventsWorker(map[proto.Message][]EventHandler{
		template: {handler},
	})

	msg := newTestMessage(t, &ferrpb.NotFoundError{Kind: "chat", Id: "1"}, nil)

	shouldCommit, err := worker.handleMessage(context.Background(), msg, map[proto.Message][]EventHandler{
		template: {handler},
	}, IgnoreError)

	assert.True(t, shouldCommit)
	assert.Nil(t, err)
	assert.Equal(t, 1, handler.calls)
	assert.False(t, *shutdown)
}

// A worker subscribes to whole topics, so it constantly reads event types it
// does not handle. Those offsets were never committed, which pinned the
// committed offset behind the read position: the lag grew without bound and
// every restart replayed the tail of the topic.
func TestHandleMessageCommitsEventWithoutHandlers(t *testing.T) {
	worker, shutdown := newTestEventsWorker(map[proto.Message][]EventHandler{
		&ferrpb.NotFoundError{}: {&recordingHandler{}},
	})

	msg := newTestMessage(t, &ferrpb.StaleObjectError{Kind: "chat"}, nil)

	shouldCommit, err := worker.handleMessage(context.Background(), msg, nil, IgnoreError)

	assert.True(t, shouldCommit, "an unhandled event must still advance the committed offset")
	assert.Nil(t, err)
	assert.False(t, *shutdown)
}

func TestHandleMessageCommitsEventWithoutProtoNameHeader(t *testing.T) {
	worker, _ := newTestEventsWorker(map[proto.Message][]EventHandler{
		&ferrpb.NotFoundError{}: {&recordingHandler{}},
	})

	msg := newRawTestMessage([]byte("whatever"), "", nil)

	shouldCommit, err := worker.handleMessage(context.Background(), msg, nil, IgnoreError)

	assert.True(t, shouldCommit)
	assert.Nil(t, err)
}

// An unparsable payload will not become parsable on a retry. It has to be
// reported and committed, otherwise it blocks its partition forever.
func TestHandleMessageCommitsUnparsablePayload(t *testing.T) {
	handler := &recordingHandler{}
	template := &ferrpb.NotFoundError{}
	handlers := map[proto.Message][]EventHandler{template: {handler}}

	worker, shutdown := newTestEventsWorker(handlers)

	msg := newRawTestMessage([]byte{0xff, 0xff, 0xff, 0xff}, ProtoToName(template), nil)

	shouldCommit, err := worker.handleMessage(context.Background(), msg, handlers, IgnoreError)

	assert.True(t, shouldCommit)
	require.NotNil(t, err)
	assert.Contains(t, err.Error(), "failed to unmarshal payload")
	assert.Equal(t, 0, handler.calls)
	assert.False(t, *shutdown)
}

func TestHandleMessageShutsDownOnUnparsablePayloadWhenAsked(t *testing.T) {
	handler := &recordingHandler{}
	template := &ferrpb.NotFoundError{}
	handlers := map[proto.Message][]EventHandler{template: {handler}}

	worker, shutdown := newTestEventsWorker(handlers)

	msg := newRawTestMessage([]byte{0xff, 0xff, 0xff, 0xff}, ProtoToName(template), nil)

	shouldCommit, err := worker.handleMessage(context.Background(), msg, handlers, ShutdownOnError)

	assert.False(t, shouldCommit, "ShutdownOnError must leave the offset for investigation")
	assert.NotNil(t, err)
	assert.True(t, *shutdown)
}

func TestHandleMessageCommitsAfterHandlerErrorByDefault(t *testing.T) {
	handler := &recordingHandler{err: ferr.NewNotFoundError(nil, "chat", "1")}
	template := &ferrpb.NotFoundError{}
	handlers := map[proto.Message][]EventHandler{template: {handler}}

	worker, shutdown := newTestEventsWorker(handlers)

	msg := newTestMessage(t, &ferrpb.NotFoundError{Kind: "chat", Id: "1"}, nil)

	shouldCommit, err := worker.handleMessage(context.Background(), msg, handlers, IgnoreError)

	assert.True(t, shouldCommit)
	assert.NotNil(t, err)
	assert.Equal(t, 1, handler.calls)
	assert.False(t, *shutdown)
}

func TestHandleMessageShutsDownOnHandlerError(t *testing.T) {
	handler := &recordingHandler{err: ferr.NewNotFoundError(nil, "chat", "1")}
	template := &ferrpb.NotFoundError{}
	handlers := map[proto.Message][]EventHandler{template: {handler}}

	worker, shutdown := newTestEventsWorker(handlers)

	msg := newTestMessage(t, &ferrpb.NotFoundError{Kind: "chat", Id: "1"}, nil)

	shouldCommit, err := worker.handleMessage(context.Background(), msg, handlers, ShutdownOnError)

	assert.False(t, shouldCommit)
	assert.NotNil(t, err)
	assert.True(t, *shutdown)
}

// The first failing handler stops the chain.
func TestHandleMessageStopsAfterFirstFailingHandler(t *testing.T) {
	failing := &recordingHandler{err: ferr.NewNotFoundError(nil, "chat", "1")}
	next := &recordingHandler{}
	template := &ferrpb.NotFoundError{}
	handlers := map[proto.Message][]EventHandler{template: {failing, next}}

	worker, _ := newTestEventsWorker(handlers)

	msg := newTestMessage(t, &ferrpb.NotFoundError{Kind: "chat", Id: "1"}, nil)

	_, err := worker.handleMessage(context.Background(), msg, handlers, IgnoreError)

	assert.NotNil(t, err)
	assert.Equal(t, 1, failing.calls)
	assert.Equal(t, 0, next.calls, "handlers after a failure must not run")
}

func TestEventsWorkerGetTopics(t *testing.T) {
	t.Run("explicit topics win", func(t *testing.T) {
		opts := &EventsWorkerOptions{Topics: []string{"b", "a"}}
		assert.Equal(t, []string{"b", "a"}, opts.GetTopics())
	})

	t.Run("derived from handled protos, sorted and deduplicated", func(t *testing.T) {
		opts := &EventsWorkerOptions{
			Handlers: map[proto.Message][]EventHandler{
				&ferrpb.NotFoundError{}:    {&recordingHandler{}},
				&ferrpb.StaleObjectError{}: {&recordingHandler{}},
			},
		}

		assert.Equal(t, []string{"foundation.errors"}, opts.GetTopics())
	})

	t.Run("no handlers, no topics", func(t *testing.T) {
		assert.Nil(t, (&EventsWorkerOptions{}).GetTopics())
	})
}

func TestShutdownIsSafeWithoutStart(t *testing.T) {
	assert.NotPanics(t, func() { (&Service{}).Shutdown() })
}
