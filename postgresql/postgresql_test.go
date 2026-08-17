package postgresql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Stop dereferenced a Connection that only exists after a successful Start, and
// logged through a logger that is nil unless WithLogger was passed.
func TestComponentIsSafeBeforeStart(t *testing.T) {
	c := NewComponent()

	require.NotPanics(t, func() { assert.NoError(t, c.Stop()) })
	require.NotPanics(t, func() { assert.ErrorContains(t, c.Health(), "not initialized") })
	require.NotPanics(t, func() {
		assert.ErrorContains(t, c.HealthContext(context.Background()), "not initialized")
	})
}

func TestStartRejectsANonPositivePoolSize(t *testing.T) {
	c := NewComponent(WithDatabaseURL("postgres://localhost:5432/test"), WithPoolSize(0))

	err := c.Start()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "pool size")
}

func TestStartRejectsAMalformedURL(t *testing.T) {
	c := NewComponent(WithDatabaseURL("://nonsense"), WithPoolSize(1))

	assert.Error(t, c.Start())
}

func TestName(t *testing.T) {
	assert.Equal(t, ComponentName, NewComponent().Name())
}

func TestNewNullTimeFromPbTimestamp(t *testing.T) {
	assert.False(t, NewNullTimeFromPbTimestamp(nil).Valid)

	ts := timestamppb.New(timestamppb.Now().AsTime())
	assert.True(t, NewNullTimeFromPbTimestamp(ts).Valid)
}

func TestNullHelpers(t *testing.T) {
	assert.False(t, NewNullInt32(nil).Valid)
	assert.False(t, NewNullInt64(nil).Valid)
	assert.False(t, NewNullString(nil).Valid)
	assert.False(t, NewNullUUID(nil).Valid)

	i32 := int32(1)
	i64 := int64(2)
	str := "three"
	id := "0f8fad5b-d9cb-469f-a165-70867728950e"

	assert.True(t, NewNullInt32(&i32).Valid)
	assert.True(t, NewNullInt64(&i64).Valid)
	assert.True(t, NewNullString(&str).Valid)
	assert.True(t, NewNullUUID(&id).Valid)
}
