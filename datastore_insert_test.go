package dalgo2datastore

import (
	"errors"
	"testing"

	"github.com/dal-go/record"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func Test_wrapIfAlreadyExists(t *testing.T) {
	t.Run("nil error stays nil", func(t *testing.T) {
		assert.NoError(t, wrapIfAlreadyExists(nil))
	})

	t.Run("AlreadyExists status is classified without discarding it", func(t *testing.T) {
		original := status.Error(codes.AlreadyExists, "entity already exists")
		err := wrapIfAlreadyExists(original)
		assert.True(t, record.IsAlreadyExists(err), "want record.IsAlreadyExists(err), got %v", err)
		assert.ErrorContains(t, err, "entity already exists", "the original gRPC error must still be visible, not replaced")
	})

	t.Run("other gRPC status codes pass through unclassified", func(t *testing.T) {
		original := status.Error(codes.Aborted, "concurrent transaction")
		err := wrapIfAlreadyExists(original)
		assert.Same(t, original, err)
		assert.False(t, record.IsAlreadyExists(err))
	})

	t.Run("non-status errors pass through unclassified", func(t *testing.T) {
		original := errors.New("boom")
		err := wrapIfAlreadyExists(original)
		assert.Same(t, original, err)
		assert.False(t, record.IsAlreadyExists(err))
	})
}
