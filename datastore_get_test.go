package dalgo2datastore

import (
	"cloud.google.com/go/datastore"
	"github.com/dal-go/dalgo/dal"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_handleGetByKeyError(t *testing.T) {
	k := dal.NewKeyWithID("K", "1")
	// Wrap NoSuchEntity into dal not found
	err := handleGetByKeyError(k, datastore.ErrNoSuchEntity)
	assert.True(t, dal.IsNotFound(err))
	// Other errors are returned as-is
	other := assert.AnError
	assert.Equal(t, other, handleGetByKeyError(k, other))
}
