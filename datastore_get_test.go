package dalgo2datastore

import (
	"cloud.google.com/go/datastore"
	"github.com/dal-go/record"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_handleGetByKeyError(t *testing.T) {
	k := record.NewKeyWithID("K", "1")
	// Wrap NoSuchEntity into dal not found
	err := handleGetByKeyError(k, datastore.ErrNoSuchEntity)
	assert.True(t, record.IsNotFound(err))
	// Other errors are returned as-is
	other := assert.AnError
	assert.Equal(t, other, handleGetByKeyError(k, other))
}
