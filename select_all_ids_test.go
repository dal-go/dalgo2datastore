package dalgo2datastore

import (
	"cloud.google.com/go/datastore"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func Test_idFromDatastoreKey_StringIDKind(t *testing.T) {
	// When idKind is string, prefer Name if present, otherwise format ID
	k1 := datastore.NameKey("K", "s1", nil)
	id, err := idFromDatastoreKey(k1, reflect.String)
	assert.NoError(t, err)
	assert.Equal(t, "s1", id)

	k2 := datastore.IDKey("K", 55, nil)
	id, err = idFromDatastoreKey(k2, reflect.String)
	assert.NoError(t, err)
	assert.Equal(t, "55", id)
}

func Test_idFromDatastoreKey_IntKinds(t *testing.T) {
	k := datastore.IDKey("K", 42, nil)
	for _, kind := range []reflect.Kind{reflect.Int64, reflect.Int, reflect.Int32, reflect.Int16, reflect.Int8} {
		id, err := idFromDatastoreKey(k, kind)
		assert.NoError(t, err)
		// All should be numerics equal to 42 one way or another
		switch v := id.(type) {
		case int64:
			assert.Equal(t, int64(42), v)
		case int:
			assert.Equal(t, 42, v)
		default:
			// Some branches convert to int for Int32/16/8
			assert.IsType(t, int(0), v)
			assert.Equal(t, 42, v)
		}
	}
}

func Test_idFromDatastoreKey_ParseNameToInt(t *testing.T) {
	k := datastore.NameKey("K", "101", nil)
	id, err := idFromDatastoreKey(k, reflect.Int)
	assert.NoError(t, err)
	assert.Equal(t, 101, id)
}

func Test_idFromDatastoreKey_Errors(t *testing.T) {
	// Incomplete key
	inc := datastore.IncompleteKey("K", nil)
	_, err := idFromDatastoreKey(inc, reflect.Int)
	assert.Error(t, err)

	// Invalid id kind
	k := datastore.IDKey("K", 1, nil)
	_, err = idFromDatastoreKey(k, reflect.Invalid)
	assert.Error(t, err)

	// Unsupported kind
	_, err = idFromDatastoreKey(k, reflect.Bool)
	assert.Error(t, err)
}
