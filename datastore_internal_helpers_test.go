package dalgo2datastore

import (
	"cloud.google.com/go/datastore"
	"context"
	"errors"
	"github.com/dal-go/dalgo/dal"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func Test_existsByKey_Behaviors(t *testing.T) {
	key := dal.NewKeyWithID("K", "1")

	// 1) Getter returns ErrFieldMismatch -> existsByKey returns nil
	{
		getter := func(_ *datastore.Key, _ any) error {
			return &datastore.ErrFieldMismatch{StructType: reflect.TypeOf(struct{}{}), FieldName: "X"}
		}
		err := existsByKey(key, getter)
		assert.NoError(t, err)
	}

	// 2) Getter returns other error -> propagated
	{
		other := errors.New("boom")
		getter := func(_ *datastore.Key, _ any) error { return other }
		err := existsByKey(key, getter)
		assert.Equal(t, other, err)
	}

	// 3) Getter returns nil -> existsByKey returns nil
	{
		getter := func(_ *datastore.Key, _ any) error { return nil }
		err := existsByKey(key, getter)
		assert.NoError(t, err)
	}
}

func Test_getByKey_WrapsNoSuchEntityAndCallsGetter(t *testing.T) {
	called := 0
	key := dal.NewKeyWithID("K", 7)
	getter := func(_ *datastore.Key, _ any) error {
		called++
		return datastore.ErrNoSuchEntity
	}
	var dst struct{}
	err := getByKey(key, getter, &dst)
	assert.True(t, dal.IsNotFound(err))
	assert.Equal(t, 1, called)
}

func Test_getByKey_IncompleteKey_CallsGetter(t *testing.T) {
	// Note: getDatastoreKey returns an incomplete datastore key but isPartial=false,
	// so getByKey does not reject it and still calls the getter.
	called := 0
	rec := dal.NewRecordWithIncompleteKey("KindZ", reflect.String, &struct{}{})
	getter := func(dsKey *datastore.Key, _ any) error {
		called++
		// Ensure the key we receive is indeed incomplete
		assert.True(t, dsKey.Incomplete())
		return nil
	}
	var dst struct{}
	err := getByKey(rec.Key(), getter, &dst)
	assert.NoError(t, err)
	assert.Equal(t, 1, called)
}

func Test_insert_IncompleteKeyNoGenerator_CallsInserter(t *testing.T) {
	rec := dal.NewRecordWithIncompleteKey("KindA", reflect.String, &struct{}{})
	called := 0
	ins := func(dsKey *datastore.Key, isPartial bool, _ any) error {
		called++
		assert.True(t, dsKey.Incomplete())
		assert.False(t, isPartial) // current implementation does not set isPartial for incomplete keys
		return nil
	}
	exists := func(_ *datastore.Key) error { return nil }
	err := insert(context.Background(), rec, ins, exists, dal.NewInsertOptions())
	assert.NoError(t, err)
	assert.Equal(t, 1, called)
}

func Test_updatePartialKey(t *testing.T) {
	rec := dal.NewRecordWithIncompleteKey("K", reflect.Int64, &struct{}{})
	k := rec.Key()
	dsKey := datastore.IDKey("K", 123, nil)
	updatePartialKey(k, dsKey)
	assert.Equal(t, int64(123), k.ID)
}
