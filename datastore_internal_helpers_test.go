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

func Test_getByKey_IncompleteKey_Rejected(t *testing.T) {
	called := 0
	rec := dal.NewRecordWithIncompleteKey("KindZ", reflect.String, &struct{}{})
	getter := func(_ *datastore.Key, _ any) error {
		called++
		return nil
	}
	var dst struct{}
	err := getByKey(rec.Key(), getter, &dst)
	assert.Error(t, err)
	assert.Equal(t, 0, called)
}

func Test_insert_IncompleteKeyNoGenerator_CallsInserter(t *testing.T) {
	rec := dal.NewRecordWithIncompleteKey("KindA", reflect.String, &struct{}{})
	called := 0
	ins := func(dsKey *datastore.Key, isPartial bool, _ any) error {
		called++
		assert.True(t, dsKey.Incomplete())
		assert.True(t, isPartial) // incomplete keys use Datastore's native ID allocation
		return nil
	}
	exists := func(_ *datastore.Key) error { return nil }
	err := insert(context.Background(), rec, ins, exists, dal.NewInsertOptions())
	assert.NoError(t, err)
	assert.Equal(t, 1, called)
}

func Test_insert_IncompleteKeyWithAdapterGeneratedID_CallsInserter(t *testing.T) {
	rec := dal.NewRecordWithIncompleteKey("KindB", reflect.Int64, &struct{}{})
	called := 0
	ins := func(dsKey *datastore.Key, isPartial bool, _ any) error {
		called++
		assert.True(t, dsKey.Incomplete())
		assert.True(t, isPartial)
		return nil
	}
	exists := func(_ *datastore.Key) error { return nil }
	err := insert(context.Background(), rec, ins, exists, dal.NewInsertOptions(dal.WithAdapterGeneratedID()))
	assert.NoError(t, err)
	assert.Equal(t, 1, called)
}

func Test_insert_IncompleteKeyWithIDGenerator(t *testing.T) {
	t.Run("inserts_with_generated_id", func(t *testing.T) {
		rec := dal.NewRecordWithIncompleteKey("KindC", reflect.String, &struct{}{})
		called := 0
		ins := func(dsKey *datastore.Key, isPartial bool, _ any) error {
			called++
			assert.False(t, isPartial)
			assert.False(t, dsKey.Incomplete())
			assert.NotEmpty(t, dsKey.Name)
			return nil
		}
		exists := func(_ *datastore.Key) error { return datastore.ErrNoSuchEntity }
		err := insert(context.Background(), rec, ins, exists, dal.NewInsertOptions(dal.WithRandomStringKey(10, 5)))
		assert.NoError(t, err)
		assert.Equal(t, 1, called)
		assert.NotEmpty(t, rec.Key().ID)
	})
	t.Run("fails_if_all_generated_ids_taken", func(t *testing.T) {
		rec := dal.NewRecordWithIncompleteKey("KindC", reflect.String, &struct{}{})
		ins := func(_ *datastore.Key, _ bool, _ any) error {
			t.Error("inserter should not be called when all generated IDs are taken")
			return nil
		}
		existsCalled := 0
		exists := func(_ *datastore.Key) error {
			existsCalled++
			return nil // entity exists
		}
		err := insert(context.Background(), rec, ins, exists, dal.NewInsertOptions(dal.WithRandomStringKey(10, 5)))
		assert.Error(t, err)
		assert.Greater(t, existsCalled, 1)
	})
	t.Run("explicit_generator_wins_over_adapter_generated_id", func(t *testing.T) {
		rec := dal.NewRecordWithIncompleteKey("KindC", reflect.String, &struct{}{})
		called := 0
		ins := func(dsKey *datastore.Key, isPartial bool, _ any) error {
			called++
			assert.False(t, isPartial)
			assert.NotEmpty(t, dsKey.Name)
			return nil
		}
		exists := func(_ *datastore.Key) error { return datastore.ErrNoSuchEntity }
		options := dal.NewInsertOptions(dal.WithAdapterGeneratedID(), dal.WithRandomStringKey(10, 5))
		err := insert(context.Background(), rec, ins, exists, options)
		assert.NoError(t, err)
		assert.Equal(t, 1, called)
	})
}

func Test_updatePartialKey(t *testing.T) {
	rec := dal.NewRecordWithIncompleteKey("K", reflect.Int64, &struct{}{})
	k := rec.Key()
	dsKey := datastore.IDKey("K", 123, nil)
	updatePartialKey(k, dsKey)
	assert.Equal(t, int64(123), k.ID)
}
