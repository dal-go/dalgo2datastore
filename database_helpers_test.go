package dalgo2datastore

import (
	"cloud.google.com/go/datastore"
	"context"
	"github.com/dal-go/dalgo/dal"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func TestDatabase_ID(t *testing.T) {
	var db database
	// Empty ProjectID returns DetectProjectID
	assert.Equal(t, datastore.DetectProjectID, db.ID())
	// Non-empty ProjectID
	db.ProjectID = "my-project"
	assert.Equal(t, "my-project", db.ID())
}

func TestDatabase_AdapterAndSchema(t *testing.T) {
	var db database
	a := db.Adapter()
	assert.Equal(t, "datastore", a.Name())
	assert.Equal(t, "v1", a.Version())
	assert.Nil(t, db.Schema())
}

func TestSetRecordID_StringAndInt(t *testing.T) {
	// String ID
	recStr := dal.NewRecordWithIncompleteKey("KindA", reflect.String, &struct{}{})
	dsKeyStr := datastore.NameKey("KindA", "abc123", nil)
	setRecordID(dsKeyStr, recStr)
	assert.Equal(t, "abc123", recStr.Key().ID)

	// Int ID
	recInt := dal.NewRecordWithIncompleteKey("KindB", reflect.Int64, &struct{}{})
	dsKeyInt := datastore.IDKey("KindB", 789, nil)
	setRecordID(dsKeyInt, recInt)
	assert.Equal(t, int64(789), recInt.Key().ID)
}

func TestGetDatastoreKey_Various(t *testing.T) {
	// Incomplete key (nil ID)
	rec := dal.NewRecordWithIncompleteKey("K1", reflect.String, &struct{}{})
	dsKey, isPartial, err := getDatastoreKey(rec.Key())
	assert.NoError(t, err)
	// function currently returns an incomplete datastore key, but isPartial flag is not set
	assert.False(t, isPartial)
	assert.True(t, dsKey.Incomplete())
	assert.Equal(t, "K1", dsKey.Kind)

	// String ID
	kStr := dal.NewKeyWithID("K2", "s-1")
	dsKey, isPartial, err = getDatastoreKey(kStr)
	assert.NoError(t, err)
	assert.False(t, isPartial)
	assert.Equal(t, "s-1", dsKey.Name)
	assert.Equal(t, int64(0), dsKey.ID)

	// Int ID
	kInt := dal.NewKeyWithID("K3", 123)
	dsKey, isPartial, err = getDatastoreKey(kInt)
	assert.NoError(t, err)
	assert.False(t, isPartial)
	assert.Equal(t, int64(123), dsKey.ID)
	assert.Equal(t, "", dsKey.Name)

	// Unsupported ID type
	kBad := dal.NewKeyWithID("K4", 3.14)
	_, _, err = getDatastoreKey(kBad)
	assert.Error(t, err)
}

func TestTransactionContextHelpers(t *testing.T) {
	ctx := context.Background()
	var db database
	// default false
	assert.False(t, db.IsInTransaction(ctx))
	// mark in-transaction
	ctxTx := context.WithValue(ctx, &isInTransactionFlag, true)
	assert.True(t, db.IsInTransaction(ctxTx))
	// NonTransactionalContext returns embedded if present
	type key string
	nonTx := context.WithValue(ctx, key("k"), "v")
	ctxTx2 := context.WithValue(ctx, &nonTransactionalContextKey, nonTx)
	actual := db.NonTransactionalContext(ctxTx2)
	assert.Equal(t, nonTx, actual)
	// If not present returns same
	assert.Equal(t, ctx, db.NonTransactionalContext(ctx))
}
