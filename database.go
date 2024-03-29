package dalgo2datastore

import (
	"cloud.google.com/go/datastore"
	"context"
	"errors"
	"fmt"
	"github.com/dal-go/dalgo/dal"
	"google.golang.org/api/option"
)

var _ dal.DB = (*database)(nil)

type database struct {
	ProjectID string
	client    *datastore.Client
	dal.QueryExecutor
}

func (db database) ID() string {
	if db.ProjectID == "" {
		return datastore.DetectProjectID
	}
	return db.ProjectID
}

func (db database) Adapter() dal.Adapter {
	return dal.NewAdapter("datastore", "v1")
}

func (database) Upsert(_ context.Context, _ dal.Record) error {
	panic("implement me")
}

// NewDatabase create database provider to Google Datastore
func NewDatabase(ctx context.Context, projectID string) (db dal.DB, err error) {
	var database database
	database.ProjectID = projectID
	database.client, err = datastore.NewClient(ctx, projectID, option.WithoutAuthentication())
	var getReader = func(c context.Context, query dal.Query) (dal.Reader, error) {
		return getReader(c, database.ProjectID, query)
	}
	database.QueryExecutor = dal.NewQueryExecutor(getReader)
	return database, err
}

//func (db database) exists(c context.Context, recordKey *dal.Key) error {
//	var empty struct{}
//	return db.Get(c, dal.NewRecordWithData(recordKey, &empty))
//}

func setRecordID(key *datastore.Key, record dal.Record) {
	recordKey := record.Key()
	if strID := key.Name; strID != "" {
		recordKey.ID = strID
	} else {
		recordKey.ID = key.ID
	}
}

// ErrKeyHasBothIds indicates record has both string and int ids
//var ErrKeyHasBothIds = errors.New("record has both string and int ids")

// ErrEmptyKind indicates record holder returned empty kind
var ErrEmptyKind = errors.New("record holder returned empty kind")

func getDatastoreKey(dalKey *dal.Key) (datastoreKey *datastore.Key, isPartial bool, err error) {
	if dalKey == nil {
		panic(dalKey == nil)
	}
	ref := dalKey
	if ref.Collection() == "" {
		err = ErrEmptyKind
	} else {
		if ref.ID == nil {
			datastoreKey = NewIncompleteKey(ref.Collection(), nil)
		} else {
			switch id := ref.ID.(type) {
			case string:
				//id = dal.EscapeID(id)
				datastoreKey = datastore.NameKey(ref.Collection(), id, nil)
			case int:
				datastoreKey = datastore.IDKey(ref.Collection(), (int64)(id), nil)
			default:
				err = fmt.Errorf("unsupported ID type: %T", id)
			}
		}
	}
	return
}

//var xgTransaction = &datastore.TransactionOptions{XG: true}

var isInTransactionFlag = "is in transaction"
var nonTransactionalContextKey = "non transactional context"

//func (database) RunInTransaction(ctx context.Context, f func(ctx context.Context, tx dal.Transaction) error, options ...dal.TransactionOption) error {
//	txOptions := dal.NewTransactionOptions(options...)
//	var to *datastore.TransactionOptions
//	if txOptions.IsCrossGroup() {
//		to = xgTransaction
//	}
//	return RunInTransaction(ctx, f, to)
//}

func (database) IsInTransaction(c context.Context) bool {
	if v := c.Value(&isInTransactionFlag); v != nil && v.(bool) {
		return true
	}
	return false
}

func (database) NonTransactionalContext(tc context.Context) context.Context {
	if c := tc.Value(&nonTransactionalContextKey); c != nil {
		return c.(context.Context)
	}
	return tc
}
