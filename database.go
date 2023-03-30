package dalgo2gaedatastore

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/strongo/dalgo/dal"
	"github.com/strongo/log"
	"google.golang.org/appengine/v2/datastore"
	"strconv"
)

var _ dal.Database = (*database)(nil)

type database struct {
}

func (database) Select(ctx context.Context, query dal.Select) (dal.Reader, error) {
	//TODO implement me
	panic("implement me")
}

func (database) Upsert(c context.Context, record dal.Record) error {
	panic("implement me")
}

// NewDatabase create database provider to Google Datastore
func NewDatabase() dal.Database {
	return database{}
}

func (database) Get(c context.Context, record dal.Record) (err error) {
	if record == nil {
		panic("record == nil")
	}
	key, isIncomplete, err := getDatastoreKey(c, record.Key())
	if err != nil {
		return
	}
	if isIncomplete {
		panic("can't get record by incomplete key")
	}
	entity := record.Data()
	if err = Get(c, key, entity); err != nil {
		if err == datastore.ErrNoSuchEntity {
			err = dal.NewErrNotFoundByKey(record.Key(), err)
		}
		return
	}
	return
}

func (database) Delete(c context.Context, recordKey *dal.Key) (err error) {
	if recordKey == nil {
		panic("recordKey == nil")
	}
	key, isIncomplete, err := getDatastoreKey(c, recordKey)
	if err != nil {
		return
	}
	if isIncomplete {
		panic("can't delete record by incomplete key")
	}
	if err = Delete(c, key); err != nil {
		return
	}
	return
}

func (database) DeleteMulti(c context.Context, recordKeys []*dal.Key) (err error) {
	if len(recordKeys) == 0 {
		return
	}
	keys := make([]*datastore.Key, len(recordKeys))
	for i, recordKey := range recordKeys {
		key, isIncomplete, err := getDatastoreKey(c, recordKey)
		if err != nil {
			return errors.WithMessage(err, "i="+strconv.Itoa(i))
		}
		if isIncomplete {
			panic("can't delete record by incomplete key, i=" + strconv.Itoa(i))
		}
		keys[i] = key
	}
	if err = DeleteMulti(c, keys); err != nil {
		return
	}
	return
}

func (db database) Insert(c context.Context, record dal.Record, opts ...dal.InsertOption) (err error) {
	if record == nil {
		panic("record == nil")
	}
	recordKey := record.Key()
	kind := recordKey.Collection()
	log.Debugf(c, "Insert(kind=%v)", kind)
	data := record.Data()
	if data == nil {
		panic("data == nil")
	}
	options := dal.NewInsertOptions(opts...)
	if generateID := options.IDGenerator(); generateID != nil {
		exists := func(key *dal.Key) error {
			return db.exists(c, recordKey)
		}
		insert := func(record dal.Record) error {
			return db.insert(c, record)
		}
		return dal.InsertWithRandomID(c, record, generateID, 5, exists, insert)
	}
	return
}

func (database) insert(c context.Context, record dal.Record) (err error) {
	if record == nil {
		panic("record == nil")
	}
	recordKey := record.Key()
	kind := recordKey.Collection()
	log.Debugf(c, "InsertWithRandomIntID(kind=%v)", kind)
	entity := record.Data()
	if entity == nil {
		panic("record == nil")
	}

	wrapErr := func(err error) error {
		return errors.WithMessage(err, "failed to create record with random str ID for: "+kind)
	}
	key, isIncomplete, err := getDatastoreKey(c, recordKey)
	if err != nil {
		return wrapErr(err)
	}
	if isIncomplete {
		panic(fmt.Sprintf("database.insert() called for key with incomplete ID: %+v", key))
	}

	_, err = Put(c, key, record.Data())
	return err
}

func (db database) exists(c context.Context, recordKey *dal.Key) error {
	var empty struct{}
	return db.Get(c, dal.NewRecordWithData(recordKey, &empty))
}

func setRecordID(key *datastore.Key, record dal.Record) {
	recordKey := record.Key()
	if strID := key.StringID(); strID != "" {
		recordKey.ID = strID
	} else {
		recordKey.ID = key.IntID()
	}
}

// ErrKeyHasBothIds indicates record has both string and int ids
var ErrKeyHasBothIds = errors.New("record has both string and int ids")

// ErrEmptyKind indicates record holder returned empty kind
var ErrEmptyKind = errors.New("record holder returned empty kind")

func getDatastoreKey(c context.Context, recordKey *dal.Key) (key *datastore.Key, isIncomplete bool, err error) {
	if recordKey == nil {
		panic(recordKey == nil)
	}
	ref := recordKey
	if ref.Collection() == "" {
		err = ErrEmptyKind
	} else {
		if ref.ID == nil {
			key = NewIncompleteKey(c, ref.Collection(), nil)
		} else {
			switch v := ref.ID.(type) {
			case string:
				key = NewKey(c, ref.Collection(), v, 0, nil)
			case int:
				key = NewKey(c, ref.Collection(), "", (int64)(v), nil)
			default:
				err = fmt.Errorf("unsupported ID type: %T", ref.ID)
			}
		}
	}
	return
}

func (database) GetMulti(c context.Context, records []dal.Record) error {
	count := len(records)
	keys := make([]*datastore.Key, count)
	values := make([]any, count)
	for i := range records {
		record := records[i]
		recordKey := record.Key()
		kind := recordKey.Collection()
		var intID int64
		var strID string
		switch v := recordKey.ID.(type) {
		case string:
			strID = v
		case int:
			intID = (int64)(v)
		}
		keys[i] = NewKey(c, kind, strID, intID, nil)
		values[i] = record.Data()
	}
	if err := GetMulti(c, keys, values); err != nil {
		return err
	}
	return nil
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
