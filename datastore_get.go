package dalgo2datastore

import (
	"cloud.google.com/go/datastore"
	"context"
	"errors"
	"github.com/dal-go/dalgo/dal"
	"strconv"
)

type getter = func(key *datastore.Key, dst interface{}) error

func (tx transaction) Get(_ context.Context, record dal.Record) error {
	return get(record, tx.datastoreTx.Get)
}

func (db database) Get(ctx context.Context, record dal.Record) (err error) {
	return get(record, func(key *datastore.Key, dst interface{}) error {
		return db.client.Get(ctx, key, dst)
	})
}

func get(record dal.Record, get getter) error {
	recordKey := record.Key()
	datastoreKey, isIncomplete, err := getDatastoreKey(recordKey)
	if err != nil {
		return err
	}
	if isIncomplete {
		return errors.New("can't get record by incomplete key: " + strconv.Quote(record.Key().String()))
	}
	record.SetError(nil)
	data := record.Data()
	if err := get(datastoreKey, data); err != nil {
		if errors.Is(err, datastore.ErrNoSuchEntity) {
			err = dal.NewErrNotFoundByKey(recordKey, err)
			record.SetError(err)
			return err
		}
	}
	return nil
}
