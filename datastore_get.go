package dalgo2datastore

import (
	"cloud.google.com/go/datastore"
	"context"
	"errors"
	"github.com/dal-go/dalgo/dal"
	"strconv"
)

type getter = func(key *datastore.Key, dst interface{}) error

func (tx transaction) Get(ctx context.Context, record dal.Record) error {
	return get(ctx, record, tx.datastoreTx.Get)
}

func (db database) Get(c context.Context, record dal.Record) (err error) {
	return get(c, record, func(key *datastore.Key, dst interface{}) error {
		return db.client.Get(c, key, dst)
	})
}

func get(ctx context.Context, record dal.Record, get getter) error {
	datastoreKey, isIncomplete, err := getDatastoreKey(ctx, record.Key())
	if err != nil {
		return err
	}
	if isIncomplete {
		return errors.New("can't get record by incomplete key: " + strconv.Quote(record.Key().String()))
	}
	if err := get(datastoreKey, record.Data()); err != nil {
		if err == datastore.ErrNoSuchEntity {
			err = dal.NewErrNotFoundByKey(record.Key(), err)
			record.SetError(err)
		}
	}
	return nil
}
