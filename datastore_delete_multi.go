package dalgo2datastore

import (
	"cloud.google.com/go/datastore"
	"context"
	"errors"
	"github.com/dal-go/record"
	"strconv"
)

type multiDeleter = func(keys []*datastore.Key) error

func (tx transaction) DeleteMulti(ctx context.Context, keys []*record.Key) error {
	return deleteMulti(keys, tx.datastoreTx.DeleteMulti)
}

func (db database) DeleteMulti(c context.Context, recordKeys []*record.Key) (err error) {
	return deleteMulti(recordKeys, func(keys []*datastore.Key) error {
		return db.client.DeleteMulti(c, keys)
	})
}

func deleteMulti(dalgoKeys []*record.Key, deleteMulti multiDeleter) (err error) {
	keys := make([]*datastore.Key, len(dalgoKeys))
	for i, k := range dalgoKeys {
		var isIncomplete bool
		if keys[i], isIncomplete, err = getDatastoreKey(k); err != nil {
			return err
		}
		if isIncomplete {
			return errors.New("can't delete record by incomplete key: " + strconv.Quote(k.String()))
		}
	}
	return deleteMulti(keys)
}
