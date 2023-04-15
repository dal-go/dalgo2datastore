package dalgo2datastore

import (
	"cloud.google.com/go/datastore"
	"context"
	"errors"
	"github.com/dal-go/dalgo/dal"
	"strconv"
)

type multiDeleter = func(keys []*datastore.Key) error

func (tx transaction) DeleteMulti(ctx context.Context, keys []*dal.Key) error {
	return deleteMulti(ctx, keys, tx.datastoreTx.DeleteMulti)
}

func (db database) DeleteMulti(c context.Context, recordKeys []*dal.Key) (err error) {
	return deleteMulti(c, recordKeys, func(keys []*datastore.Key) error {
		return db.client.DeleteMulti(c, keys)
	})
}

func deleteMulti(ctx context.Context, dalgoKeys []*dal.Key, deleteMulti multiDeleter) (err error) {
	keys := make([]*datastore.Key, len(dalgoKeys))
	for i, k := range dalgoKeys {
		var isIncomplete bool
		if keys[i], isIncomplete, err = getDatastoreKey(ctx, k); err != nil {
			return err
		}
		if isIncomplete {
			return errors.New("can't delete record by incomplete key: " + strconv.Quote(k.String()))
		}
	}
	return deleteMulti(keys)
}
