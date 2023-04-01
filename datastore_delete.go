package dalgo2datastore

import (
	"cloud.google.com/go/datastore"
	"context"
	"errors"
	"github.com/strongo/dalgo/dal"
	"strconv"
)

type deleter = func(key *datastore.Key) error

func (tx transaction) Delete(ctx context.Context, key *dal.Key) error {
	return delete(ctx, key, tx.datastoreTx.Delete)
}

func (db database) Delete(c context.Context, record dal.Record) (err error) {
	return delete(c, record.Key(), func(key *datastore.Key) error {
		return db.Client.Delete(c, key)
	})
}

func delete(ctx context.Context, dalgoKey *dal.Key, delete deleter) error {
	datastoreKey, isIncomplete, err := getDatastoreKey(ctx, dalgoKey)
	if err != nil {
		return err
	}
	if isIncomplete {
		return errors.New("can't delete record by incomplete key: " + strconv.Quote(dalgoKey.String()))
	}
	return delete(datastoreKey)
}
