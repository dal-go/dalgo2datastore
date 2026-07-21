package dalgo2datastore

import (
	"cloud.google.com/go/datastore"
	"context"
	"errors"
	"github.com/dal-go/record"
	"strconv"
)

type deleter = func(key *datastore.Key) error

func (tx transaction) Delete(_ context.Context, key *record.Key) error {
	return runDeleter(key, tx.datastoreTx.Delete)
}

func (db database) Delete(ctx context.Context, record record.Record) (err error) {
	return runDeleter(record.Key(), func(key *datastore.Key) error {
		return db.client.Delete(ctx, key)
	})
}

func runDeleter(dalgoKey *record.Key, delete deleter) error {
	datastoreKey, isIncomplete, err := getDatastoreKey(dalgoKey)
	if err != nil {
		return err
	}
	if isIncomplete {
		return errors.New("can't delete record by incomplete key: " + strconv.Quote(dalgoKey.String()))
	}
	return delete(datastoreKey)
}
