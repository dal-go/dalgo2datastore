package dalgo2datastore

import (
	"cloud.google.com/go/datastore"
	"context"
	"errors"
	"github.com/dal-go/dalgo/dal"
	"strconv"
)

type getter = func(key *datastore.Key, dst any) error

func (tx transaction) Get(_ context.Context, record dal.Record) error {
	return getRecord(record, tx.datastoreTx.Get)
}

func (tx transaction) Exists(_ context.Context, key *dal.Key) (exists bool, err error) {
	err = existsByKey(key, func(datastoreKey *datastore.Key, dst any) error {
		return tx.datastoreTx.Get(datastoreKey, dst)
	})
	if dal.IsNotFound(err) {
		return false, nil
	}
	return err == nil, err
}

func (db database) Get(ctx context.Context, record dal.Record) (err error) {
	return getRecord(record, func(datastoreKey *datastore.Key, dst any) error {
		return db.client.Get(ctx, datastoreKey, dst)
	})
}

func (db database) Exists(ctx context.Context, key *dal.Key) (exists bool, err error) {
	err = existsByKey(key, func(key *datastore.Key, dst any) error {
		return db.client.Get(ctx, key, dst)
	})
	if dal.IsNotFound(err) {
		return false, nil
	}
	return err == nil, err
}

func handleGetByKeyError(key *dal.Key, err error) error {
	if errors.Is(err, datastore.ErrNoSuchEntity) {
		err = dal.NewErrNotFoundByKey(key, err)
	}
	return err
}

func existsByKey(key *dal.Key, get getter) error {
	return getByKey(key, get, &struct{}{})
}

func getByKey(key *dal.Key, get getter, dst any) (err error) {
	datastoreKey, isIncomplete, err := getDatastoreKey(key)
	if err != nil {
		return err
	}
	if isIncomplete {
		return errors.New("can't get record by incomplete key: " + strconv.Quote(key.String()))
	}
	if err = get(datastoreKey, dst); err != nil {
		err = handleGetByKeyError(key, err)
	}
	return err
}

func getRecord(record dal.Record, get getter) (err error) {
	recordKey := record.Key()
	record.SetError(nil) // This is needed to call record.Data()
	data := record.Data()
	if err = getByKey(recordKey, get, data); err != nil {
		record.SetError(err)
		return err
	}
	return nil
}
