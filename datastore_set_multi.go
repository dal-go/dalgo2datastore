package dalgo2datastore

import (
	"cloud.google.com/go/datastore"
	"context"
	"github.com/dal-go/dalgo/dal"
)

type multiSetter = func(keys []*datastore.Key, dst []any) error

func (tx transaction) SetMulti(ctx context.Context, records []dal.Record) error {
	return setMulti(records, func(keys []*datastore.Key, dst []any) error {
		_, err := tx.datastoreTx.PutMulti(keys, dst)
		if err != nil {
			return err
		}
		return nil
	})
}

func (db database) SetMulti(c context.Context, records []dal.Record) error {
	return setMulti(records, func(keys []*datastore.Key, dst []any) error {
		_, err := db.client.PutMulti(c, keys, dst)
		if err != nil {
			return err
		}
		return nil
	})
}

func setMulti(records []dal.Record, setMulti multiSetter) (err error) {
	keys, values := datastoreKeysAndValues(records)
	if err := setMulti(keys, values); err != nil {
		switch err := err.(type) {
		case datastore.MultiError:
			return handleMultiError(err, records, operationSet)
		}
		return err
	}
	return nil
}
