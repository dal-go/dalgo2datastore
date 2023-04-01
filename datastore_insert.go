package dalgo2datastore

import (
	"cloud.google.com/go/datastore"
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/strongo/dalgo/dal"
	"github.com/strongo/log"
)

type inserter = func(key *datastore.Key, isPartialKey bool, dst interface{}) error
type exister = func(key *datastore.Key) error

func (tx transaction) Insert(c context.Context, record dal.Record, opts ...dal.InsertOption) error {
	var inserter = func(key *datastore.Key, isPartialKey bool, dst interface{}) (err error) {
		var pendingKey *datastore.PendingKey
		pendingKey, err = tx.datastoreTx.Put(key, dst)
		if err != nil {
			return err
		}
		if isPartialKey && pendingKey != nil {
			tx.pendingKeys = append(tx.pendingKeys, partialKey{record.Key(), pendingKey})
		}
		return nil
	}
	var exister = func(key *datastore.Key) error {
		return tx.datastoreTx.Get(key, nil)
	}
	return insert(c, record, inserter, exister, dal.NewInsertOptions(opts...))
}

func (db database) Insert(c context.Context, record dal.Record, opts ...dal.InsertOption) error {
	if record == nil {
		panic("record == nil")
	}
	recordKey := record.Key()
	kind := recordKey.Collection()
	log.Debugf(c, "Insert(kind=%v)", kind)
	data := record.Data()
	if data == nil {
		return errors.New("not allowed to insert nil data")
	}
	options := dal.NewInsertOptions(opts...)
	var inserter = func(key *datastore.Key, isPartialKey bool, dst interface{}) (err error) {
		if key, err = db.Client.Put(c, key, dst); err != nil {
			return err
		}
		updatePartialKey(record.Key(), key)
		return nil
	}
	var exister = func(key *datastore.Key) error {
		return db.Client.Get(c, key, nil)
	}
	return insert(c, record, inserter, exister, options)
}

func updatePartialKey(key *dal.Key, dsKey *datastore.Key) {
	key.ID = dsKey.ID
}

func insert(c context.Context, record dal.Record, insert inserter, exists exister, options dal.InsertOptions) error {
	if record == nil {
		panic("record == nil")
	}
	recordKey := record.Key()
	kind := recordKey.Collection()
	entity := record.Data()
	if entity == nil {
		panic("record == nil")
	}

	wrapErr := func(err error) error {
		return errors.WithMessage(err, "failed to create record with random str ID for: "+kind)
	}
	key, isPartial, err := getDatastoreKey(c, recordKey)
	if err != nil {
		return wrapErr(err)
	}
	if isPartial {
		if idGenerator := options.IDGenerator(); idGenerator != nil {
			recordExists := func(key *dal.Key) error {
				k, _, err := getDatastoreKey(c, key)
				if err != nil {
					return err
				}
				if err := exists(k); err == datastore.ErrNoSuchEntity {
					return dal.ErrRecordNotFound
				} else {
					return err
				}
			}
			insertRandom := func(record dal.Record) error {
				return insert(key, false, record.Data())
			}
			return dal.InsertWithRandomID(c, record, idGenerator, 5, recordExists, insertRandom)
		}

		panic(fmt.Sprintf("database.insert() called for key with incomplete ID: %+v", key))
	}

	err = insert(key, isPartial, record.Data())
	return err
}
