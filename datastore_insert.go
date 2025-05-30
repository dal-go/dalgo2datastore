package dalgo2datastore

import (
	"cloud.google.com/go/datastore"
	"context"
	"errors"
	"fmt"
	"github.com/dal-go/dalgo/dal"
	"github.com/strongo/log"
)

type dsInserter = func(key *datastore.Key, isPartialKey bool, dst any) error
type dsExister = func(key *datastore.Key) error

func (tx transaction) Insert(c context.Context, record dal.Record, opts ...dal.InsertOption) error {
	var inserter = func(key *datastore.Key, isPartialKey bool, dst any) (err error) {
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
	var inserter = func(key *datastore.Key, isPartialKey bool, dst any) (err error) {
		if key, err = db.client.Put(c, key, dst); err != nil {
			return err
		}
		updatePartialKey(record.Key(), key)
		return nil
	}
	var exister = func(key *datastore.Key) error {
		return db.client.Get(c, key, nil)
	}
	return insert(c, record, inserter, exister, options)
}

func updatePartialKey(key *dal.Key, dsKey *datastore.Key) {
	key.ID = dsKey.ID
}

func insert(ctx context.Context, record dal.Record, insert dsInserter, exists dsExister, options dal.InsertOptions) error {
	if record == nil {
		panic("record == nil")
	}
	recordKey := record.Key()
	kind := recordKey.Collection()
	record.SetError(nil)
	entity := record.Data()
	if entity == nil {
		panic("record == nil")
	}

	wrapErr := func(err error) error {
		return fmt.Errorf("failed to create record with random str ID for [%s]: %w", kind, err)
	}
	key, isPartial, err := getDatastoreKey(recordKey)
	if err != nil {
		return wrapErr(err)
	}
	if isPartial {
		if idGenerator := options.IDGenerator(); idGenerator != nil {
			recordExists := func(key *dal.Key) error {
				var k *datastore.Key
				k, _, err = getDatastoreKey(key)
				if err != nil {
					return err
				}
				if err = exists(k); errors.Is(err, datastore.ErrNoSuchEntity) {
					return dal.ErrRecordNotFound
				} else {
					return err
				}
			}
			insertRandom := func(record dal.Record) error {
				return insert(key, false, record.Data())
			}
			return dal.InsertWithIdGenerator(ctx, record, idGenerator, 5, recordExists, insertRandom)
		}

		panic(fmt.Sprintf("database.insert() called for key with incomplete ID: %+v", key))
	}

	err = insert(key, isPartial, record.Data())
	return err
}
