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

// maxIDGenerationAttempts bounds retries when an explicit dal.IDGenerator is supplied
// and the generated ID is already taken by an existing entity.
const maxIDGenerationAttempts = 5

func (tx transaction) Insert(c context.Context, record dal.Record, opts ...dal.InsertOption) error {
	var inserter = func(key *datastore.Key, isPartialKey bool, dst any) (err error) {
		if isPartialKey {
			// The ID of a key put inside a Datastore transaction is not known until
			// the transaction commits, so allocate the ID upfront to be able to write
			// it back into the record key before returning.
			var keys []*datastore.Key
			if keys, err = tx.db.client.AllocateIDs(c, []*datastore.Key{key}); err != nil {
				return fmt.Errorf("failed to allocate ID for incomplete key: %w", err)
			}
			key = keys[0]
			updatePartialKey(record.Key(), key)
		}
		if _, err = tx.datastoreTx.Put(key, dst); err != nil {
			return err
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
				var k *datastore.Key
				if k, _, err = getDatastoreKey(record.Key()); err != nil {
					return err
				}
				return insert(k, false, record.Data())
			}
			return dal.InsertWithIdGenerator(ctx, record, idGenerator, maxIDGenerationAttempts, recordExists, insertRandom)
		}

		// Both dal.WithAdapterGeneratedID and the default behavior for incomplete keys
		// use Datastore's native ID allocation; the inserter writes the allocated ID
		// back into record.Key().ID before returning.
		return insert(key, true, record.Data())
	}

	err = insert(key, isPartial, record.Data())
	return err
}
