package dalgo2datastore

import (
	"cloud.google.com/go/datastore"
	"context"
	"errors"
	"fmt"
	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/record"
	"github.com/strongo/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type dsInserter = func(key *datastore.Key, isPartialKey bool, dst any) error
type dsExister = func(key *datastore.Key) error

// maxIDGenerationAttempts bounds retries when an explicit dal.IDGenerator is supplied
// and the generated ID is already taken by an existing entity.
const maxIDGenerationAttempts = 5

func (tx transaction) Insert(c context.Context, record record.Record, opts ...dal.InsertOption) error {
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
		// NewInsert (not Put) so a duplicate key fails the write instead of
		// silently overwriting the existing entity: Put/PutMulti are pure
		// upserts in the Datastore client, with no existence check at all.
		//
		// A Datastore transaction only buffers Put/Mutate calls locally and
		// performs the write when the transaction commits, so a duplicate-key
		// AlreadyExists status can NOT surface from this call — it surfaces
		// from the Commit inside runInTransaction, which classifies it there.
		if _, err = tx.datastoreTx.Mutate(datastore.NewInsert(key, dst)); err != nil {
			return err
		}
		return nil
	}
	var exister = func(key *datastore.Key) error {
		return tx.datastoreTx.Get(key, nil)
	}
	return insert(c, record, inserter, exister, dal.NewInsertOptions(opts...))
}

func (db database) Insert(c context.Context, record record.Record, opts ...dal.InsertOption) error {
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
		// NewInsert (not Put) so a duplicate key fails the write instead of
		// silently overwriting the existing entity; see the comment in
		// transaction.Insert above for why Put/PutMulti cannot detect this.
		// Outside a transaction, Mutate performs its Commit synchronously, so
		// (unlike the transactional path) an AlreadyExists status surfaces
		// directly from this call and is classified here.
		var keys []*datastore.Key
		if keys, err = db.client.Mutate(c, datastore.NewInsert(key, dst)); err != nil {
			return wrapIfAlreadyExists(err)
		}
		updatePartialKey(record.Key(), keys[0])
		return nil
	}
	var exister = func(key *datastore.Key) error {
		return db.client.Get(c, key, nil)
	}
	return insert(c, record, inserter, exister, options)
}

func updatePartialKey(key *record.Key, dsKey *datastore.Key) {
	key.ID = dsKey.ID
}

// wrapIfAlreadyExists classifies a Datastore duplicate-key failure so it
// satisfies record.IsAlreadyExists, without discarding the underlying gRPC
// status error (wrap, don't replace).
//
// NewInsert is the only mutation type this adapter issues that Datastore can
// reject as a duplicate (Set/SetMulti stay on Put/PutMulti, which are pure
// upserts and never fail this way), so any codes.AlreadyExists observed here
// — or at a transaction's Commit, see runInTransaction — unambiguously means
// a duplicate key on an Insert.
func wrapIfAlreadyExists(err error) error {
	if err == nil {
		return nil
	}
	if status.Code(err) == codes.AlreadyExists {
		return fmt.Errorf("%w: %v", record.ErrRecordExists, err)
	}
	return err
}

func insert(ctx context.Context, rec record.Record, insert dsInserter, exists dsExister, options dal.InsertOptions) error {
	if rec == nil {
		panic("record == nil")
	}
	recordKey := rec.Key()
	kind := recordKey.Collection()
	rec.SetError(nil)
	entity := rec.Data()
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
			recordExists := func(key *record.Key) error {
				var k *datastore.Key
				k, _, err = getDatastoreKey(key)
				if err != nil {
					return err
				}
				if err = exists(k); errors.Is(err, datastore.ErrNoSuchEntity) {
					return record.ErrRecordNotFound
				} else {
					return err
				}
			}
			insertRandom := func(record record.Record) error {
				var k *datastore.Key
				if k, _, err = getDatastoreKey(record.Key()); err != nil {
					return err
				}
				return insert(k, false, record.Data())
			}
			return dal.InsertWithIdGenerator(ctx, rec, idGenerator, maxIDGenerationAttempts, recordExists, insertRandom)
		}

		// Both dal.WithAdapterGeneratedID and the default behavior for incomplete keys
		// use Datastore's native ID allocation; the inserter writes the allocated ID
		// back into record.Key().ID before returning.
		return insert(key, true, rec.Data())
	}

	err = insert(key, isPartial, rec.Data())
	return err
}
