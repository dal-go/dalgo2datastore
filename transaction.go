package dalgo2datastore

import (
	"cloud.google.com/go/datastore"
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/strongo/dalgo/dal"
	"github.com/strongo/log"
)

func (db database) RunReadonlyTransaction(ctx context.Context, f dal.ROTxWorker, options ...dal.TransactionOption) error {
	_, err := db.runInTransaction(ctx, append(options, dal.TxWithReadonly()), func(tx transaction) error {
		return f(ctx, tx)
	})
	if err != nil {
		return err
	}
	return nil
}

func (db database) RunReadwriteTransaction(ctx context.Context, f dal.RWTxWorker, options ...dal.TransactionOption) error {
	_, err := db.runInTransaction(ctx, options, func(tx transaction) error {
		return f(ctx, tx)
	})
	return err
}

func (db database) runInTransaction(c context.Context, opts []dal.TransactionOption, f func(tx transaction) error) (cmt *datastore.Commit, err error) {
	var tx transaction
	tx.db = db
	tx.dalgoTxOptions = dal.NewTransactionOptions(opts...)
	var dsTxOptions []datastore.TransactionOption
	//tx.datastoreTxOptions.XG = tx.dalgoTxOptions.IsCrossGroup()
	if tx.dalgoTxOptions.IsReadonly() {
		dsTxOptions = append(dsTxOptions, datastore.ReadOnly)
	}
	if tx.dalgoTxOptions.IsCrossGroup() {
		dsTxOptions = append(dsTxOptions, datastore.MaxAttempts(tx.dalgoTxOptions.Attempts()))
	}
	return db.Client.RunInTransaction(c, func(datastoreTx *datastore.Transaction) error {
		tx.datastoreTx = datastoreTx
		if err := f(tx); err != nil {
			return err
		}
		//if _, err := datastoreTx.Commit(); err != nil {
		//	return err
		//}
		return nil
	}, dsTxOptions...)
}

var _ dal.Transaction = (*transaction)(nil)
var _ dal.ReadwriteTransaction = (*transaction)(nil)

type partialKey struct {
	dalgo   *dal.Key
	pending *datastore.PendingKey
}

type transaction struct {
	db             database
	dalgoTxOptions dal.TransactionOptions
	datastoreTx    *datastore.Transaction
	pendingKeys    []partialKey
}

func (tx transaction) Select(ctx context.Context, query dal.Select) (dal.Reader, error) {
	return nil, errors.New("implement me")
}

func (tx transaction) Update(ctx context.Context, key *dal.Key, updates []dal.Update, preconditions ...dal.Precondition) error {
	return dal.ErrNotSupported
}

func (tx transaction) UpdateMulti(c context.Context, keys []*dal.Key, updates []dal.Update, preconditions ...dal.Precondition) error {
	return dal.ErrNotSupported
}

func (tx transaction) Options() dal.TransactionOptions {
	return tx.dalgoTxOptions
}

func (tx transaction) Set(c context.Context, record dal.Record) error {
	data := record.Data()
	log.Debugf(c, "data: %+v", data)
	if data == nil {
		panic("record.Data() == nil")
	}
	if key, isIncomplete, err := getDatastoreKey(c, record.Key()); err != nil {
		return err
	} else if isIncomplete {
		log.Errorf(c, "database.Update() called for incomplete key, will insert.")
		panic("not implemented")
		//return gaeDb.Insert(c, record, dal.NewInsertOptions(dal.WithRandomStringID(5)))
	} else if _, err = Put(c, tx.db.Client, key, data); err != nil {
		return errors.WithMessage(err, "failed to update "+key2str(key))
	}
	return nil
}

func (tx transaction) SetMultiOld(c context.Context, records []dal.Record) (err error) { // TODO: Rename to PutMulti?

	keys := make([]*datastore.Key, len(records))
	values := make([]any, len(records))

	insertedIndexes := make([]int, 0, len(records))

	for i, record := range records {
		if record == nil {
			panic(fmt.Sprintf("records[%v] is nil: %v", i, record))
		}
		isIncomplete := false
		if keys[i], isIncomplete, err = getDatastoreKey(c, record.Key()); err != nil {
			return
		} else if isIncomplete {
			insertedIndexes = append(insertedIndexes, i)
		}
		if values[i] = record.Data(); values[i] == nil {
			return fmt.Errorf("records[%d].Data() == nil", i)
		}
	}

	// logKeys(c, "database.SetMulti", keys)

	if keys, err = PutMulti(c, tx.db.Client, keys, values); err != nil {
		switch err := err.(type) {
		case datastore.MultiError:
			if len(err) == len(records) {
				for i, e := range err {
					if err != nil {
						records[i].SetError(e)
					}
				}
				return nil
			}
		}
		return
	}

	for _, i := range insertedIndexes {
		setRecordID(keys[i], records[i])
		//records[i].SetData(values[i]) // it seems useless but covers case when .Data() returned newly created object without storing inside record
	}
	return
}

//func (t transaction) Update(ctx context.Context, key *dal.Key, updates []dal.Update, preconditions ...dal.Precondition) error {
//	//TODO implement me
//	panic("implement me")
//}
//
//func (t transaction) SetMulti(c context.Context, keys []*dal.Key, updates []dal.Update, preconditions ...dal.Precondition) error {
//	//TODO implement me
//	panic("implement me")
//}
//
//func (t transaction) Select(ctx context.Context, query dal.Select) (dal.Reader, error) {
//	panic("implement me")
//}

//func (t transaction) Insert(ctx context.Context, record dal.Record, opts ...dal.InsertOption) error {
//	options := dal.NewInsertOptions(opts...)
//	idGenerator := options.IDGenerator()
//	key := record.Key()
//	if key.ID == nil {
//		key.ID = idGenerator(ctx, record)
//	}
//	dr := t.dtb.doc(key)
//	data := record.Data()
//	return t.tx.Create(dr, data)
//}
//
//func (t transaction) Upsert(_ context.Context, record dal.Record) error {
//	dr := t.dtb.doc(record.Key())
//	return t.tx.Set(dr, record.Data())
//}
//
//func (t transaction) Get(_ context.Context, record dal.Record) error {
//	key := record.Key()
//	docRef := t.dtb.doc(key)
//	docSnapshot, err := t.tx.Get(docRef)
//	return docSnapshotToRecord(err, docSnapshot, record, func(ds *firestore.DocumentSnapshot, p interface{}) error {
//		return ds.DataTo(p)
//	})
//}
//
//func (t transaction) Set(ctx context.Context, record dal.Record) error {
//	dr := t.dtb.doc(record.Key())
//	return t.tx.Set(dr, record.Data())
//}
//
//func (t transaction) Delete(ctx context.Context, key *dal.Key) error {
//	dr := t.dtb.doc(key)
//	return t.tx.Delete(dr)
//}
//
//func (t transaction) GetMulti(ctx context.Context, records []dal.Record) error {
//	dr := make([]*firestore.DocumentRef, len(records))
//	for i, r := range records {
//		dr[i] = t.dtb.doc(r.Key())
//	}
//	ds, err := t.tx.GetAll(dr)
//	if err != nil {
//		return err
//	}
//	for i, d := range ds {
//		err = docSnapshotToRecord(nil, d, records[i], func(ds *firestore.DocumentSnapshot, p interface{}) error {
//			return ds.DataTo(p)
//		})
//		if err != nil {
//			return err
//		}
//	}
//	return nil
//}
//
//func (t transaction) SetMulti(ctx context.Context, records []dal.Record) error {
//	for _, record := range records { // TODO: can we do this in parallel?
//		doc := t.dtb.doc(record.Key())
//		_, err := doc.Set(ctx, record.Data())
//		if err != nil {
//			record.SetError(err)
//			return err
//		}
//	}
//	return nil
//}
//
//func (t transaction) DeleteMulti(_ context.Context, keys []*dal.Key) error {
//	for _, k := range keys {
//		dr := t.dtb.doc(k)
//		if err := t.tx.Delete(dr); err != nil {
//			return fmt.Errorf("failed to delete record: %w", err)
//		}
//	}
//	return nil
//}
