package dalgo2datastore

import (
	"cloud.google.com/go/datastore"
	"errors"
	"fmt"
	"github.com/dal-go/dalgo/dal"
	"google.golang.org/api/iterator"
)

var _ dal.Reader = (*datastoreReader)(nil)

type datastoreReader struct {
	i        int // iteration
	query    dal.Query
	iterator *datastore.Iterator
}

func (d *datastoreReader) Close() error {
	return nil
}

func (d *datastoreReader) Next() (record dal.Record, err error) {
	if limit := d.query.Limit(); limit > 0 && d.i >= limit {
		return nil, dal.ErrNoMoreRecords
	}

	if into := d.query.Into(); into == nil {
		from := d.query.From()
		record = dal.NewRecordWithIncompleteKey(from.Name(), d.query.IDKind(), nil)
	} else {
		record = into()
	}
	record.SetError(nil)
	data := record.Data()
	if rd, ok := data.(dal.DataWrapper); ok {
		data = rd.Data()
	}
	var key *datastore.Key
	if key, err = d.iterator.Next(data); err != nil {
		if errors.Is(err, iterator.Done) {
			err = fmt.Errorf("%w: %v", dal.ErrNoMoreRecords, err)
		}
		return record, err
	}
	k := record.Key()
	if k.ID, err = idFromDatastoreKey(key, k.IDKind); err != nil {
		return record, err
	}
	d.i++
	return
}

func (d *datastoreReader) Cursor() (string, error) {
	cursor, err := d.iterator.Cursor()
	if err != nil {
		return "", err
	}
	return cursor.String(), nil
}
