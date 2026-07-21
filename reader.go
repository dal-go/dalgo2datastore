package dalgo2datastore

import (
	"errors"
	"fmt"

	"cloud.google.com/go/datastore"
	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/record"
	"google.golang.org/api/iterator"
)

var _ dal.Reader = (*datastoreReader)(nil)

type datastoreReader struct {
	i        int // iteration
	query    dal.Query
	client   *datastore.Client
	iterator *datastore.Iterator
}

func (d *datastoreReader) Close() error {
	return d.client.Close()
}

func (d *datastoreReader) Next() (rec record.Record, err error) {
	if limit := d.query.Limit(); limit > 0 && d.i >= limit {
		return nil, dal.ErrNoMoreRecords
	}

	switch query := d.query.(type) {
	case dal.StructuredQuery:
		rec = query.IntoRecord()
		if rec == nil {
			from := query.From()
			rec = record.NewRecordWithIncompleteKey(from.Base().Name(), query.IDKind(), nil)
		}
		rec.SetError(nil)
		data := rec.Data()
		if rd, ok := data.(dal.DataWrapper); ok {
			data = rd.Data()
		}
		var key *datastore.Key
		if key, err = d.iterator.Next(data); err != nil {
			if errors.Is(err, iterator.Done) {
				err = fmt.Errorf("%w: %v", dal.ErrNoMoreRecords, err)
			}
			return rec, err
		}
		k := rec.Key()
		if k.ID, err = idFromDatastoreKey(key, k.IDKind); err != nil {
			return rec, err
		}
		d.i++
	default:
		err = fmt.Errorf("%w: %T", dal.ErrNotSupported, d.query)
	}
	return
}

func (d *datastoreReader) Cursor() (string, error) {
	cursor, err := d.iterator.Cursor()
	if err != nil {
		return "", err
	}
	return cursor.String(), nil
}
