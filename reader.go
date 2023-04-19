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

func (d datastoreReader) Next() (record dal.Record, err error) {
	if d.query.Limit > 0 && d.i >= d.query.Limit {
		return nil, dal.ErrNoMoreRecords
	}
	if d.query.Into == nil {
		record = dal.NewRecordWithIncompleteKey(d.query.From.Name, d.query.IDKind, nil)
	} else {
		record = d.query.Into()
	}
	data := record.Data()
	if rd, ok := data.(dal.RecordData); ok {
		data = rd.DTO()
	}
	var key *datastore.Key
	if key, err = d.iterator.Next(data); err != nil {
		if errors.Is(err, iterator.Done) {
			err = fmt.Errorf("%w: %v", dal.ErrNoMoreRecords, err)
		}
		return record, err
	}
	idKind := record.Key().IDKind
	k := dal.NewIncompleteKey(d.query.From.Name, idKind, nil)
	if k.ID, err = idFromKey(key, idKind); err != nil {
		return record, err
	}
	record = dal.NewRecordWithData(k, record.Data())
	d.i++
	return
}

func (d datastoreReader) Cursor() (string, error) {
	cursor, err := d.iterator.Cursor()
	if err != nil {
		return "", err
	}
	return cursor.String(), nil
}
