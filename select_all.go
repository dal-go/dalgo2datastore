package dalgo2datastore

import (
	"cloud.google.com/go/datastore"
	"context"
	"errors"
	"fmt"
	"github.com/dal-go/dalgo/dal"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

func getDatastoreIterator(c context.Context, projectID string, query dal.Query) (reader *datastore.Iterator, err error) {
	var client *datastore.Client
	if client, err = datastore.NewClient(c, projectID, option.WithoutAuthentication()); err != nil {
		return
	}
	q := dalQuery2datastoreQuery(query)
	return client.Run(c, q), nil
}

func getReader(c context.Context, projectID string, query dal.Query) (reader dal.Reader, err error) {
	var dsIterator *datastore.Iterator
	if dsIterator, err = getDatastoreIterator(c, projectID, query); err != nil {
		return
	}
	return datastoreReader{query: query, iterator: dsIterator}, nil
}

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

func selectAll(c context.Context, projectID string, query dal.Query) (records []dal.Record, err error) {
	var reader dal.Reader
	if reader, err = getReader(c, projectID, query); err != nil {
		return nil, err
	}
	for i := 0; i < query.Limit; i++ {
		var record dal.Record
		if record, err = reader.Next(); err != nil {
			return records, err
		}
		records = append(records, record)
	}
	return records, err
}
