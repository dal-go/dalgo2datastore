package dalgo2datastore

import (
	"context"

	"cloud.google.com/go/datastore"
	"github.com/dal-go/dalgo/dal"
	"google.golang.org/api/option"
)

func getDatastoreIterator(c context.Context, projectID string, query dal.Query) (client *datastore.Client, reader *datastore.Iterator, err error) {
	var q *datastore.Query
	if q, err = dalQuery2datastoreQuery(query); err != nil {
		return
	}
	if client, err = datastore.NewClient(c, projectID, option.WithoutAuthentication()); err != nil {
		return
	}
	return client, client.Run(c, q), nil
}

func getRecordsReader(c context.Context, projectID string, query dal.Query) (reader dal.RecordsReader, err error) {
	r := datastoreReader{
		query: query,
	}
	r.client, r.iterator, err = getDatastoreIterator(c, projectID, query)
	return &r, err
}
