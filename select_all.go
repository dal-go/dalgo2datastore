package dalgo2datastore

import (
	"cloud.google.com/go/datastore"
	"context"
	"github.com/dal-go/dalgo/dal"
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
