package dalgo2datastore

import (
	"cloud.google.com/go/datastore"
	"github.com/dal-go/dalgo/dal"
)

func dalQuery2datastoreQuery(query dal.Query) *datastore.Query {
	q := datastore.NewQuery(query.From.Name)
	if query.Limit > 0 {
		q = q.Limit(query.Limit)
	}
	return q
}
