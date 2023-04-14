package dalgo2datastore

import (
	"cloud.google.com/go/datastore"
	"context"
	"github.com/dal-go/dalgo/dal"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

func selectAll(c context.Context, projectID string, query dal.Query) (records []dal.Record, err error) {
	var client *datastore.Client
	if client, err = datastore.NewClient(c, projectID, option.WithoutAuthentication()); err != nil {
		return
	}
	q := dalQuery2datastoreQuery(query)
	reader := client.Run(c, q)
	for {
		var key *datastore.Key
		r := query.Into()
		data := r.Data()
		if rd, ok := data.(dal.RecordData); ok {
			data = rd.DTO()
		}
		key, err = reader.Next(data)
		if err != nil {
			if err == iterator.Done {
				err = nil
				break
			}
			return records, err
		}
		idKind := r.Key().IDKind
		k := dal.NewIncompleteKey(query.From.Name, idKind, nil)
		if k.ID, err = idFromKey(key, idKind); err != nil {
			return records, err
		}
		records = append(records, r)
	}
	return records, err
}
