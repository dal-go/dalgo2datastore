package dalgo2datastore

import (
	"cloud.google.com/go/datastore"
	"context"
	"errors"
	"fmt"
	"github.com/dal-go/dalgo/dal"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"reflect"
)

func selectAllIDs(c context.Context, projectID string, query dal.Query) (ids []any, err error) {
	var client *datastore.Client
	if client, err = datastore.NewClient(c, projectID, option.WithoutAuthentication()); err != nil {
		return ids, err
	}
	q := dalQuery2datastoreQuery(query).KeysOnly()
	reader := client.Run(c, q)
	for {
		var key *datastore.Key
		key, err = reader.Next(nil)
		if err != nil {
			if err == iterator.Done {
				err = nil
				break
			}
			return ids, err
		}
		id, err := idFromKey(key, query.IDKind)
		if err != nil {
			return ids, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func idFromKey(key *datastore.Key, idKind reflect.Kind) (id any, err error) {
	switch idKind {
	case reflect.Invalid:
		return nil, errors.New("id kind is 0 e.g. 'reflect.Invalid'")
	case reflect.String:
		return key.Name, nil
	case reflect.Int64:
		return key.ID, nil
	case reflect.Int:
		return int(key.ID), nil
	case reflect.Int32:
		return int(key.ID), nil
	case reflect.Int16:
		return int(key.ID), nil
	case reflect.Int8:
		return int(key.ID), nil
	default:
		return key, fmt.Errorf("unsupported id kind: %v", idKind)
	}
}
