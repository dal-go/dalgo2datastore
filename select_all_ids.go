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

func selectAllIDsWorker(c context.Context, projectID string, query dal.Query, addID func(key *datastore.Key) error) (err error) {
	var client *datastore.Client
	if client, err = datastore.NewClient(c, projectID, option.WithoutAuthentication()); err != nil {
		return err
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
			return err
		}
		if err = addID(key); err != nil {
			return err
		}
	}
	return err
}

func selectAllIDs(c context.Context, projectID string, query dal.Query) (ids []any, err error) {
	return ids, selectAllIDsWorker(c, projectID, query, func(key *datastore.Key) error {
		id, err := idFromKey(key, query.IDKind)
		if err != nil {
			return err
		}
		ids = append(ids, id)
		return nil
	})
}

func selectAllStrIDs(c context.Context, projectID string, query dal.Query) (ids []string, err error) {
	return ids, selectAllIDsWorker(c, projectID, query, func(key *datastore.Key) error {
		ids = append(ids, key.Name)
		return nil
	})
}

func selectAllIntIDs(c context.Context, projectID string, query dal.Query) (ids []int, err error) {
	return ids, selectAllIDsWorker(c, projectID, query, func(key *datastore.Key) error {
		ids = append(ids, int(key.ID))
		return nil
	})
}

func selectAllInt64IDs(c context.Context, projectID string, query dal.Query) (ids []int64, err error) {
	return ids, selectAllIDsWorker(c, projectID, query, func(key *datastore.Key) error {
		ids = append(ids, key.ID)
		return nil
	})
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
