package dalgo2datastore

import (
	"cloud.google.com/go/datastore"
	"errors"
	"fmt"
	"reflect"
)

func idFromDataStoreKey(key *datastore.Key, idKind reflect.Kind) (id any, err error) {
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
