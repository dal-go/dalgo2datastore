package dalgo2datastore

import (
	"cloud.google.com/go/datastore"
	"errors"
	"fmt"
	"reflect"
	"strconv"
)

func idFromDatastoreKey(key *datastore.Key, idKind reflect.Kind) (id any, err error) {
	if key.Incomplete() {
		return nil, errors.New("datastore key is incomplete: neither key.Name nor key.ID is set")
	}
	switch idKind {
	case reflect.Invalid:
		return nil, errors.New("id kind is 0 e.g. 'reflect.Invalid'")
	case reflect.String:
		if key.Name == "" {
			return strconv.FormatInt(key.ID, 10), nil
		}
		return key.Name, nil
	default:
		id := key.ID
		if id == 0 {
			if id, err = strconv.ParseInt(key.Name, 10, 64); err != nil {
				return nil, fmt.Errorf("failed to autoconvert key.Name to int: %w", err)
			}
		}
		switch idKind {
		case reflect.Int64:
			return id, nil
		case reflect.Int:
			return int(id), nil
		case reflect.Int32:
			return int(id), nil
		case reflect.Int16:
			return int(id), nil
		case reflect.Int8:
			return int(id), nil
		default:
			return key, fmt.Errorf("unsupported id type: %T=%v", idKind, idKind)
		}
	}
}
