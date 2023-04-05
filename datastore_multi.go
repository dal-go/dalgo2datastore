package dalgo2datastore

import (
	"cloud.google.com/go/datastore"
	"fmt"
	"github.com/dal-go/dalgo/dal"
)

func datastoreKeysAndValues(records []dal.Record) (keys []*datastore.Key, values []any) {
	count := len(records)
	keys = make([]*datastore.Key, count)
	values = make([]any, count)
	for i := range records {
		record := records[i]
		recordKey := record.Key()
		kind := recordKey.Collection()
		switch v := recordKey.ID.(type) {
		case string:
			keys[i] = datastore.NameKey(kind, v, nil)
		case int64:
			keys[i] = datastore.IDKey(kind, v, nil)
		case int:
			keys[i] = datastore.IDKey(kind, int64(v), nil)
		}
		values[i] = record.Data()
	}
	return
}

type operation string

const (
	operationGet operation = "get"
	operationSet operation = "set"
)

func handleMultiError(err datastore.MultiError, records []dal.Record, op operation) datastore.MultiError {
	if len(err) == 0 {
		return nil
	}
	if len(err) == len(records) {
		for i, e := range err {
			record := records[i]
			if e == datastore.ErrNoSuchEntity {
				record.SetError(fmt.Errorf("%w: %v", dal.ErrRecordNotFound, e))
			} else if e != nil {
				record.SetError(e)
			} else {
				record.SetError(dal.NoError)
			}
		}
		if op == operationGet {
			err = nil
		}
	}
	return err
}
