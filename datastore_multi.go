package dalgo2datastore

import (
	"cloud.google.com/go/datastore"
	"fmt"
	"github.com/dal-go/dalgo/dal"
)

func datastoreKeysAndValues(records []dal.Record) (keys []*datastore.Key, values []any, err error) {
	count := len(records)
	keys = make([]*datastore.Key, count)
	values = make([]any, count)
	for i := range records {
		record := records[i]
		recordKey := record.Key()
		if keys[i], _, err = getDatastoreKey(recordKey); err != nil {
			return
		}
		data := record.Data()
		if recordData, ok := data.(dal.RecordData); ok {
			data = recordData.DTO()
		}
		values[i] = data
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
