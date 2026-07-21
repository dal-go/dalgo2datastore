package dalgo2datastore

import (
	"cloud.google.com/go/datastore"
	"errors"
	"fmt"
	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/record"
)

func datastoreKeysAndValues(records []record.Record) (keys []*datastore.Key, values []any, err error) {
	count := len(records)
	keys = make([]*datastore.Key, count)
	values = make([]any, count)
	for i := range records {
		record := records[i]
		recordKey := record.Key()
		if keys[i], _, err = getDatastoreKey(recordKey); err != nil {
			return
		}
		record.SetError(nil)
		data := record.Data()
		if recordData, ok := data.(dal.DataWrapper); ok {
			data = recordData.Data()
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

func handleMultiError(err datastore.MultiError, records []record.Record, op operation) datastore.MultiError {
	if len(err) == 0 {
		return nil
	}
	if len(err) == len(records) {
		for i, e := range err {
			rec := records[i]
			if errors.Is(e, datastore.ErrNoSuchEntity) {
				rec.SetError(fmt.Errorf("%w: %v", record.ErrRecordNotFound, e))
			} else if e != nil {
				rec.SetError(e)
			} else {
				rec.SetError(record.ErrNoError)
			}
		}
		if op == operationGet {
			err = nil
		}
	}
	return err
}
