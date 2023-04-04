package dalgo2datastore

import (
	"cloud.google.com/go/datastore"
	"errors"
)

// SaverWrapper used to serialize struct to properties on saving
type SaverWrapper struct {
	//record dal.Record
}

var _ datastore.PropertyLoadSaver = (*SaverWrapper)(nil)

// Load loads props
func (wrapper SaverWrapper) Load([]datastore.Property) (err error) {
	return errors.New("gaedb.SaverWrapper does not support Load() method")
}

// Save saves props
func (wrapper SaverWrapper) Save() (props []datastore.Property, err error) {
	return
}
