package dalgo2gaedatastore

import (
	"cloud.google.com/go/datastore"
)

var (
	// LoggingEnabled a flag to enable or disable logging inside GAE DAL
	LoggingEnabled = true // TODO: move to Context.WithValue()
	//mockDB         *mock_dal.MockDatabase

	// NewIncompleteKey creates new incomplete key.
	NewIncompleteKey = datastore.IncompleteKey

	// NewKey creates new key.
	NewKey = datastore.IDKey

	//dbRunInTransaction = datastore.Transaction{}
	//dbGet              = datastore.Get
	//dbGetMulti         = datastore.GetMulti
	//dbPut              = datastore.Put
	//dbPutMulti         = datastore.PutMulti
	//dbDelete           = datastore.Delete
	//dbDeleteMulti      = datastore.DeleteMulti
)
