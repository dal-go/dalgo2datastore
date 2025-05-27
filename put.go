package dalgo2datastore

import (
	"bytes"
	"cloud.google.com/go/datastore"
	"context"
	"fmt"
	"github.com/strongo/log"
)

// Put saves record to datastore
var Put = func(c context.Context, client *datastore.Client, key *datastore.Key, val any) (*datastore.Key, error) {
	if val == nil {
		panic("val == nil")
	}
	var err error
	isPartialKey := key.Incomplete()
	if LoggingEnabled {
		buf := new(bytes.Buffer)
		if err = logEntityProperties(buf, fmt.Sprintf("dbPut(%v) => properties:", key2str(key)), val); err != nil {
			log.Errorf(c, "Put(%v) failed to log properties: %v", key2str(key), err)
		} else {
			log.Debugf(c, buf.String())
		}
	}
	if key, err = client.Put(c, key, val); err != nil {
		return key, fmt.Errorf("failed to put to db (key=%v): %w", key2str(key), err)
	} else if LoggingEnabled && isPartialKey {
		log.Debugf(c, "dbPut() inserted new record with key: "+key2str(key))
	}
	return key, err
}

func logEntityProperties(buf *bytes.Buffer, prefix string, val any) (err error) {
	var props []datastore.Property
	if propertyLoadSaver, ok := val.(datastore.PropertyLoadSaver); ok {
		if props, err = propertyLoadSaver.Save(); err != nil {
			return fmt.Errorf("failed to call val.(datastore.PropertyLoadSaver).Save(): %w", err)
		}
	} else if props, err = datastore.SaveStruct(val); err != nil {
		return fmt.Errorf("failed to call datastore.SaveStruct(): %w", err)
	}
	_, _ = fmt.Fprint(buf, prefix)
	var prevPropName string
	for _, prop := range props {
		if prop.Name == prevPropName {
			_, _ = fmt.Fprintf(buf, ", %v", prop.Value)
		} else {
			_, _ = fmt.Fprintf(buf, "\n\t%v: %v", prop.Name, prop.Value)
		}
		prevPropName = prop.Name
	}
	return
}

// PutMulti saves multipe entities to datastore
var PutMulti = func(c context.Context, client *datastore.Client, keys []*datastore.Key, vals any) ([]*datastore.Key, error) {
	if LoggingEnabled {
		//buf := new(bytes.Buffer)
		//buf.WriteString(" => \n")
		//for i, key := range keys {
		//	logEntityProperties(buf, key2str(key) + ": ", vals[i]) // TODO: Needs use of reflection
		//}
		//logKeys(c, "dbPutMulti", buf.String(), keys)
		logKeys(c, "dbPutMulti", "", keys)
	}
	return client.PutMulti(c, keys, vals)
}
