package dalgo2datastore

import (
	"bytes"
	"cloud.google.com/go/datastore"
	"context"
	"fmt"
	"github.com/strongo/log"
	"strconv"
)

func key2str(key *datastore.Key) string {
	if key == nil {
		return "nil"
	}
	kind := key.Kind
	if intID := key.ID; intID != 0 {
		return kind + ":int=" + strconv.FormatInt(intID, 10)
	} else if strID := key.Name; strID != "" {
		return kind + ":str=" + strID
	} else {
		return kind + ":new"
	}
}

func logKeys(c context.Context, f, suffix string, keys []*datastore.Key) {
	var buffer bytes.Buffer
	buffer.WriteString(f)
	if len(keys) == 0 {
		buffer.WriteString(")")
		return
	}
	buffer.WriteString("(\n")
	prevKey := "-"
	for _, key := range keys {
		ks := key2str(key)
		if ks == prevKey {
			log.Errorf(c, "Duplicate keys: "+ks)
		}
		buffer.WriteString(fmt.Sprintf("\t%v\n", ks))
		prevKey = ks
	}
	buffer.WriteString(")")
	log.Debugf(c, buffer.String())
}
