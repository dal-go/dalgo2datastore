package dalgo2datastore

// NewMockKeyFromDatastoreKey create new mock key
//func NewMockKeyFromDatastoreKey(key *datastore.Key) *dal.Key {
//	if id := key.StringID(); id != "" {
//		return dal.NewKeyWithID(key.Kind(), id)
//	} else {
//		return dal.NewKeyWithID(key.Kind(), key.IntID())
//	}
//}

// SetupNdsMock sets NDS mock
//func SetupNdsMock(t *testing.T) {
//	if err := os.Setenv("GAE_LONG_APP_ID", "debtstracker"); err != nil {
//		panic(err)
//	}
//	if err := os.Setenv("GAE_PARTITION", "DEVTEST"); err != nil {
//		panic(err)
//	}
//	//c := gomock.NewController(t)
//	//mockDB = mock_dal.NewMockDatabase(c)
//
//	Get = func(c context.Context, key *datastore.Key, val any) error {
//		panic("not implemented")
//		//if c == nil {
//		//	panic("c == nil")
//		//}
//		//if key == nil {
//		//	panic("key == nil")
//		//}
//		//log.Debugf(c, "gaedb.Get(key=%v:%v)", key.Kind(), key.IntID())
//		//kind := key.Kind()
//		//
//		//if entitiesByKey, ok := mockDB.EntitiesByKind[kind]; !ok {
//		//	return datastore.ErrNoSuchEntity
//		//} else {
//		//	mockKey := NewMockKeyFromDatastoreKey(key)
//		//	if p, ok := entitiesByKey[mockKey]; !ok {
//		//		return datastore.ErrNoSuchEntity
//		//	} else {
//		//		if e, ok := val.(datastore.PropertyLoadSaver); ok {
//		//			return e.Load(p)
//		//		} else {
//		//			return datastore.LoadStruct(e, p)
//		//		}
//		//	}
//		//}
//	}
//
//	Put = func(c context.Context, key *datastore.Key, val any) (*datastore.Key, error) {
//		if c == nil {
//			panic("c == nil")
//		}
//		panic("not implemented")
//		//kind := key.Kind()
//		//entitiesByKey, ok := mockDB.EntitiesByKind[kind]
//		//if !ok {
//		//	//entitiesByKey = make(map[mockdb.MockKey][]datastore.Property)
//		//	//mockDB.EntitiesByKind[kind] = entitiesByKey
//		//}
//		//mockKey := NewMockKeyFromDatastoreKey(key)
//		//if key.StringID() == "" {
//		//	for k, _ := range entitiesByKey {
//		//		if k.Kind == key.Kind() && k.IntID > mockKey.IntID {
//		//			mockKey.IntID = k.IntID + 1
//		//		}
//		//	}
//		//}
//		//
//		//var p []datastore.Property
//		//var err error
//		//if e, ok := val.(datastore.PropertyLoadSaver); ok {
//		//	if p, err = e.Save(); err != nil {
//		//		return key, err
//		//	}
//		//} else {
//		//	if p, err = datastore.SaveStruct(val); err != nil {
//		//		return key, err
//		//	}
//		//}
//		//entitiesByKey[mockKey] = p
//		//return NewKey(c, mockKey.Kind, mockKey.StrID, mockKey.IntID, nil), nil
//	}
//
//	PutMulti = func(c context.Context, keys []*datastore.Key, vals any) ([]*datastore.Key, error) {
//		entityHolders := vals.([]dal.Record)
//		var err error
//		var errs []error
//		for i, key := range keys {
//			if key, err = Put(c, key, entityHolders[i]); err != nil {
//				errs = append(errs, err)
//			}
//			keys[i] = key
//		}
//		if len(errs) > 0 {
//			return keys, appengine.MultiError(errs)
//		}
//		return keys, nil
//	}
//}

//func onSave(entityHolder dal.Record) (dal.Record, error) {
//	return entityHolder, nil
//}
//
//func onLoad(entityHolder dal.Record) (dal.Record, error) {
//	return entityHolder, nil
//}
