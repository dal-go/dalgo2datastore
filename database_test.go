package dalgo2datastore

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewDatabase(t *testing.T) {
	v, err := NewDatabase(context.Background(), "sneat-team")
	if err != nil {
		t.Fatalf("NewDatabase() failed: %v", err)
	}
	//if v == nil {
	//	t.Fatalf("v == nil")
	//}
	switch v := v.(type) {
	case database: // OK
		assert.NotNilf(t, v.client, "database.client == nil")
	default:
		t.Errorf("unexpected DB type: %T", v)
	}
}

//func TestDatabase_RunInTransaction(t *testing.T) {
//	dbInstance := database{}
//	i, j := 0, 0
//
//	var xg bool
//
//	RunInTransaction = func(c context.Context, tx transaction, f func(tc context.Context) error) error {
//		assert.Equal(t, xg, tx.datastoreTxOptions.XG, "XG")
//		j++
//		return f(c)
//	}
//
//	t.Run("xg=true", func(t *testing.T) {
//		xg = true
//		err := dbInstance.RunReadonlyTransaction(context.Background(), func(c context.Context, tx dal.ReadTransaction) error {
//			i++
//			return nil
//		}, dal.TxWithCrossGroup())
//
//		if err != nil {
//			t.Errorf("Got unexpected error: %v", err)
//		}
//
//		if i != 1 {
//			t.Errorf("Expected 1 exection, got: %d", i)
//		}
//		if j != 1 {
//			t.Errorf("Expected 1 exection, got: %d", i)
//		}
//	})
//
//	t.Run("xg=false", func(t *testing.T) {
//		i, j = 0, 0
//		xg = false
//		err := dbInstance.RunReadonlyTransaction(context.Background(), func(c context.Context, tx dal.ReadTransaction) error {
//			i++
//			return errors.New("Test1")
//		})
//
//		if err == nil {
//			t.Error("Expected error, got nil")
//		} else if err.Error() != "Test1" {
//			t.Errorf("Got unexpected error: %v", err)
//		}
//
//		if i != 1 {
//			t.Errorf("Expected 1 exection, got: %d", i)
//		}
//		if j != 1 {
//			t.Errorf("Expected 1 exection, got: %d", i)
//		}
//	})
//
//}
