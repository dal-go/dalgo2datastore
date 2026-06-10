package dalgo2datastore

import (
	"errors"
	"reflect"
	"testing"

	"github.com/dal-go/dalgo/dal"
	"github.com/stretchr/testify/assert"
)

func TestDalQuery2datastoreQuery_Where(t *testing.T) {
	newQB := func() dal.IQueryBuilder {
		return dal.From(dal.NewRootCollectionRef("Kind1", "")).NewQuery()
	}

	t.Run("where_field_equal", func(t *testing.T) {
		query := newQB().WhereField("field1", dal.Equal, "value1").SelectKeysOnly(reflect.String)
		q, err := dalQuery2datastoreQuery(query)
		assert.NoError(t, err)
		assert.NotNil(t, q)
	})

	t.Run("where_field_in_array", func(t *testing.T) {
		// dal.WhereArrayContainsAny: field IN array => Datastore "in" filter
		query := newQB().WhereArrayContainsAny("tags", []string{"a", "b"}).SelectKeysOnly(reflect.String)
		q, err := dalQuery2datastoreQuery(query)
		assert.NoError(t, err)
		assert.NotNil(t, q)
	})

	t.Run("where_array_contains", func(t *testing.T) {
		// dal.WhereArrayContains: constant IN field => Datastore "=" filter on array property
		query := newQB().WhereArrayContains("tags", "important").SelectKeysOnly(reflect.String)
		q, err := dalQuery2datastoreQuery(query)
		assert.NoError(t, err)
		assert.NotNil(t, q)
	})

	t.Run("unsupported_operator_for_array_right_operand", func(t *testing.T) {
		condition := dal.Comparison{
			Left:     dal.NewFieldRef("tags", ""),
			Operator: dal.Equal,
			Right:    dal.NewArray([]string{"a"}),
		}
		query := newQB().Where(condition).SelectKeysOnly(reflect.String)
		_, err := dalQuery2datastoreQuery(query)
		assert.True(t, errors.Is(err, dal.ErrNotSupported))
	})

	t.Run("unsupported_operator_for_constant_left_operand", func(t *testing.T) {
		condition := dal.Comparison{
			Left:     dal.Constant{Value: "v"},
			Operator: dal.Equal,
			Right:    dal.NewFieldRef("field1", ""),
		}
		query := newQB().Where(condition).SelectKeysOnly(reflect.String)
		_, err := dalQuery2datastoreQuery(query)
		assert.True(t, errors.Is(err, dal.ErrNotSupported))
	})
}
