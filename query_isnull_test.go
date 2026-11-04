package dalgo2datastore

import (
	"errors"
	"reflect"
	"testing"

	"cloud.google.com/go/datastore"
	"github.com/dal-go/dalgo/dal"
)

func Test_applyWhere_isNull(t *testing.T) {
	base := datastore.NewQuery("K")
	field := dal.NewFieldRef("", "f")
	notNull := dal.NewIsNotNullCondition(field)

	t.Run("is_not_null_top_level", func(t *testing.T) {
		actual, err := applyWhere(notNull, base)
		if err != nil {
			t.Fatal(err)
		}
		if want := base.FilterField("f", "!=", nil); !reflect.DeepEqual(actual, want) {
			t.Fatalf("got %+v want %+v", actual, want)
		}
	})

	t.Run("is_not_null_in_and_group", func(t *testing.T) {
		cond := dal.NewGroupCondition(dal.And, dal.WhereField("a", dal.Equal, 1), dal.NewIsNotNullCondition(dal.NewFieldRef("", "b")))
		actual, err := applyWhere(cond, base)
		if err != nil {
			t.Fatal(err)
		}
		if want := base.FilterField("a", "=", 1).FilterField("b", "!=", nil); !reflect.DeepEqual(actual, want) {
			t.Fatalf("got %+v want %+v", actual, want)
		}
	})

	for _, tt := range []struct {
		name string
		cond dal.Condition
	}{
		{"is_null_rejected", dal.NewIsNullCondition(field)},
		{"is_null_in_group_rejected", dal.NewGroupCondition(dal.And, dal.NewIsNullCondition(field))},
		{"non_field_operand_rejected", dal.NewIsNotNullCondition(dal.Constant{Value: 1})},
		{"non_field_operand_in_group_rejected", dal.NewGroupCondition(dal.And, dal.NewIsNotNullCondition(dal.Constant{Value: 1}))},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := applyWhere(tt.cond, base); !errors.Is(err, dal.ErrNotSupported) {
				t.Fatalf("expected ErrNotSupported, got %v", err)
			}
		})
	}
}
