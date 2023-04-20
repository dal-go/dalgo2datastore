package dalgo2datastore

import (
	"cloud.google.com/go/datastore"
	"fmt"
	"github.com/dal-go/dalgo/dal"
)

func dalQuery2datastoreQuery(query dal.Query) (q *datastore.Query, err error) {
	q = datastore.NewQuery(query.From().Name)
	if limit := query.Limit(); limit > 0 {
		q = q.Limit(limit)
	}
	if offset := query.Offset(); offset > 0 {
		q.Offset(offset)
	}
	if where := query.Where(); where != nil {

		var applyComparison = func(comparison dal.Comparison) error {
			switch left := comparison.Left.(type) {
			case dal.FieldRef:
				switch right := comparison.Right.(type) {
				case dal.Constant:
					q = q.FilterField(left.Name, string(comparison.Operator), right.Value)
				default:
					return fmt.Errorf("only FieldRef are supported as left operand, got: %T", right)
				}
			default:
				return fmt.Errorf("only FieldRef are supported as left operand, got: %T", left)
			}
			return nil
		}

		switch cond := where.(type) {
		case dal.GroupCondition:
			if cond.Operator() != dal.And {
				return q, fmt.Errorf("only AND operator is supported in group condition, got: %v", cond.Operator())
			}
			for _, c := range cond.Conditions() {
				switch c := c.(type) {
				case dal.Comparison:
					if err = applyComparison(c); err != nil {
						return q, err
					}
				default:
					return q, fmt.Errorf("only comparisons are supported in group condition, got: %T", c)
				}
			}
		case dal.Comparison:
			if err = applyComparison(cond); err != nil {
				return q, nil
			}
		default:
			return q, fmt.Errorf("only comparison or group conditions are supported at root level of where clause, got: %T", cond)
		}

	}
	return q, nil
}
