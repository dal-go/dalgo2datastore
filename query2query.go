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
		if q, err = applyWhere(query.Where(), q); err != nil {
			return q, err
		}
	}
	if orderBy := query.OrderBy(); len(orderBy) > 0 {
		if q, err = applyOrderBy(orderBy, q); err != nil {
			return q, err
		}
	}
	return q, nil
}

func applyOrderBy(orderBy []dal.OrderExpression, q *datastore.Query) (*datastore.Query, error) {
	for _, o := range orderBy {
		expression := o.String()
		if o.Descending() {
			expression = "-" + expression
		}
		q = q.Order(expression)
	}
	return q, nil
}

func applyWhere(where dal.Condition, q *datastore.Query) (*datastore.Query, error) {
	if where == nil {
		return q, nil
	}
	var applyComparison = func(comparison dal.Comparison) (*datastore.Query, error) {
		switch left := comparison.Left.(type) {
		case dal.FieldRef:
			switch right := comparison.Right.(type) {
			case dal.Constant:
				var operator string
				switch comparison.Operator {
				case dal.Equal:
					operator = "="
				default:
					operator = string(comparison.Operator)
				}
				q = q.FilterField(left.Name, operator, right.Value)
			default:
				return q, fmt.Errorf("only FieldRef are supported as left operand, got: %T", right)
			}
		default:
			return q, fmt.Errorf("only FieldRef are supported as left operand, got: %T", left)
		}
		return q, nil
	}

	switch cond := where.(type) {
	case dal.GroupCondition:
		if cond.Operator() != dal.And {
			return q, fmt.Errorf("only AND operator is supported in group condition, got: %v", cond.Operator())
		}
		for _, c := range cond.Conditions() {
			switch c := c.(type) {
			case dal.Comparison:
				var err error
				if q, err = applyComparison(c); err != nil {
					return q, err
				}
			default:
				return q, fmt.Errorf("only comparisons are supported in group condition, got: %T", c)
			}
		}
		return q, nil
	case dal.Comparison:
		return applyComparison(cond)
	default:
		return q, fmt.Errorf("only comparison or group conditions are supported at root level of where clause, got: %T", cond)
	}
}
