package org.jumbodb.database.service.query.sql;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;
import org.jumbodb.common.query.DataQuery;
import org.jumbodb.common.query.FieldType;
import org.jumbodb.common.query.QueryOperation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Carsten on 12.09.2014.
 */
public class WhereVisitor extends ExpressionVisitorAdapter {
    private List<Object> expressions = new ArrayList<Object>();
    private List<DataQuery> ors = new LinkedList<DataQuery>();
    private List<DataQuery> ands = new LinkedList<DataQuery>();
    private DataQuery current;


    public List<DataQuery> getOrs() {
        if(current != null)  {
            return Arrays.asList(current);
        } else if(!ands.isEmpty()) {
            DataQuery last = null;
            for (DataQuery and : ands) {
                if (last != null) {
                    and.setAnd(last);
                }
                last = and;
            }
            if (last != null) {
                ors.add(last);
            }
        }
        return ors;
    }

    @Override
    public void visit(AndExpression expr) {
        WhereVisitor leftWhereVisitor = new WhereVisitor();
        expr.getLeftExpression().accept(leftWhereVisitor);
        WhereVisitor rightWhereVisitor = new WhereVisitor();
        expr.getRightExpression().accept(rightWhereVisitor);
        if (rightWhereVisitor.current != null) {
            ands.add(rightWhereVisitor.current);
        }
        if (leftWhereVisitor.current != null) {
            ands.add(leftWhereVisitor.current);
        }

        if (!leftWhereVisitor.ors.isEmpty()) {
            ands.add(new DataQuery(leftWhereVisitor.ors));
        }
        if (!rightWhereVisitor.ors.isEmpty()) {
            ands.add(new DataQuery(rightWhereVisitor.ors));
        }
        ands.addAll(leftWhereVisitor.ands);
        ands.addAll(rightWhereVisitor.ands);
    }

    @Override
    public void visit(OrExpression expr) {
        WhereVisitor leftWhereVisitor = new WhereVisitor();
        expr.getLeftExpression().accept(leftWhereVisitor);
        WhereVisitor rightWhereVisitor = new WhereVisitor();
        expr.getRightExpression().accept(rightWhereVisitor);
        List<DataQuery> leftJsonQueries = leftWhereVisitor.ors;
        List<DataQuery> rightJsonQueries = rightWhereVisitor.ors;
        ors.addAll(leftJsonQueries);
        ors.addAll(rightJsonQueries);
        if (leftWhereVisitor.current != null) {
            ors.add(leftWhereVisitor.current);
        }
        if (rightWhereVisitor.current != null) {
            ors.add(rightWhereVisitor.current);
        }
        concatAnd(leftWhereVisitor);
        concatAnd(rightWhereVisitor);
    }

    private void concatAnd(WhereVisitor leftWhereVisitor) {
        if (!leftWhereVisitor.ands.isEmpty()) {
            DataQuery last = null;
            for (DataQuery and : leftWhereVisitor.ands) {
                if (last != null) {
                    and.setAnd(last);
                }
                last = and;
            }
            if (last != null) {
                ors.add(last);
            }
        }
    }

    @Override
    public void visit(EqualsTo expr) {
        super.visit(expr);
        boolEvaluation(QueryOperation.EQ);
    }

    private void boolEvaluation(QueryOperation eq) {
        if(expressions.size() == 2) {
            current = createDataQuery(eq);
        }
        else {
            throw new IllegalArgumentException("minimum one field is required for query criteria");
        }
    }

    private DataQuery createDataQuery(QueryOperation operation) {
        FieldType leftType = getFieldTypeLeft();
        FieldType rightType = getFieldTypeRight();
        Object left = leftType == FieldType.FIELD ? getColumnLeft() : getValueLeft();
        Object right = rightType == FieldType.FIELD ? getColumnRight() : getValueRight();
        return new DataQuery(left, leftType, operation, right, rightType);
    }

    private String getColumnRight() {
        return ((Column)expressions.get(1)).getFullyQualifiedName();
    }

    private String getColumnLeft() {
        return ((Column)expressions.get(0)).getFullyQualifiedName();
    }

    private FieldType getFieldTypeLeft() {
        return expressions.get(0) instanceof Column ? FieldType.FIELD : FieldType.VALUE;
    }

    private FieldType getFieldTypeRight() {
        return expressions.get(1) instanceof Column ? FieldType.FIELD : FieldType.VALUE;
    }

    private Object getValueRight() {
        return expressions.get(1);
    }

    private Object getValueLeft() {
        return expressions.get(0);
    }

    private boolean hasColumnRight() {
        return expressions.get(1) instanceof Column;
    }

    private boolean hasColumnLeft() {
        return expressions.get(0) instanceof Column;
    }


    @Override
    public void visit(Column column) {
        // workaround, true is a comparision and not a field!
        if("true".equals(column.getFullyQualifiedName())) {
            expressions.add(true);
        } else {
            expressions.add(column);
        }
    }

    // CARSTEN implement ALL and ANY and SOME in where clause, SOME and ANY do the same http://www.oracle-base.com/articles/misc/all-any-some-comparison-conditions-in-sql.php

    @Override
    public void visit(LikeExpression expr) {
        // CARSTEN implement with contains
//        operation = QueryOperation.EQ;
//        super.visit(expr);
//        current = new JsonQuery(column.getFullyQualifiedName(), operation, value);
    }

    @Override
    public void visit(StringValue value) {
        expressions.add(value.getValue());
    }

    @Override
    public void visit(GreaterThan expr) {
        super.visit(expr);
        boolEvaluation(QueryOperation.GT);
    }

    @Override
    public void visit(GreaterThanEquals expr) {
        super.visit(expr);
        boolEvaluation(QueryOperation.GT_EQ);
    }

    @Override
    public void visit(DoubleValue value) {
        expressions.add(value.getValue());
    }

    @Override
    public void visit(LongValue value) {
        expressions.add(value.getValue());
    }



    @Override
    public void visit(DateValue value) {
        expressions.add(value.getValue());
        // CARSTEN date sauber implementieren
    }

    @Override
    public void visit(IsNullExpression expr) {
        throw new IllegalArgumentException("not supported");
    }

    @Override
    public void visit(NotEqualsTo expr) {
        super.visit(expr);
        boolEvaluation(QueryOperation.NE);
    }

    @Override
    public void visit(InExpression expr) {
        // CARSTEN implement later
        expr.getLeftExpression().accept(this);
//        expr.getLeftItemsList().accept(this); // causes null
        expr.getRightItemsList().accept(this);
        throw new IllegalArgumentException("not supported");
    }

    @Override
    public void visit(Between expr) {
        // CARSTEN implement later
        throw new IllegalArgumentException("not supported");
    }

    @Override
    public void visit(NullValue value) {
        super.visit(value);
        throw new IllegalArgumentException("not supported");
    }

    @Override
    public void visit(MinorThan expr) {
        super.visit(expr);
        boolEvaluation(QueryOperation.LT);
    }

    @Override
    public void visit(MinorThanEquals expr) {
        super.visit(expr);
        boolEvaluation(QueryOperation.LT_EQ);
    }

    @Override
    public void visit(ExistsExpression expr) {
        // CARSTEN implement later
        super.visit(expr);
        throw new IllegalArgumentException("not supported");
    }

    @Override
    public void visit(TimestampValue value) {
        super.visit(value);
        throw new IllegalArgumentException("not supported");
    }

    @Override
    public void visit(SubSelect subSelect) {
        super.visit(subSelect);
        throw new IllegalArgumentException("not supported");
    }

    @Override
    public void visit(Function function) {
        // CARSTEN implement geo spatial functions
        // CARSTEN implement idx functions idx('fieldName') or idx(fieldName)
        // CARSTEN implement restriced names functions field('delete') or field(delete)
        // CARSTEN implement dateField('fieldName', 'date format') or dateField('fieldName') using the default, for date itself use {ts ...}
        System.out.println("function name " + function.getName());
        System.out.println("function params 1 " + function.getParameters().getExpressions().get(0));
        System.out.println("function params 2 " + function.getParameters().getExpressions().get(1));
    }
}
