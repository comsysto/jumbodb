package org.jumbodb.database.service.query.sql;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import org.jumbodb.common.query.JsonQuery;
import org.jumbodb.common.query.QueryOperation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Carsten on 12.09.2014.
 */
public class WhereVisitor extends ExpressionVisitorAdapter {
    private List<Object> expressions = new ArrayList<Object>(2);
    private List<JsonQuery> ors = new LinkedList<JsonQuery>();
    private List<JsonQuery> ands = new LinkedList<JsonQuery>();
    private JsonQuery current;


    public List<JsonQuery> getOrs() {
        if(current != null)  {
            return Arrays.asList(current);
        } else if(!ands.isEmpty()) {
            JsonQuery last = null;
            for (JsonQuery and : ands) {
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
            ands.add(new JsonQuery(leftWhereVisitor.ors));
        }
        if (!rightWhereVisitor.ors.isEmpty()) {
            ands.add(new JsonQuery(rightWhereVisitor.ors));
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
        List<JsonQuery> leftJsonQueries = leftWhereVisitor.ors;
        List<JsonQuery> rightJsonQueries = rightWhereVisitor.ors;
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
            JsonQuery last = null;
            for (JsonQuery and : leftWhereVisitor.ands) {
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
        idenicalEvaluation(QueryOperation.EQ);
    }

    private void idenicalEvaluation(QueryOperation eq) {
        if(hasTwoColumns()) {
            throw new IllegalArgumentException("comparision on two columns are not supported yet");
        }
        else if(hasOneColumns()) {
            Column firstColumn = getFirstColumn();
            current = new JsonQuery(firstColumn.getFullyQualifiedName(), eq, getFirstValue());
        }
        else {
            throw new IllegalArgumentException("minimum one field is required for query criteria");
        }
    }

    private boolean hasOneColumns() {
        return hasColumnLeft()
                || hasColumnRight();
    }

    private boolean hasTwoColumns() {
        return hasColumnLeft()
                && hasColumnRight();
    }

    private Column getFirstColumn() {
        if(hasColumnLeft()) {
            return getColumnLeft();
        }
        return getColumnRight();
    }

    private Object getFirstValue() {
        if(hasColumnLeft()) {
            return getValueRight();
        }
        return getValueLeft();
    }

    private Column getColumnRight() {
        return (Column)expressions.get(1);
    }

    private Column getColumnLeft() {
        return (Column)expressions.get(0);
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
        expressions.add(column);
    }

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
        greaterLessEvaluation(QueryOperation.GT, QueryOperation.LT);
    }

    @Override
    public void visit(GreaterThanEquals expr) {
        throw new IllegalArgumentException("not supported");
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
        idenicalEvaluation(QueryOperation.NE);
    }

    @Override
    public void visit(InExpression expr) {
        // CARSTEN implement later
        throw new IllegalArgumentException("not supported");
//        super.visit(expr);
    }

    @Override
    public void visit(Between expr) {
        // CARSTEN implement later
        throw new IllegalArgumentException("not supported");
    }

    @Override
    public void visit(NullValue value) {
        super.visit(value);
    }

    @Override
    public void visit(MinorThan expr) {
        super.visit(expr);
        greaterLessEvaluation(QueryOperation.LT, QueryOperation.GT);
    }

    private void greaterLessEvaluation(QueryOperation leftOp, QueryOperation rightOp) {
        if(hasTwoColumns()) {
            throw new IllegalArgumentException("comparision on two columns are not supported yet");
        }
        else if(hasOneColumns()) {
            if(hasColumnLeft()) {
                Column firstColumn = getColumnLeft();
                current = new JsonQuery(firstColumn.getFullyQualifiedName(), leftOp, getValueRight());
            }
            else {
                Column firstColumn = getColumnRight();
                current = new JsonQuery(firstColumn.getFullyQualifiedName(), rightOp, getValueLeft());
            }
        }
        else {
            throw new IllegalArgumentException("minimum one field is required for query criteria");
        }
    }

    @Override
    public void visit(MinorThanEquals expr) {
        super.visit(expr);
    }

    @Override
    public void visit(ExistsExpression expr) {
        // CARSTEN implement later
        throw new IllegalArgumentException("not supported");
    }


    @Override
    public void visit(Function function) {
        // CARSTEN implement geo spatial functions
        System.out.println("function name " + function.getName());
        System.out.println("function params 1 " + function.getParameters().getExpressions().get(0));
        System.out.println("function params 2 " + function.getParameters().getExpressions().get(1));
    }
}
