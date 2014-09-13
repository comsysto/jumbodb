package org.jumbodb.database.service.query.sql;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import org.jumbodb.common.query.JsonQuery;
import org.jumbodb.common.query.QueryOperation;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Carsten on 12.09.2014.
 */
public class WhereVisitor extends ExpressionVisitorAdapter {
    private QueryOperation operation;
    private Column column;
    private Object value;
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
        operation = QueryOperation.EQ;
        super.visit(expr);
        current = new JsonQuery(column.getFullyQualifiedName(), operation, value);
    }


    @Override
    public void visit(Column column) {
        System.out.print("col  " + column.getFullyQualifiedName());
        this.column = column;
        super.visit(column);
    }

    @Override
    public void visit(LikeExpression expr) {
        operation = QueryOperation.EQ;
        super.visit(expr);
        current = new JsonQuery(column.getFullyQualifiedName(), operation, value);
    }

    @Override
    public void visit(StringValue value) {
        this.value = value.getValue();
    }

    @Override
    public void visit(GreaterThan expr) {
        // CARSTEN aufpassen, spaltenname kann auch rechts stehen
        this.operation = QueryOperation.GT;
        super.visit(expr);
    }

    @Override
    public void visit(GreaterThanEquals expr) {
        throw new IllegalArgumentException("not supported");
    }

    @Override
    public void visit(DoubleValue value) {
        this.value = value.getValue();
    }

    @Override
    public void visit(LongValue value) {
        this.value = value.getValue();
    }



    @Override
    public void visit(DateValue value) {
        // CARSTEN date sauber implementieren
        this.value = value.getValue();
    }

    @Override
    public void visit(IsNullExpression expr) {
        throw new IllegalArgumentException("not supported");
    }

    @Override
    public void visit(NotEqualsTo expr) {
        super.visit(expr);
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
