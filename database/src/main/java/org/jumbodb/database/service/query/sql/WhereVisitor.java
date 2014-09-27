package org.jumbodb.database.service.query.sql;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;
import org.jumbodb.common.query.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Carsten Hufe
 */
public class WhereVisitor extends ExpressionVisitorAdapter {
    private List<Object> expressions = new ArrayList<Object>();
    private List<DataQuery> dataOrs = new LinkedList<DataQuery>();
    private List<DataQuery> dataAnds = new LinkedList<DataQuery>();
    private List<IndexQuery> indexOrs = new LinkedList<IndexQuery>();
    private List<IndexQuery> indexAnds = new LinkedList<IndexQuery>();
    private DataQuery currentData;
    private IndexQuery currentIndex;
    private boolean index = false;
    private HintType hintType = HintType.NONE;

    public List<IndexQuery> getIndexOrs() {
        if(currentIndex != null)  {
            return Arrays.asList(currentIndex);
        } else if(!indexAnds.isEmpty()) {
            IndexQuery last = null;
            for (IndexQuery and : indexAnds) {
                if (last != null) {
                    and.setAndIndex(last);
                }
                last = and;
            }
            if (last != null) {
                indexOrs.add(last);
            }
        }
        return indexOrs;
    }

    public List<DataQuery> getDataOrs() {
        if(currentData != null)  {
            return Arrays.asList(currentData);
        } else if(!dataAnds.isEmpty()) {
            DataQuery last = null;
            for (DataQuery and : dataAnds) {
                if (last != null) {
                    and.setAnd(last);
                }
                last = and;
            }
            if (last != null) {
                dataOrs.add(last);
            }
        }
        return dataOrs;
    }

    @Override
    public void visit(AndExpression expr) {
        WhereVisitor leftWhereVisitor = new WhereVisitor();
        expr.getLeftExpression().accept(leftWhereVisitor);
        WhereVisitor rightWhereVisitor = new WhereVisitor();
        expr.getRightExpression().accept(rightWhereVisitor);
        if (rightWhereVisitor.currentData != null) {
            dataAnds.add(rightWhereVisitor.currentData);
        }
        if (leftWhereVisitor.currentData != null) {
            dataAnds.add(leftWhereVisitor.currentData);
        }

        if (!leftWhereVisitor.dataOrs.isEmpty()) {
            dataAnds.add(new DataQuery(leftWhereVisitor.dataOrs));
        }
        if (!rightWhereVisitor.dataOrs.isEmpty()) {
            dataAnds.add(new DataQuery(rightWhereVisitor.dataOrs));
        }
        dataAnds.addAll(leftWhereVisitor.dataAnds);
        dataAnds.addAll(rightWhereVisitor.dataAnds);
    }

    @Override
    public void visit(OrExpression expr) {
        WhereVisitor leftWhereVisitor = new WhereVisitor();
        expr.getLeftExpression().accept(leftWhereVisitor);
        WhereVisitor rightWhereVisitor = new WhereVisitor();
        expr.getRightExpression().accept(rightWhereVisitor);
        List<DataQuery> leftJsonQueries = leftWhereVisitor.dataOrs;
        List<DataQuery> rightJsonQueries = rightWhereVisitor.dataOrs;
        dataOrs.addAll(leftJsonQueries);
        dataOrs.addAll(rightJsonQueries);
        if (leftWhereVisitor.currentData != null) {
            dataOrs.add(leftWhereVisitor.currentData);
        }
        if (rightWhereVisitor.currentData != null) {
            dataOrs.add(rightWhereVisitor.currentData);
        }
        concatAnd(leftWhereVisitor);
        concatAnd(rightWhereVisitor);
    }

    private void concatAnd(WhereVisitor leftWhereVisitor) {
        if (!leftWhereVisitor.dataAnds.isEmpty()) {
            DataQuery last = null;
            for (DataQuery and : leftWhereVisitor.dataAnds) {
                if (last != null) {
                    and.setAnd(last);
                }
                last = and;
            }
            if (last != null) {
                dataOrs.add(last);
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
            if(index) {
                currentIndex = createIndexQuery(eq);
            } else {
                currentData = createDataQuery(eq);
            }
        }
        else {
            throw new SQLParseException("Minimum one field is required for query criteria.");
        }
    }

    private IndexQuery createIndexQuery(QueryOperation eq) {
        validNotTwoFieldsForIndex();
        boolean isFieldRight = getFieldTypeRight() == FieldType.FIELD;
        QueryOperation op = eq;
        String columnName;
        Object value;
        if(isFieldRight) {
            columnName = getColumnRight();
            value = getValueLeft();
            switch(eq) {
                case GT:
                    op = QueryOperation.LT;
                    break;
                case GT_EQ:
                    op = QueryOperation.LT_EQ;
                    break;
                case LT:
                    op = QueryOperation.GT;
                    break;
                case LT_EQ:
                    op = QueryOperation.GT_EQ;
                    break;
            }
        }
        else {
            columnName = getColumnLeft();
            value = getValueRight();
        }
        return new IndexQuery(columnName, op, value);
    }

    private void validNotTwoFieldsForIndex() {
        int fields = 0;
        for (Object expression : expressions) {
            if(expression instanceof Column) {
                fields++;
            }
        }
        if(fields > 1) {
            throw new SQLParseException("Indexes cannot be compared with other fields.");
        }
    }

    private DataQuery createDataQuery(QueryOperation operation) {
        FieldType leftType = getFieldTypeLeft();
        FieldType rightType = getFieldTypeRight();
        Object left = leftType == FieldType.FIELD ? getColumnLeft() : getValueLeft();
        Object right = rightType == FieldType.FIELD ? getColumnRight() : getValueRight();
        return new DataQuery(left, leftType, operation, right, rightType, hintType);
    }

    private String getColumnRight() {
        return ((Column)expressions.get(1)).getFullyQualifiedName();
    }

    private String getLeftColumnOrColumnAsValue() {
        if(getFieldTypeLeft() == FieldType.FIELD) {
            return getColumnLeft();
        }
        return getValueLeft().toString();
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

    @Override
    public void visit(LikeExpression expr) {
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
        hintType = HintType.DATE;
        expressions.add(value.getValue().getTime());
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
        expr.getLeftExpression().accept(this);
//        expr.getLeftItemsList().accept(this); // causes null
        expr.getRightItemsList().accept(this);
        throw new IllegalArgumentException("not supported");
    }

    @Override
    public void visit(Between expr) {
        super.visit(expr);

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
        super.visit(expr);
        if(index) {
            throw new SQLParseException("EXISTS is not allowed with indexes.");
        }
        Object left = getLeftColumnOrColumnAsValue();
        currentData = new DataQuery(left, FieldType.FIELD, expr.isNot() ? QueryOperation.NOT_EXISTS : QueryOperation.EXISTS);
    }

    @Override
    public void visit(TimestampValue value) {
        hintType = HintType.DATE;
        expressions.add(value.getValue().getTime());
    }

    @Override
    public void visit(SubSelect subSelect) {
        super.visit(subSelect);
        throw new SQLParseException("SUBSELECTS are currently not supported.");
    }

    @Override
    public void visit(TimeValue value) {
        super.visit(value);
    }

    @Override
    public void visit(AnyComparisonExpression expr) {
        super.visit(expr);
        throw new SQLParseException("ANY is not supported.");
    }

    @Override
    public void visit(AllComparisonExpression expr) {
        super.visit(expr);
        throw new SQLParseException("ALL is not supported.");
    }

    @Override
    public void visit(Concat expr) {
        super.visit(expr);
        throw new SQLParseException("CONCAT is not supported.");
    }

    @Override
    public void visit(Matches expr) {
        super.visit(expr);
        throw new SQLParseException("MATCHES is not supported.");
    }

    @Override
    public void visit(Function function) {
        super.visit(function);
        if("IDX".equalsIgnoreCase(function.getName())) {
            handleIdxFunction(function);
        } else {
            throw new SQLParseException("The function '" + function.getName().toUpperCase() + "' is not supported.");
        }
    }

    private void handleIdxFunction(Function function) {
        ExpressionList parameters = function.getParameters();
        if(parameters.getExpressions().size() != 1) {
            throw new SQLParseException("IDX function requires exactly one parameter.");
        }
        index = true;
        Expression expression = parameters.getExpressions().get(0);
        Column column = getColumn(expression);
        expressions.add(column);
    }

    private Column getColumn(Expression expression) {
        if(expression instanceof Column) {
            return ((Column)expression);
        }
        else if(expression instanceof StringValue) {
            return new Column(((StringValue)expression).getValue());
        }
        throw new SQLParseException("Column names must be defined as string with quotes or without.");
    }
}
