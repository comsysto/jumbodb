package org.jumbodb.database.service.query.sql;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;
import org.apache.commons.lang.StringUtils;
import org.jumbodb.common.query.*;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
        if (currentIndex != null) {
            return Arrays.asList(currentIndex);
        } else if (!indexAnds.isEmpty()) {
            IndexQuery lastIndex = null;
            DataQuery lastData = null;
            for (DataQuery and : dataAnds) {
                if (lastData != null) {
                    and.setDataAnd(lastData);
                }
                lastData = and;
            }
            for (IndexQuery and : indexAnds) {
                if (lastIndex != null) {
                    and.setIndexAnd(lastIndex);
                } else if(lastData != null) {
                    and.setDataAnd(lastData);
                }
                lastIndex = and;
            }
            if (lastIndex != null) {
                indexOrs.add(lastIndex);
            }
        } else if (!dataAnds.isEmpty()) {
            DataQuery lastData = null;
            for (DataQuery and : dataAnds) {
                if (lastData != null) {
                    and.setDataAnd(lastData);
                }
                lastData = and;
            }
            for (IndexQuery ors : indexOrs) {
                ors.setDataAnd(lastData);
            }
        }
        return indexOrs;
    }

    public List<DataQuery> getDataOrs() {
        if (currentData != null) {
            return Arrays.asList(currentData);
        } else if (!dataAnds.isEmpty() && indexAnds.isEmpty()) {
            if(!indexOrs.isEmpty()) {
                return Collections.emptyList();
            }
            DataQuery last = null;
            for (DataQuery and : dataAnds) {
                if (last != null) {
                    and.setDataAnd(last);
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

        if (rightWhereVisitor.currentIndex != null) {
            indexAnds.add(rightWhereVisitor.currentIndex);
        }
        if (leftWhereVisitor.currentIndex != null) {
            indexAnds.add(leftWhereVisitor.currentIndex);
        }

        if (!leftWhereVisitor.dataOrs.isEmpty()) {
            dataAnds.add(new DataQuery(leftWhereVisitor.dataOrs));
        }
        if (!rightWhereVisitor.dataOrs.isEmpty()) {
            dataAnds.add(new DataQuery(rightWhereVisitor.dataOrs));
        }
//        if (!leftWhereVisitor.indexOrs.isEmpty()) {
//           throw new SQLParseException("not supported!");
////            dataAnds.add(new DataQuery(leftWhereVisitor.dataOrs));
//        }
//        if (!rightWhereVisitor.indexOrs.isEmpty()) {
//            if(!dataAnds.isEmpty()) {
//
//            }
//            else {
//                throw new SQLParseException("You hit an unsupported query.");
//            }
//
////            dataAnds.add(new DataQuery(rightWhereVisitor.dataOrs));
//        }
        dataAnds.addAll(leftWhereVisitor.dataAnds);
        dataAnds.addAll(rightWhereVisitor.dataAnds);
        indexAnds.addAll(leftWhereVisitor.indexAnds);
        indexAnds.addAll(rightWhereVisitor.indexAnds);
        indexOrs.addAll(leftWhereVisitor.indexOrs);
        indexOrs.addAll(rightWhereVisitor.indexOrs);

    }

    @Override
    public void visit(OrExpression expr) {
        WhereVisitor leftWhereVisitor = new WhereVisitor();
        expr.getLeftExpression().accept(leftWhereVisitor);
        WhereVisitor rightWhereVisitor = new WhereVisitor();
        expr.getRightExpression().accept(rightWhereVisitor);
        dataOrs.addAll(leftWhereVisitor.dataOrs);
        dataOrs.addAll(rightWhereVisitor.dataOrs);
        indexOrs.addAll(leftWhereVisitor.indexOrs);
        indexOrs.addAll(rightWhereVisitor.indexOrs);
        if (leftWhereVisitor.currentData != null) {
            dataOrs.add(leftWhereVisitor.currentData);
        }
        if (rightWhereVisitor.currentData != null) {
            dataOrs.add(rightWhereVisitor.currentData);
        }
        if (leftWhereVisitor.currentIndex != null) {
            indexOrs.add(leftWhereVisitor.currentIndex);
        }
        if (rightWhereVisitor.currentIndex != null) {
            indexOrs.add(rightWhereVisitor.currentIndex);
        }
        concatAnd(leftWhereVisitor);
        concatAnd(rightWhereVisitor);
    }

    private void concatAnd(WhereVisitor leftWhereVisitor) {
        if (!leftWhereVisitor.dataAnds.isEmpty()
                && !leftWhereVisitor.indexAnds.isEmpty()) {
            DataQuery lastData = null;
            IndexQuery lastIndex = null;
            for (DataQuery and : leftWhereVisitor.dataAnds) {
                if (lastData != null) {
                    and.setDataAnd(lastData);
                }
                lastData = and;
            }
            for (IndexQuery and : leftWhereVisitor.indexAnds) {
                if (lastIndex != null) {
                    and.setIndexAnd(lastIndex);
                } else if (lastData != null) {
                    and.setDataAnd(lastData);
                }
                lastIndex = and;
            }

            if (lastIndex != null) {
                indexOrs.add(lastIndex);
            }

        } else if (!leftWhereVisitor.dataAnds.isEmpty()) {
            DataQuery last = null;
            for (DataQuery and : leftWhereVisitor.dataAnds) {
                if (last != null) {
                    and.setDataAnd(last);
                }
                last = and;
            }
            if (last != null) {
                dataOrs.add(last);
            }
        } else if (!leftWhereVisitor.indexAnds.isEmpty()) {
            IndexQuery last = null;
            for (IndexQuery and : leftWhereVisitor.indexAnds) {
                if (last != null) {
                    and.setIndexAnd(last);
                }
                last = and;
            }
            if (last != null) {
                indexOrs.add(last);
            }
        }
    }

    @Override
    public void visit(EqualsTo expr) {
        super.visit(expr);
        boolEvaluation(QueryOperation.EQ);
    }

    private void singleEvaluation(QueryOperation operation) {
        if (expressions.size() == 1) {
            if (index) {
                throw new SQLParseException(operation.getOperation() + " is not allowed for indexes.");
            } else {
                FieldType leftType = getFieldTypeLeft();
                if (leftType != FieldType.FIELD) {
                    throw new SQLParseException(operation.getOperation() + " is only allowed on fields.");
                }
                currentData = new DataQuery(getColumnLeft(), leftType, operation, null, FieldType.NOT_SET, hintType);
            }
        } else {
            throw new SQLParseException("Minimum one field is required for query criteria.");
        }
    }

    private void boolEvaluation(QueryOperation eq) {
        if (expressions.size() == 2) {
            if (index) {
                currentIndex = createIndexQuery(eq);
            } else {
                currentData = createDataQuery(eq);
            }
        } else {
            throw new SQLParseException("Minimum one field is required for query criteria.");
        }
    }

    private IndexQuery createIndexQuery(QueryOperation eq) {
        validNotTwoFieldsForIndex();
        boolean isFieldRight = getFieldTypeRight() == FieldType.FIELD;
        QueryOperation op = eq;
        String columnName;
        Object value;
        if (isFieldRight) {
            columnName = getColumnRight();
            value = getValueLeft();
            switch (eq) {
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
        } else {
            columnName = getColumnLeft();
            value = getValueRight();
        }
        return new IndexQuery(columnName, op, value);
    }

    private void validNotTwoFieldsForIndex() {
        int fields = 0;
        for (Object expression : expressions) {
            if (expression instanceof Column) {
                fields++;
            }
        }
        if (fields > 1) {
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
        return ((Column) expressions.get(1)).getFullyQualifiedName();
    }

    private String getLeftColumnOrColumnAsValue() {
        if (getFieldTypeLeft() == FieldType.FIELD) {
            return getColumnLeft();
        }
        return getValueLeft().toString();
    }

    private String getColumnLeft() {
        return ((Column) expressions.get(0)).getFullyQualifiedName();
    }

    private FieldType getFieldTypeLeft() {
        return expressions.get(0) instanceof Column ? FieldType.FIELD : FieldType.VALUE;
    }

    private FieldType getFieldTypeRight() {
        return expressions.get(1) instanceof Column ? FieldType.FIELD : FieldType.VALUE;
    }

    private Object getValueRight() {
        return getValueAtIndex(1);
    }

    private Object getValueAtIndex(final int index) {
        Object o = expressions.get(index);
        if (o instanceof Column) {
            return ((Column) o).getFullyQualifiedName();
        }
        else if (o instanceof StringValue) {
            return ((StringValue) o).getValue();
        }
        return o;
    }

    private Object getValueLeft() {
        return getValueAtIndex(0);
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
        if ("true".equals(column.getFullyQualifiedName())) {
            expressions.add(true);
        } else {
            expressions.add(column);
        }
    }

    @Override
    public void visit(LikeExpression expr) {
        super.visit(expr);
        boolEvaluation(expr.isNot() ? QueryOperation.NOT_LIKE : QueryOperation.LIKE);
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
        super.visit(expr);
        singleEvaluation(expr.isNot() ? QueryOperation.IS_NOT_NULL : QueryOperation.IS_NULL);
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
        String columnLeft = getColumnLeft();
        for (int i = 1; i < expressions.size(); i++) {
            if (index) {
                indexOrs.add(new IndexQuery(columnLeft, QueryOperation.EQ, getValueAtIndex(i)));
            } else {
                dataOrs.add(new DataQuery(columnLeft, FieldType.FIELD, QueryOperation.EQ, getValueAtIndex(i), FieldType.VALUE));
            }
        }
    }

    @Override
    public void visit(Between expr) {
        super.visit(expr);
        String columnLeft = getColumnLeft();
        Object val1 = getValueAtIndex(1);
        Object val2 = getValueAtIndex(2);
        if (index) {
            currentIndex = new IndexQuery(columnLeft, QueryOperation.BETWEEN, Arrays.asList(val1, val2));
        } else {
            currentData = new DataQuery(columnLeft, FieldType.FIELD, QueryOperation.BETWEEN, Arrays.asList(val1, val2), FieldType.VALUE);
        }
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
        if (index) {
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
        String name = function.getName();
        if ("IDX".equalsIgnoreCase(name)) {
            handleIdxFunction(function);
        } else if ("TO_DATE".equalsIgnoreCase(name)) {
            handleToDateFunction(function);
        } else if ("DATE_FIELD".equalsIgnoreCase(name)) {
            handleDateFieldFunction(function);
        } else if ("GEO_BOUNDARY_BOX".equalsIgnoreCase(name) || "GEO_BOUNDARY_BOX_IDX".equalsIgnoreCase(name)) {
            handleGeoBoundaryBoxFunction(function, "GEO_BOUNDARY_BOX_IDX".equalsIgnoreCase(name));
        } else if ("GEO_WITHIN_RANGE_METER".equalsIgnoreCase(name) || "GEO_WITHIN_RANGE_METER_IDX".equalsIgnoreCase(name)) {
            handleGeoWithinRangeMeterFunction(function, "GEO_WITHIN_RANGE_METER_IDX".equalsIgnoreCase(name));
        } else {
            throw new SQLParseException("The function '" + function.getName().toUpperCase() + "' is not supported.");
        }
    }

    private void handleGeoWithinRangeMeterFunction(Function function, boolean index) {
        ExpressionList parameters = function.getParameters();
        List<Expression> params = parameters.getExpressions();
        if(params.size() != 4) {
            throw new SQLParseException("GEO_WITHIN_RANGE_METER requires exactly 4 arguments!");
        }
        WhereVisitor v = new WhereVisitor();
        v.visit(parameters);
        List<Number> coords = new ArrayList<Number>();
        coords.add((Number) v.getValueAtIndex(1));
        coords.add((Number) v.getValueAtIndex(2));
        List<Object> values = new ArrayList<Object>();
        values.add(coords);
        values.add(v.getValueAtIndex(3));
        Object field = v.getValueAtIndex(0);
        if(index) {
            currentIndex = new IndexQuery(field.toString(), QueryOperation.GEO_WITHIN_RANGE_METER, values);
        }
        else {
            currentData = new DataQuery(field, FieldType.FIELD, QueryOperation.GEO_WITHIN_RANGE_METER,
              values, FieldType.VALUE);
        }
    }

    private void handleGeoBoundaryBoxFunction(Function function, boolean index) {
        ExpressionList parameters = function.getParameters();
        List<Expression> params = parameters.getExpressions();
        if(params.size() != 5) {
            throw new SQLParseException("GEO_BOUNDARY_BOX requires exactly 5 arguments!");
        }
        WhereVisitor v = new WhereVisitor();
        v.visit(parameters);
        List<Number> coords1 = new ArrayList<Number>();
        coords1.add((Number) v.getValueAtIndex(1));
        coords1.add((Number) v.getValueAtIndex(2));
        List<Number> coords2 = new ArrayList<Number>();
        coords2.add((Number) v.getValueAtIndex(3));
        coords2.add((Number) v.getValueAtIndex(4));
        List<Object> values = new ArrayList<Object>();
        values.add(coords1);
        values.add(coords2);
        Object field = v.getValueAtIndex(0);
        if(index) {
            currentIndex = new IndexQuery(field.toString(), QueryOperation.GEO_BOUNDARY_BOX, values);
        }
        else {
            currentData = new DataQuery(field, FieldType.FIELD, QueryOperation.GEO_BOUNDARY_BOX,
              values, FieldType.VALUE);
        }
    }

    private void handleDateFieldFunction(Function function) {
        ExpressionList parameters = function.getParameters();
        if (parameters.getExpressions().size() != 1) {
            throw new SQLParseException("DATE_FIELD function requires exactly one parameter, the field name.");
        }
        hintType = HintType.DATE;
        Expression expression = parameters.getExpressions().get(0);
        Column column = getColumn(expression);
        expressions.add(column);
    }

    private void handleToDateFunction(Function function) {
        ExpressionList parameters = function.getParameters();
        if (parameters.getExpressions().size() != 2) {
            throw new SQLParseException("TO_DATE function requires exactly two parameter, the date and the date format.");
        }
        hintType = HintType.DATE;
        String date = ((StringValue) parameters.getExpressions().get(0)).getValue();
        String pattern = ((StringValue) parameters.getExpressions().get(1)).getValue();
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        try {
            expressions.add(sdf.parse(date).getTime());
        } catch (ParseException e) {
            throw new SQLParseException("Cannot parse date in TO_DATE function: " + e.getMessage());
        }
    }

    private void handleIdxFunction(Function function) {
        ExpressionList parameters = function.getParameters();
        if (parameters.getExpressions().size() != 1) {
            throw new SQLParseException("IDX function requires exactly one parameter.");
        }
        index = true;
        Expression expression = parameters.getExpressions().get(0);
        Column column = getColumn(expression);
        expressions.add(column);
    }

    private Column getColumn(Expression expression) {
        if (expression instanceof Column) {
            return ((Column) expression);
        } else if (expression instanceof StringValue) {
            return new Column(((StringValue) expression).getValue());
        }
        throw new SQLParseException("Column names must be defined as string with quotes or without.");
    }
}
