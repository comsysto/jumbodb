package org.jumbodb.database.service.query.sql;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.*;
import org.jumbodb.common.query.SelectField;
import org.jumbodb.common.query.SelectFieldFunction;

import java.util.List;

/**
 * Created by Carsten on 27.09.2014.
 */
public class SelectedItemsVisitor extends SelectItemVisitorAdapter {
    private SelectField field = new SelectField();
    private Distinct distinct;

    public SelectedItemsVisitor(Distinct distinct) {

        this.distinct = distinct;
    }

    @Override
    public void visit(AllColumns columns) {
        field.setColumnName(SelectField.ALL);
        field.setAlias(SelectField.ALL);
        field.setDistinct(false);
        field.setFunction(SelectFieldFunction.NONE);
    }

    @Override
    public void visit(AllTableColumns columns) {
        throw new SQLParseException("\"SELECT TableName.* FROM ...\" is not supported!");
    }

    @Override
    public void visit(SelectExpressionItem item) {
        Expression expression = item.getExpression();
        if(expression instanceof Column) {
            Column column = (Column)expression;
            field.setColumnName(column.getFullyQualifiedName());
            field.setDistinct(distinct != null);
            field.setFunction(SelectFieldFunction.NONE);
        }
        else if(expression instanceof Function) {
            Function function = (Function)expression;
            field.setFunction(resolveFunction(function));
            field.setDistinct(function.isDistinct());
            field.setColumnName(getColumnName(function));
        }
        field.setAlias(item.getAlias() != null ? item.getAlias().getName() : item.getExpression().toString());
    }

    private String getColumnName(Function func) {
        if(func.isAllColumns()) {
            return SelectField.ALL;
        }
        List<Expression> expressions = func.getParameters().getExpressions();
        if(expressions.size() != 1) {
            throw new SQLParseException("Select functions require exactly one parameter.");
        }
        Expression expression = expressions.get(0);
        if(expression instanceof Column) {
            Column column = (Column)expression;
            return column.getFullyQualifiedName();
        } else if(expression instanceof StringValue) {
            return ((StringValue)expression).getValue();
        }
        else if(expression instanceof Function) {
            Function function = (Function)expression;
            String functionName = function.getName();
            if(isFieldFunction(functionName)) {
                return handleFieldFunction(function);
            }
            else {
                throw new SQLParseException("Function '" + functionName + "' is not allowed at this position. In function statements are only 'FIELD' functions allowed.");
            }
        }
        return null;
    }

    private boolean isFieldFunction(String functionName) {
        return "FIELD".equalsIgnoreCase(functionName);
    }

    public static String handleFieldFunction(Function function) {
        List<Expression> fieldExpressions = function.getParameters().getExpressions();
        if(fieldExpressions.size() != 1) {
            throw new SQLParseException("FIELD function requires exactly one parameter.");
        }
        Expression field = fieldExpressions.get(0);
        if(field instanceof Column) {
            Column column = (Column)field;
            return column.getFullyQualifiedName();
        }
        else if(field instanceof StringValue) {
            return ((StringValue)field).getValue();
        }
        throw new SQLParseException("Unsupported type for FIELD function. Must be a string or field name.");
    }

    private SelectFieldFunction resolveFunction(Function function) {
        String functionName = function.getName();
        for (SelectFieldFunction selectFieldFunction : SelectFieldFunction.values()) {
            if(selectFieldFunction.toString().equalsIgnoreCase(functionName)) {
                return selectFieldFunction;
            }
        }
        if(isFieldFunction(functionName)) {
            return SelectFieldFunction.NONE;
        }
        throw new SQLParseException("Function '" + functionName + "' is not supported by JumboDB!");
    }

    public SelectField getField() {
        return field;
    }
}
