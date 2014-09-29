package org.jumbodb.database.service.query;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import org.apache.commons.lang.StringUtils;
import org.jumbodb.common.query.*;
import org.jumbodb.database.service.query.sql.SQLParseException;
import org.jumbodb.database.service.query.sql.SelectedItemsVisitor;
import org.jumbodb.database.service.query.sql.WhereVisitor;

import java.util.*;

/**
 * @author Carsten Hufe
 */
public class JumboQueryConverterService {

    public JumboQuery convertSqlToJumboQuery(String sql) {
        try {
            String trimmedSql = StringUtils.trim(sql);
            boolean resultCacheEnabled = true;
            if(StringUtils.endsWithIgnoreCase(trimmedSql, " NOCACHE")) {
                resultCacheEnabled = false;
            }
            String plainSql = StringUtils.removeEndIgnoreCase(trimmedSql, " NOCACHE");
            plainSql = StringUtils.removeEndIgnoreCase(plainSql, " CACHE");
            Statement stmt = CCJSqlParserUtil.parse(plainSql);
            if(stmt instanceof Select) {
                Select select = (Select)stmt;
                PlainSelect selectBody = (PlainSelect) select.getSelectBody(); // nur plain select
                assertSqlFeatures(selectBody);
                JumboQuery jumboQuery = buildJumboQuery(selectBody);
                jumboQuery.setResultCacheEnabled(resultCacheEnabled);
                verifySelectFunctionsAndGroupBy(jumboQuery);
                return jumboQuery;
            }
            throw new SQLParseException("Only plain SELECT statements are allowed!");
        } catch (JSQLParserException e) {
            throw new JumboCommonException(e.getCause().getMessage());
        }
    }

    private void verifySelectFunctionsAndGroupBy(JumboQuery jumboQuery) {
        boolean hasSelectFunctions = hasSelectFunctions(jumboQuery);
        if(hasSelectFunctions) {
            Set<String> groupByFields = findGroupByFields(jumboQuery);
            Set<String> plainFields = findSelectFieldsWithoutFunction(jumboQuery);
            plainFields.removeAll(groupByFields);
            if(plainFields.size() > 0) {
                throw new SQLParseException("The selected fields " + plainFields + " require a group by, if you want to collect all values use COLLECT or COLLECT(DISTINCT ...).");
            }
        }
    }

    private Set<String> findSelectFieldsWithoutFunction(JumboQuery jumboQuery) {
        Set<String> result = new HashSet<String>();
        for (SelectField selectField : jumboQuery.getSelectedFields()) {
            if(selectField.getFunction() == SelectFieldFunction.NONE) {
                result.add(selectField.getColumnName());
            }
        }
        return result;
    }

    private Set<String> findGroupByFields(JumboQuery jumboQuery) {
        return new HashSet<String>(jumboQuery.getGroupByFields());
    }

    private boolean hasSelectFunctions(JumboQuery jumboQuery) {
        List<SelectField> selectedFields = jumboQuery.getSelectedFields();
        for (SelectField selectedField : selectedFields) {
            if(selectedField.getFunction() != SelectFieldFunction.NONE) {
                return true;
            }
        }
        return false;
    }

    private JumboQuery buildJumboQuery(PlainSelect selectBody) {
        JumboQuery jumboQuery = new JumboQuery();
        jumboQuery.setCollection(getCollection(selectBody));
        jumboQuery.setSelectedFields(getSelectedFields(selectBody));
        if(selectBody.getLimit() != null) {
            jumboQuery.setLimit((int)selectBody.getLimit().getRowCount());
        }
        jumboQuery.setOrderBy(getOrderByFields(selectBody));
        jumboQuery.setGroupByFields(getGroupByFields(selectBody));
        WhereVisitor expressionVisitor = new WhereVisitor();
        Expression where = selectBody.getWhere();
        if(where != null) {
            where.accept(expressionVisitor);
            List<DataQuery> dataOrs = expressionVisitor.getDataOrs();
            jumboQuery.setDataOrs(dataOrs);
            List<IndexQuery> indexOrs = expressionVisitor.getIndexOrs();
            jumboQuery.setIndexOrs(indexOrs);
            if(!dataOrs.isEmpty() && !indexOrs.isEmpty()) {
                throw new SQLParseException("It is not allowed to combine a query on an index with OR to a field, because it results in a full scan and index resolution is not required and inperformant. Rebuild your query by not using indexes.");
            }
        }
        return jumboQuery;
    }

    private List<String> getGroupByFields(PlainSelect selectBody) {
        List<Expression> groupByFields = selectBody.getGroupByColumnReferences();
        if(groupByFields == null) {
            return Collections.emptyList();
        }
        List<String> result = new LinkedList<String>();
        for (Expression groupByField : groupByFields) {
            if(groupByField instanceof Column) {
                Column column = (Column)groupByField;
                result.add(column.getFullyQualifiedName());
            }
            else if(groupByField instanceof Function) {
                Function function = (Function) groupByField;
                if("FIELD".equalsIgnoreCase(function.getName())) {
                    result.add(SelectedItemsVisitor.handleFieldFunction(function));
                }
                else {
                    throw new SQLParseException("Only fields and FIELD function is allowed in group by.");
                }
            }
            else {
                throw new SQLParseException("Only fields and FIELD function is allowed in group by.");
            }
        }
        return result;
    }

    private List<OrderField> getOrderByFields(PlainSelect selectBody) {
        List<OrderByElement> orderByElements = selectBody.getOrderByElements();
        if(orderByElements == null) {
            return Collections.emptyList();
        }
        List<OrderField> result = new LinkedList<OrderField>();
        for (OrderByElement orderByElement : orderByElements) {
            Expression expression = orderByElement.getExpression();
            if(expression instanceof Column) {
                Column column = (Column)expression;
                result.add(new OrderField(column.getFullyQualifiedName(), orderByElement.isAsc()));
            }
            else if(expression instanceof Function) {
                Function function = (Function) expression;
                if("FIELD".equalsIgnoreCase(function.getName())) {
                    result.add(new OrderField(SelectedItemsVisitor.handleFieldFunction(function), orderByElement.isAsc()));
                }
                else {
                    throw new SQLParseException("Only fields and FIELD function is allowed in group by.");
                }
            }
            else {
                throw new SQLParseException("Only fields and FIELD function is allowed in group by.");
            }
        }
        return result;
    }

    private List<SelectField> getSelectedFields(PlainSelect selectBody) {
        List<SelectItem> selectItems = selectBody.getSelectItems();
        List<SelectField> result = new LinkedList<SelectField>();
        for (SelectItem selectItem : selectItems) {
            SelectedItemsVisitor selectItemVisitor = new SelectedItemsVisitor(selectBody.getDistinct());
            selectItem.accept(selectItemVisitor);
            result.add(selectItemVisitor.getField());
        }
        return result;
    }

    private static String getCollection(PlainSelect selectBody) {
        return selectBody.getFromItem().toString();
    }

    private void assertSqlFeatures(PlainSelect selectBody) {
        if(selectBody.getJoins() != null) {
            throw new SQLParseException("JOINS are currently not supported.");
        }
        else if(selectBody.getHaving() != null) {
            throw new SQLParseException("HAVING is not supported.");
        }
        else if(selectBody.getTop() != null) {
            throw new SQLParseException("TOP is not supported.");
        }
        else if(selectBody.getInto() != null) {
            throw new SQLParseException("INTO is not supported, remember it's a read-only database!");
        }
        else if(selectBody.getFromItem().getAlias() != null) {
            throw new SQLParseException("Table aliases are currently not supported.");
        }
    }
}
