package org.jumbodb.common.query;

import org.codehaus.jackson.annotate.JsonAutoDetect;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class QueryClause {
    private QueryOperation queryOperation;
    private Object value;

    public QueryClause() {
    }

    public QueryClause(QueryOperation queryOperation, String value) {
        this.queryOperation = queryOperation;
        this.value = value;
    }

    public QueryOperation getQueryOperation() {
        return queryOperation;
    }

    public void setQueryOperation(QueryOperation queryOperation) {
        this.queryOperation = queryOperation;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "QueryClause{" +
                "queryOperation=" + queryOperation +
                ", value='" + value + '\'' +
                '}';
    }
}