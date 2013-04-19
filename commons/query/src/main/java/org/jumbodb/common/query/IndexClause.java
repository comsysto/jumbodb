package org.jumbodb.common.query;

import org.codehaus.jackson.annotate.JsonAutoDetect;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class IndexClause {
    private QueryOperation queryOperation;
    private Object value;

    public IndexClause() {
    }

    public IndexClause(QueryOperation queryOperation, String value) {
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
        return "IndexClause{" +
                "queryOperation=" + queryOperation +
                ", value='" + value + '\'' +
                '}';
    }
}