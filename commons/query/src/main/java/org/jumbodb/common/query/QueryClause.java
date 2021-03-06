package org.jumbodb.common.query;


import com.fasterxml.jackson.annotation.JsonAutoDetect;

import java.util.LinkedList;
import java.util.List;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class QueryClause {
    private QueryOperation queryOperation;
    private Object value;
    private List<JsonQuery> queryClauses = new LinkedList<JsonQuery>();

    public QueryClause() {
    }

    public QueryClause(QueryOperation queryOperation, Object value) {
        this.queryOperation = queryOperation;
        this.value = value;
    }

    public QueryClause(QueryOperation queryOperation, Object value, List<JsonQuery> queryClauses) {
        this.queryOperation = queryOperation;
        this.value = value;
        this.queryClauses = queryClauses;
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

    public List<JsonQuery> getQueryClauses() {
        return queryClauses;
    }

    public void setQueryClauses(List<JsonQuery> queryClauses) {
        this.queryClauses = queryClauses;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        QueryClause that = (QueryClause) o;

        if (queryClauses != null ? !queryClauses.equals(that.queryClauses) : that.queryClauses != null) return false;
        if (queryOperation != that.queryOperation) return false;
        if (value != null ? !value.equals(that.value) : that.value != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = queryOperation != null ? queryOperation.hashCode() : 0;
        result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + (queryClauses != null ? queryClauses.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "QueryClause{" +
                "queryOperation=" + queryOperation +
                ", value='" + value + '\'' +
                '}';
    }
}