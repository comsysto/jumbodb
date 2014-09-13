package org.jumbodb.common.query;


import com.fasterxml.jackson.annotation.JsonAutoDetect;

import java.util.LinkedList;
import java.util.List;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class JsonQuery {
    private String fieldName;
    private QueryOperation queryOperation;
    private Object value;
    private JsonQuery and;
    private List<JsonQuery> ors = new LinkedList<JsonQuery>();

    public JsonQuery() {
    }

    public JsonQuery(List<JsonQuery> ors) {
        queryOperation = QueryOperation.OR;
        this.ors = ors;
    }

    public JsonQuery(String fieldName, QueryOperation queryOperation, Object value) {
        this.fieldName = fieldName;
        this.queryOperation = queryOperation;
        this.value = value;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
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

    public JsonQuery getAnd() {
        return and;
    }

    public void setAnd(JsonQuery and) {
        this.and = and;
    }

    public List<JsonQuery> getOrs() {
        return ors;
    }

    public void setOrs(List<JsonQuery> ors) {
        this.ors = ors;
    }

    @Override
    public String toString() {
        return "JsonQuery{" +
                "fieldName='" + fieldName + '\'' +
                ", queryOperation=" + queryOperation +
                ", value=" + value +
                ", and=" + and +
                ", ors=" + ors +
                '}';
    }
}

