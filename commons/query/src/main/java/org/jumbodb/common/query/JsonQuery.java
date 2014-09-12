package org.jumbodb.common.query;


import com.fasterxml.jackson.annotation.JsonAutoDetect;

import java.util.List;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class JsonQuery {
    private String fieldName;
    private List<QueryClause> clauses;

    public JsonQuery() {
    }

    public JsonQuery(String fieldName, List<QueryClause> clauses) {
        this.fieldName = fieldName;
        this.clauses = clauses;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public List<QueryClause> getClauses() {
        return clauses;
    }

    public void setClauses(List<QueryClause> clauses) {
        this.clauses = clauses;
    }

    @Override
    public String toString() {
        return "JsonQuery{" +
                "name='" + fieldName + '\'' +
                ", values='" + clauses + '\'' +
                '}';
    }
}

