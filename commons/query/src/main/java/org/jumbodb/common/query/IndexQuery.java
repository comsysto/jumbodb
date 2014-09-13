package org.jumbodb.common.query;

import com.fasterxml.jackson.annotation.JsonAutoDetect;

import java.util.LinkedList;
import java.util.List;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class IndexQuery {
    private String name;
    private List<QueryClause> clauses;

    public IndexQuery() {
    }

    public IndexQuery(String name, List<QueryClause> clauses) {
        this.name = name;
        this.clauses = clauses;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<QueryClause> getClauses() {
        return clauses;
    }

    public void setClauses(List<QueryClause> clauses) {
        this.clauses = clauses;
    }

    @Override
    public String toString() {
        return "IndexQuery{" +
                "name='" + name + '\'' +
                ", values='" + clauses + '\'' +
                '}';
    }
}

