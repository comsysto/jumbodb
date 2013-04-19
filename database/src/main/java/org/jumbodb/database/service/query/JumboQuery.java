package org.jumbodb.database.service.query;

import java.util.*;

/**
 * User: carsten
 * Date: 11/23/12
 * Time: 1:05 PM
 */
public class JumboQuery {
    private List<IndexQuery> indexQuery;
    private List<JsonValueComparision> jsonComparision;

    public List<IndexQuery> getIndexQuery() {
        return indexQuery;
    }

    public void setIndexQuery(List<IndexQuery> indexQuery) {
        this.indexQuery = indexQuery;
    }

    public List<JsonValueComparision> getJsonComparision() {
        return jsonComparision;
    }

    public void setJsonComparision(List<JsonValueComparision> jsonComparision) {
        this.jsonComparision = jsonComparision;
    }

    @Override
    public String toString() {
        return "JumboQuery{" +
                "indexQuery=" + indexQuery +
                ", jsonComparision=" + jsonComparision +
                '}';
    }

    public enum JsonComparisionType { EQUALS, EQUALS_IGNORE_CASE }

    public static class JsonValueComparision {
        private JsonComparisionType comparisionType;
        private String name;
        private Set<Object> values;

        public JsonComparisionType getComparisionType() {
            return comparisionType;
        }

        public void setComparisionType(JsonComparisionType comparisionType) {
            this.comparisionType = comparisionType;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Set<Object> getValues() {
            return values;
        }

        public void setValues(Set<Object> values) {
            this.values = values;
        }

        @Override
        public String toString() {
            return "JsonValueComparision{" +
                    "comparisionType=" + comparisionType +
                    ", name='" + name + '\'' +
                    ", values='" + values + '\'' +
                    '}';
        }
    }

    public static class IndexQuery {
        private String name;
        private List<IndexClause> clauses;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public List<IndexClause> getClauses() {
            return clauses;
        }

        public void setClauses(List<IndexClause> clauses) {
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

    public static class IndexClause {
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
}
