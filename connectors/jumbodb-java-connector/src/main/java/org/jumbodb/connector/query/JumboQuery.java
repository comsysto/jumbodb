package org.jumbodb.connector.query;

import org.codehaus.jackson.annotate.JsonAutoDetect;

import java.util.*;

/**
 * User: carsten
 * Date: 11/23/12
 * Time: 1:05 PM
 */

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class JumboQuery {
    private List<IndexQuery> indexQuery;
    private List<JsonValueComparision> jsonComparision;


    public boolean addIndexQuery(IndexQuery indexComparision) {
        return this.indexQuery.add(indexComparision);
    }

    public boolean addJsonComparision(JsonValueComparision indexComparision) {
        return this.jsonComparision.add(indexComparision);
    }

    public boolean addIndexQuery(String indexName, List<IndexClause> indexValues) {
        return addIndexQuery(new IndexQuery(indexName, indexValues));
    }

    public boolean addJsonComparision(JsonComparisionType comparisionType, String jsonPropertyName, List<Object> jsonValues) {
        return addJsonComparision(new JsonValueComparision(comparisionType, jsonPropertyName, jsonValues));
    }

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
    public enum QueryOperation {
        EQ, LT, GT
    }

    // CARSTEN remove, use Query Operation
    @Deprecated
    public enum JsonComparisionType { EQUALS, EQUALS_IGNORE_CASE }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
    public static class JsonValueComparision {
        private JsonComparisionType comparisionType;
        private String name;
        private List<Object> values;

        public JsonValueComparision() {
        }

        public JsonValueComparision(JsonComparisionType comparisionType, String name, List<Object> values) {
            this.comparisionType = comparisionType;
            this.name = name;
            this.values = values;
        }

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

        public List<Object> getValues() {
            return values;
        }

        public void setValues(List<Object> values) {
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

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
    public static class IndexQuery {
        private String name;
        private List<IndexClause> clauses;

        public IndexQuery() {
        }

        public IndexQuery(String name, List<IndexClause> clauses) {
            this.name = name;
            this.clauses = clauses;
        }

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

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
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

