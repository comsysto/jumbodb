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
    private List<IndexComparision> indexComparision = new LinkedList<IndexComparision>();
    private List<JsonValueComparision> jsonComparision = new LinkedList<JsonValueComparision>();

    public JumboQuery() {
    }

    public boolean addIndexComparision(IndexComparision indexComparision) {
        return this.indexComparision.add(indexComparision);
    }

    public boolean addJsonComparision(JsonValueComparision indexComparision) {
        return this.jsonComparision.add(indexComparision);
    }

    public boolean addIndexComparision(String indexName, List<String> indexValues) {
        return addIndexComparision(new IndexComparision(indexName, indexValues));
    }

    public boolean addJsonComparision(JsonComparisionType comparisionType, String jsonPropertyName, List<Object> jsonValues) {
        return addJsonComparision(new JsonValueComparision(comparisionType, jsonPropertyName, jsonValues));
    }

    public List<IndexComparision> getIndexComparision() {
        return Collections.unmodifiableList(indexComparision);
    }

    public List<JsonValueComparision> getJsonComparision() {
        return Collections.unmodifiableList(jsonComparision);
    }

    @Override
    public String toString() {
        return "JumboQuery{" +
                ", indexComparision=" + indexComparision +
                ", jsonComparision=" + jsonComparision +
                '}';
    }

    public enum JsonComparisionType { EQUALS, EQUALS_IGNORE_CASE }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
    public static class JsonValueComparision {
        private JsonComparisionType comparisionType;
        private String name;
        private List<Object> values;

        public JsonValueComparision(JsonComparisionType comparisionType, String name, List<Object> values) {
            this.comparisionType = comparisionType;
            this.name = name;
            this.values = Collections.unmodifiableList(values);
        }

        public JsonComparisionType getComparisionType() {
            return comparisionType;
        }

        public String getName() {
            return name;
        }

        public List<Object> getValues() {
            return values;
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
    public static class IndexComparision {
        private String name;
        private List<String> values;

        public IndexComparision(String name, List<String> values) {
            this.name = name;
            this.values = Collections.unmodifiableList(values);
        }

        public String getName() {
            return name;
        }

        public List<String> getValues() {
            return values;
        }

        @Override
        public String toString() {
            return "IndexComparision{" +
                    "name='" + name + '\'' +
                    ", values='" + values + '\'' +
                    '}';
        }
    }
}
