package core.query;

import org.codehaus.jackson.annotate.JsonIgnore;

import java.util.*;

/**
 * User: carsten
 * Date: 11/23/12
 * Time: 1:05 PM
 */
public class JumboQuery {
    private List<IndexComparision> indexComparision;
    private List<JsonValueComparision> jsonComparision;

    public List<IndexComparision> getIndexComparision() {
        return indexComparision;
    }

    public void setIndexComparision(List<IndexComparision> indexComparision) {
        this.indexComparision = indexComparision;
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
                "indexComparision=" + indexComparision +
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

    public static class IndexComparision {
        private String name;
        private List<String> values;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public List<String> getValues() {
            return values;
        }

        public void setValues(List<String> values) {
            this.values = values;
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
