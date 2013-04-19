package org.jumbodb.common.query;

import org.codehaus.jackson.annotate.JsonAutoDetect;

import java.util.List;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class JsonValueComparision {
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