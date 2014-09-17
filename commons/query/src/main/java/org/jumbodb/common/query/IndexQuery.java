package org.jumbodb.common.query;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonSerialize(include= JsonSerialize.Inclusion.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class IndexQuery {
    private String name;
    private QueryOperation queryOperation;
    private Object value;
    private DataQuery andJson;
    private IndexQuery andIndex;

    public IndexQuery() {
    }

    public IndexQuery(String name, QueryOperation queryOperation, Object value) {
        this.name = name;
        this.queryOperation = queryOperation;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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

    public DataQuery getAndJson() {
        return andJson;
    }

    public void setAndJson(DataQuery andJson) {
        this.andJson = andJson;
    }

    public IndexQuery getAndIndex() {
        return andIndex;
    }

    public void setAndIndex(IndexQuery andIndex) {
        this.andIndex = andIndex;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexQuery that = (IndexQuery) o;

        if (andIndex != null ? !andIndex.equals(that.andIndex) : that.andIndex != null) return false;
        if (andJson != null ? !andJson.equals(that.andJson) : that.andJson != null) return false;
        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        if (queryOperation != that.queryOperation) return false;
        if (value != null ? !value.equals(that.value) : that.value != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (queryOperation != null ? queryOperation.hashCode() : 0);
        result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + (andJson != null ? andJson.hashCode() : 0);
        result = 31 * result + (andIndex != null ? andIndex.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "IndexQuery{" +
                "name='" + name + '\'' +
                ", queryOperation=" + queryOperation +
                ", value=" + value +
                ", andJson=" + andJson +
                ", andIndex=" + andIndex +
                '}';
    }
}

