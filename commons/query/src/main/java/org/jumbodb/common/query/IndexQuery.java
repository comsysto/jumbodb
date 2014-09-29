package org.jumbodb.common.query;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.LinkedList;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class IndexQuery {
    private String name;
    private QueryOperation queryOperation;
    private Object value;
    private DataQuery dataAnd;
    private IndexQuery indexAnd;

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

    public DataQuery getDataAnd() {
        return dataAnd;
    }

    public void setDataAnd(DataQuery dataAnd) {
        this.dataAnd = dataAnd;
    }

    public IndexQuery getIndexAnd() {
        return indexAnd;
    }

    public void setIndexAnd(IndexQuery indexAnd) {
        this.indexAnd = indexAnd;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexQuery that = (IndexQuery) o;

        if (dataAnd != null ? !dataAnd.equals(that.dataAnd) : that.dataAnd != null) return false;
        if (indexAnd != null ? !indexAnd.equals(that.indexAnd) : that.indexAnd != null) return false;
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
        result = 31 * result + (dataAnd != null ? dataAnd.hashCode() : 0);
        result = 31 * result + (indexAnd != null ? indexAnd.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "IndexQuery{" +
                "name='" + name + '\'' +
                ", queryOperation=" + queryOperation +
                ", value=" + value +
                ", dataAnd=" + dataAnd +
                ", indexAnd=" + indexAnd +
                '}';
    }
}

