package org.jumbodb.database.service.query.index.basic.numeric;

import org.jumbodb.common.query.QueryOperation;

import java.io.File;

/**
 * @author Carsten Hufe
 */
public class CacheIndexClause {
    private File indexFile;
    private QueryOperation queryOperation;
    private Object value;

    public CacheIndexClause(File indexFile, QueryOperation queryOperation, Object value) {
        this.indexFile = indexFile;
        this.queryOperation = queryOperation;
        this.value = value;
    }

    public File getIndexFile() {
        return indexFile;
    }

    public QueryOperation getQueryOperation() {
        return queryOperation;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CacheIndexClause that = (CacheIndexClause) o;

        if (indexFile != null ? !indexFile.equals(that.indexFile) : that.indexFile != null) return false;
        if (queryOperation != that.queryOperation) return false;
        if (value != null ? !value.equals(that.value) : that.value != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = indexFile != null ? indexFile.hashCode() : 0;
        result = 31 * result + (queryOperation != null ? queryOperation.hashCode() : 0);
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }
}
