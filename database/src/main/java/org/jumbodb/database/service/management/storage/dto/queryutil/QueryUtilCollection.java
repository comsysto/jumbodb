package org.jumbodb.database.service.management.storage.dto.queryutil;

import org.jumbodb.common.query.QueryOperation;

import java.util.List;

/**
 * @author Carsten Hufe
 */
public class QueryUtilCollection {
    private String collection;
    private List<QueryUtilIndex> indexes;
    private String dataStrategy;
    private List<QueryOperation> supportedOperations;

    public QueryUtilCollection(String collection, List<QueryUtilIndex> indexes, String dataStrategy, List<QueryOperation> supportedOperations) {
        this.collection = collection;
        this.indexes = indexes;
        this.dataStrategy = dataStrategy;
        this.supportedOperations = supportedOperations;
    }

    public String getCollection() {
        return collection;
    }

    public List<QueryUtilIndex> getIndexes() {
        return indexes;
    }

    public String getDataStrategy() {
        return dataStrategy;
    }

    public List<QueryOperation> getSupportedOperations() {
        return supportedOperations;
    }

    @Override
    public String toString() {
        return "QueryUtilCollection{" +
                "collection='" + collection + '\'' +
                ", indexes=" + indexes +
                ", dataStrategy='" + dataStrategy + '\'' +
                ", supportedOperations=" + supportedOperations +
                '}';
    }
}
