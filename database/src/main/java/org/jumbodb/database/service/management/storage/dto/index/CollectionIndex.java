package org.jumbodb.database.service.management.storage.dto.index;

/**
 * @author Carsten Hufe
 */
public class CollectionIndex {
    private String indexName;
    private String date;
    private String indexSourceFields;
    private String strategy;

    public CollectionIndex(String indexName, String date, String indexSourceFields, String strategy) {
        this.indexName = indexName;
        this.date = date;
        this.indexSourceFields = indexSourceFields;
        this.strategy = strategy;
    }

    public String getIndexName() {
        return indexName;
    }

    public String getDate() {
        return date;
    }

    public String getIndexSourceFields() {
        return indexSourceFields;
    }

    public String getStrategy() {
        return strategy;
    }

    @Override
    public String toString() {
        return "CollectionIndex{" +
                "indexName='" + indexName + '\'' +
                ", date='" + date + '\'' +
                ", indexSourceFields='" + indexSourceFields + '\'' +
                ", strategy='" + strategy + '\'' +
                '}';
    }
}
