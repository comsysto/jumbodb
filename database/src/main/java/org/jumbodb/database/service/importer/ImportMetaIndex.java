package org.jumbodb.database.service.importer;

/**
 * User: carsten
 * Date: 2/8/13
 * Time: 10:00 AM
 */
// CARSTEN remove
public class ImportMetaIndex {
    private String collection;
    private String deliveryKey;
    private String deliveryVersion;
    private String indexName;
    private String strategy;
    private String indexSourceFields;

    public ImportMetaIndex(String collection, String deliveryKey, String deliveryVersion, String indexName, String strategy, String indexSourceFields) {
        this.collection = collection;
        this.deliveryKey = deliveryKey;
        this.deliveryVersion = deliveryVersion;
        this.indexName = indexName;
        this.strategy = strategy;
        this.indexSourceFields = indexSourceFields;
    }

    public String getCollection() {
        return collection;
    }

    public String getDeliveryKey() {
        return deliveryKey;
    }

    public String getDeliveryVersion() {
        return deliveryVersion;
    }

    public String getIndexName() {
        return indexName;
    }

    public String getStrategy() {
        return strategy;
    }

    public String getIndexSourceFields() {
        return indexSourceFields;
    }

    @Override
    public String toString() {
        return "ImportMetaIndex{" +
                "collection='" + collection + '\'' +
                ", deliveryKey='" + deliveryKey + '\'' +
                ", deliveryVersion='" + deliveryVersion + '\'' +
                ", indexName='" + indexName + '\'' +
                ", strategy='" + strategy + '\'' +
                ", indexSourceFields='" + indexSourceFields + '\'' +
                '}';
    }
}
