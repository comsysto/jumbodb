package org.jumbodb.connector.importer;

/**
 * User: carsten
 * Date: 2/28/13
 * Time: 1:32 PM
 */
public class MetaIndex {
    private String collection;
    private String deliveryKey;
    private String deliveryVersion;
    private String indexName;
    private String indexStrategy;

    public MetaIndex(String collection, String deliveryKey, String deliveryVersion, String indexName, String indexStrategy) {
        this.collection = collection;
        this.deliveryKey = deliveryKey;
        this.deliveryVersion = deliveryVersion;
        this.indexName = indexName;
        this.indexStrategy = indexStrategy;
    }

    public String getCollection() {
        return collection;
    }

    public void setCollection(String collection) {
        this.collection = collection;
    }

    public String getDeliveryKey() {
        return deliveryKey;
    }

    public void setDeliveryKey(String deliveryKey) {
        this.deliveryKey = deliveryKey;
    }

    public String getDeliveryVersion() {
        return deliveryVersion;
    }

    public void setDeliveryVersion(String deliveryVersion) {
        this.deliveryVersion = deliveryVersion;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public String getIndexStrategy() {
        return indexStrategy;
    }

    public void setIndexStrategy(String indexStrategy) {
        this.indexStrategy = indexStrategy;
    }

    @Override
    public String toString() {
        return "MetaIndex{" +
                "collection='" + collection + '\'' +
                ", deliveryKey='" + deliveryKey + '\'' +
                ", deliveryVersion='" + deliveryVersion + '\'' +
                ", indexName='" + indexName + '\'' +
                ", indexStrategy='" + indexStrategy + '\'' +
                '}';
    }
}
