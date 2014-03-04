package org.jumbodb.connector.importer;

/**
 * User: carsten
 * Date: 2/28/13
 * Time: 1:32 PM
 */
// CARSTEN remove
public class MetaIndex implements Comparable<MetaIndex> {
    private String collection;
    private String deliveryKey;
    private String deliveryVersion;
    private String indexName;
    private String indexStrategy;
    private String indexSourceFields;

    public MetaIndex(String collection, String deliveryKey, String deliveryVersion, String indexName, String indexStrategy, String indexSourceFields) {
        this.collection = collection;
        this.deliveryKey = deliveryKey;
        this.deliveryVersion = deliveryVersion;
        this.indexName = indexName;
        this.indexStrategy = indexStrategy;
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

    public String getIndexStrategy() {
        return indexStrategy;
    }

    public String getIndexSourceFields() {
        return indexSourceFields;
    }

    @Override
    public int compareTo(MetaIndex o) {
        return indexName.compareTo(o.indexName);
    }

    @Override
    public String toString() {
        return "MetaIndex{" +
                "collection='" + collection + '\'' +
                ", deliveryKey='" + deliveryKey + '\'' +
                ", deliveryVersion='" + deliveryVersion + '\'' +
                ", indexName='" + indexName + '\'' +
                ", indexStrategy='" + indexStrategy + '\'' +
                ", indexSourceFields='" + indexSourceFields + '\'' +
                '}';
    }
}
