package org.jumbodb.database.service.importer;

/**
 * User: carsten
 * Date: 2/8/13
 * Time: 10:00 AM
 */
public class ImportMetaData {
    private String collection;
    private String deliveryKey;
    private String deliveryVersion;
    private String dataStrategy;
    private String sourcePath;
    private String info;

    public ImportMetaData(String collection, String deliveryKey, String deliveryVersion, String dataStrategy, String sourcePath, String info) {
        this.collection = collection;
        this.deliveryKey = deliveryKey;
        this.deliveryVersion = deliveryVersion;
        this.dataStrategy = dataStrategy;
        this.sourcePath = sourcePath;
        this.info = info;
    }

    public String getDataStrategy() {
        return dataStrategy;
    }

    public String getDeliveryKey() {
        return deliveryKey;
    }

    public String getDeliveryVersion() {
        return deliveryVersion;
    }

    public String getSourcePath() {
        return sourcePath;
    }

    public String getInfo() {
        return info;
    }

    public String getCollection() {
        return collection;
    }


    @Override
    public String toString() {
        return "ImportMetaData{" +
                "collection='" + collection + '\'' +
                ", deliveryKey='" + deliveryKey + '\'' +
                ", deliveryVersion='" + deliveryVersion + '\'' +
                ", dataStrategy='" + dataStrategy + '\'' +
                ", sourcePath='" + sourcePath + '\'' +
                ", info='" + info + '\'' +
                '}';
    }
}
