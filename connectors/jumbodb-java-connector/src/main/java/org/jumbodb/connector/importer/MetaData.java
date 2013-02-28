package org.jumbodb.connector.importer;

/**
 * User: carsten
 * Date: 2/28/13
 * Time: 1:32 PM
 */
public class MetaData {
    private String collection;
    private String deliveryKey;
    private String deliveryVersion;
    private String path;
    private boolean activate;
    private String info;

    public MetaData(String collection, String deliveryKey, String deliveryVersion, String path, boolean activate, String info) {
        this.collection = collection;
        this.deliveryKey = deliveryKey;
        this.deliveryVersion = deliveryVersion;
        this.path = path;
        this.activate = activate;
        this.info = info;
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

    public String getPath() {
        return path;
    }

    public boolean isActivate() {
        return activate;
    }

    public String getInfo() {
        return info;
    }

    @Override
    public String toString() {
        return "MetaData{" +
                "collection='" + collection + '\'' +
                ", deliveryKey='" + deliveryKey + '\'' +
                ", deliveryVersion='" + deliveryVersion + '\'' +
                ", path='" + path + '\'' +
                ", activate=" + activate +
                ", info='" + info + '\'' +
                '}';
    }
}