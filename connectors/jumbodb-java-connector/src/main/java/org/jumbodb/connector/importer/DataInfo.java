package org.jumbodb.connector.importer;

/**
 * User: carsten
 * Date: 2/26/13
 * Time: 4:48 PM
 */
public class DataInfo {
    private String collection;
    private String filename;
    private long fileLength;
    private String deliveryKey;
    private String deliveryVersion;

    public DataInfo(String collection, String filename, long fileLength, String deliveryKey, String deliveryVersion) {
        this.collection = collection;
        this.filename = filename;
        this.fileLength = fileLength;
        this.deliveryKey = deliveryKey;
        this.deliveryVersion = deliveryVersion;
    }

    public String getCollection() {
        return collection;
    }

    public String getFilename() {
        return filename;
    }

    public long getFileLength() {
        return fileLength;
    }

    public String getDeliveryKey() {
        return deliveryKey;
    }

    public String getDeliveryVersion() {
        return deliveryVersion;
    }

    @Override
    public String toString() {
        return "DataInfo{" +
                "collection='" + collection + '\'' +
                ", filename='" + filename + '\'' +
                ", fileLength=" + fileLength +
                ", deliveryKey='" + deliveryKey + '\'' +
                ", deliveryVersion='" + deliveryVersion + '\'' +
                '}';
    }
}
