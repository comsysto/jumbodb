package org.jumbodb.connector.importer;

import org.jumbodb.common.query.ChecksumType;

/**
 * User: carsten
 * Date: 2/26/13
 * Time: 4:48 PM
 */
public class DataInfo {
    private String collection;
    private String fileName;
    private long fileLength;
    private String deliveryKey;
    private String deliveryVersion;
    private ChecksumType checksumType;
    private String checksum;

    public DataInfo(String deliveryKey, String deliveryVersion, String collection, String fileName, long fileLength) {
        this(deliveryKey, deliveryVersion, collection, fileName, fileLength, ChecksumType.NONE, null);
    }

    public DataInfo(String deliveryKey, String deliveryVersion, String collection, String fileName, long fileLength,
      ChecksumType checksumType, String checksum) {
        this.collection = collection;
        this.fileName = fileName;
        this.fileLength = fileLength;
        this.deliveryKey = deliveryKey;
        this.deliveryVersion = deliveryVersion;
        this.checksumType = checksumType;
        this.checksum = checksum;
    }

    public String getCollection() {
        return collection;
    }

    public String getFileName() {
        return fileName;
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

    public ChecksumType getChecksumType() {
        return checksumType;
    }

    public String getChecksum() {
        return checksum;
    }

    @Override
    public String toString() {
        return "DataInfo{" +
          "collection='" + collection + '\'' +
          ", fileName='" + fileName + '\'' +
          ", fileLength=" + fileLength +
          ", deliveryKey='" + deliveryKey + '\'' +
          ", deliveryVersion='" + deliveryVersion + '\'' +
          ", checksumType=" + checksumType +
          ", checksum='" + checksum + '\'' +
          '}';
    }
}
