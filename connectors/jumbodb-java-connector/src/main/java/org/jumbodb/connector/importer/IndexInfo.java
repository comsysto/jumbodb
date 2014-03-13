package org.jumbodb.connector.importer;

import org.jumbodb.common.query.ChecksumType;

/**
 * User: carsten
 * Date: 2/26/13
 * Time: 4:48 PM
 */
public class IndexInfo implements Comparable<IndexInfo> {
    private String collection;
    private String indexName;
    private String fileName;
    private long fileLength;
    private String deliveryKey;
    private String deliveryVersion;
    private final ChecksumType checksumType;
    private final String checksum;

    public IndexInfo(String deliveryKey, String deliveryVersion, String collection, String indexName, String fileName,
      long fileLength) {
        this(deliveryKey, deliveryVersion, collection, indexName, fileName, fileLength, ChecksumType.NONE, null);
    }

    public IndexInfo(String deliveryKey, String deliveryVersion, String collection, String indexName, String fileName,
      long fileLength, ChecksumType checksumType, String checksum) {
        this.collection = collection;
        this.indexName = indexName;
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

    public String getIndexName() {
        return indexName;
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
    public int compareTo(IndexInfo o) {
        int i = indexName.compareTo(o.indexName);
        if(i != 0) {
            return i;
        }
        return fileName.compareTo(o.fileName);
    }

    @Override
    public String toString() {
        return "IndexInfo{" +
          "collection='" + collection + '\'' +
          ", indexName='" + indexName + '\'' +
          ", fileName='" + fileName + '\'' +
          ", fileLength=" + fileLength +
          ", deliveryKey='" + deliveryKey + '\'' +
          ", deliveryVersion='" + deliveryVersion + '\'' +
          ", checksumType=" + checksumType +
          ", checksum='" + checksum + '\'' +
          '}';
    }
}
