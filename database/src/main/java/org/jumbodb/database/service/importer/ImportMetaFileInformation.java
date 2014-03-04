package org.jumbodb.database.service.importer;

import org.jumbodb.data.common.meta.ChecksumType;

public class ImportMetaFileInformation {
    public enum FileType {INDEX, DATA}

    private FileType fileType;
    private String fileName;
    private String collection;
    private String indexName;
    private long fileLength;
    private String deliveryKey;
    private String deliveryVersion;
    private ChecksumType checksumType;
    private String checksum;

    public ImportMetaFileInformation(String deliveryKey, String deliveryVersion, String collection, String indexName,
      FileType fileType, String fileName, long fileLength, ChecksumType checksumType, String checksum) {
        this.fileType = fileType;
        this.fileName = fileName;
        this.collection = collection;
        this.indexName = indexName;
        this.fileLength = fileLength;
        this.deliveryKey = deliveryKey;
        this.deliveryVersion = deliveryVersion;
        this.checksumType = checksumType;
        this.checksum = checksum;
    }

    public FileType getFileType() {
        return fileType;
    }

    public String getFileName() {
        return fileName;
    }

    public String getCollection() {
        return collection;
    }

    public String getIndexName() {
        return indexName;
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
        return "ImportMetaFileInformation{" +
          "fileType=" + fileType +
          ", fileName='" + fileName + '\'' +
          ", collection='" + collection + '\'' +
          ", indexName='" + indexName + '\'' +
          ", fileLength=" + fileLength +
          ", deliveryKey='" + deliveryKey + '\'' +
          ", deliveryVersion='" + deliveryVersion + '\'' +
          ", checksumType=" + checksumType +
          ", checksum='" + checksum + '\'' +
          '}';
    }
}