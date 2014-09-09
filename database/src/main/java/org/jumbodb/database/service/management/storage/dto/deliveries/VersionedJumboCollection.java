package org.jumbodb.database.service.management.storage.dto.deliveries;

import org.apache.commons.io.FileUtils;

/**
 * User: carsten
 * Date: 4/3/13
 * Time: 8:14 PM
 */
public class VersionedJumboCollection implements Comparable<VersionedJumboCollection> {
    private String chunkKey;
    private String version;
    private String collectionName;
    private String info;
    private String date;
    private String sourcePath;
    private String strategy;
    private long compressedSize;
    private long uncompressedSize;
    private long indexSize;

    public VersionedJumboCollection(String chunkKey, String version, String collectionName, String info,
      String date, String sourcePath, String strategy, long compressedSize, long uncompressedSize, long indexSize) {
        this.collectionName = collectionName;
        this.version = version;
        this.chunkKey = chunkKey;
        this.info = info;
        this.date = date;
        this.sourcePath = sourcePath;
        this.strategy = strategy;
        this.compressedSize = compressedSize;
        this.uncompressedSize = uncompressedSize;
        this.indexSize = indexSize;
    }

    public String getInfo() {
        return info;
    }

    public String getSourcePath() {
        return sourcePath;
    }

    public String getStrategy() {
        return strategy;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public String getVersion() {
        return version;
    }

    public String getChunkKey() {
        return chunkKey;
    }

    public String getDate() {
        return date;
    }

    public long getCompressedSize() {
        return compressedSize;
    }

    public long getUncompressedSize() {
        return uncompressedSize;
    }

    public long getIndexSize() {
        return indexSize;
    }

    public String getFormatedCompressedSize() {
        return FileUtils.byteCountToDisplaySize(getCompressedSize());
    }

    public String getFormatedUncompressedSize() {
        return FileUtils.byteCountToDisplaySize(getUncompressedSize());
    }

    public String getFormatedIndexSize() {
        return FileUtils.byteCountToDisplaySize(getIndexSize());
    }

    @Override
    public int compareTo(VersionedJumboCollection versionedJumboCollection) {
        return getCollectionName().compareTo(versionedJumboCollection.getCollectionName());
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final VersionedJumboCollection that = (VersionedJumboCollection) o;

        if (!collectionName.equals(that.collectionName)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return collectionName.hashCode();
    }

    @Override
    public String toString() {
        return "VersionedJumboCollection{" +
          "chunkKey='" + chunkKey + '\'' +
          ", version='" + version + '\'' +
          ", collectionName='" + collectionName + '\'' +
          ", info='" + info + '\'' +
          ", date='" + date + '\'' +
          ", sourcePath='" + sourcePath + '\'' +
          ", strategy='" + strategy + '\'' +
          ", compressedSize=" + compressedSize +
          ", uncompressedSize=" + uncompressedSize +
          ", indexSize=" + indexSize +
          '}';
    }
}
