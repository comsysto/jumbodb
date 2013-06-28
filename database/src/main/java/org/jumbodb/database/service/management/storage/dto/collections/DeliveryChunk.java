package org.jumbodb.database.service.management.storage.dto.collections;

import org.apache.commons.io.FileUtils;

import java.util.List;

/**
 * User: carsten
 * Date: 4/3/13
 * Time: 8:12 PM
 */
public class DeliveryChunk implements Comparable<DeliveryChunk> {
    private String key;
    private List<DeliveryVersion> versions;
    private long compressedSize = -1;
    private long uncompressedSize = -1;
    private long indexSize = -1;

    public DeliveryChunk(String key, List<DeliveryVersion> versions) {
        this.key = key;
        this.versions = versions;
    }

    public String getKey() {
        return key;
    }

    public List<DeliveryVersion> getVersions() {
        return versions;
    }

    public long getCompressedSize() {
        if(compressedSize == -1) {
            compressedSize = calculateCompressedSize();
        }
        return compressedSize;
    }

    private long calculateCompressedSize() {
        long result = 0l;
        for (DeliveryVersion version : versions) {
            result += version.getCompressedSize();
        }
        return result;
    }

    public long getUncompressedSize() {
        if(uncompressedSize == -1) {
            uncompressedSize = calculateUncompressedSize();
        }
        return uncompressedSize;
    }

    private long calculateUncompressedSize() {
        long result = 0l;
        for (DeliveryVersion version : versions) {
            result += version.getUncompressedSize();
        }
        return result;
    }

    public long getIndexSize() {
        if(indexSize == -1) {
            indexSize = calculateIndexSize();
        }
        return indexSize;
    }

    private long calculateIndexSize() {
        long result = 0l;
        for (DeliveryVersion version : versions) {
            result += version.getIndexSize();
        }
        return result;
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
    public int compareTo(DeliveryChunk deliveryChunk) {
        return getKey().compareTo(deliveryChunk.getKey());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DeliveryChunk)) return false;

        DeliveryChunk that = (DeliveryChunk) o;

        if (!key.equals(that.key)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }

    @Override
    public String toString() {
        return "DeliveryChunk{" +
                "key='" + key + '\'' +
                ", versions=" + versions +
                ", compressedSize=" + compressedSize +
                ", uncompressedSize=" + uncompressedSize +
                ", indexSize=" + indexSize +
                '}';
    }
}
