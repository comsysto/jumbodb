package org.jumbodb.database.service.management.storage.dto.deliveries;

import org.apache.commons.io.FileUtils;
import org.jumbodb.database.service.management.storage.dto.collections.DeliveryChunk;

import java.util.List;

/**
 * User: carsten
 * Date: 4/3/13
 * Time: 8:13 PM
 */
public class ChunkedDeliveryVersion implements Comparable<ChunkedDeliveryVersion> {
    private String collapseId;
    private String chunkKey;
    private String version;
    private String info;
    private String date;
    private List<VersionedJumboCollection> collections;
    private long compressedSize = -1;
    private long uncompressedSize = -1;
    private long indexSize = -1;
    private boolean versionActive = false;
    private boolean chunkActive = false;

    public ChunkedDeliveryVersion(String collapseId, String chunkKey, String version, String info, String date, boolean versionActive, boolean chunkActive, List<VersionedJumboCollection> collections) {
        this.collapseId = collapseId;
        this.chunkKey = chunkKey;
        this.version = version;
        this.info = info;
        this.date = date;
        this.versionActive = versionActive;
        this.collections = collections;
        this.chunkActive = chunkActive;
    }

    public boolean isVersionActive() {
        return versionActive;
    }

    public boolean isChunkActive() {
        return chunkActive;
    }

    public String getCollapseId() {
        return collapseId;
    }

    public String getChunkKey() {
        return chunkKey;
    }

    public String getVersion() {
        return version;
    }

    public String getInfo() {
        return info;
    }

    public String getDate() {
        return date;
    }

    public List<VersionedJumboCollection> getCollections() {
        return collections;
    }

    public long getCompressedSize() {
        if(compressedSize == -1) {
            compressedSize = calculateCompressedSize();
        }
        return compressedSize;
    }

    private long calculateCompressedSize() {
        long result = 0l;
        for (VersionedJumboCollection collection : collections) {
            result += collection.getCompressedSize();
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
        for (VersionedJumboCollection collection : collections) {
            result += collection.getUncompressedSize();
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
        for (VersionedJumboCollection collection : collections) {
            result += collection.getIndexSize();
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
    public int compareTo(ChunkedDeliveryVersion chunkedDeliveryVersion) {
        return chunkedDeliveryVersion.getDate().compareTo(date);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ChunkedDeliveryVersion)) return false;

        ChunkedDeliveryVersion that = (ChunkedDeliveryVersion) o;

        if (date != null ? !date.equals(that.date) : that.date != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return date != null ? date.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "ChunkedDeliveryVersion{" +
                "collapseId='" + collapseId + '\'' +
                ", chunkKey='" + chunkKey + '\'' +
                ", version='" + version + '\'' +
                ", info='" + info + '\'' +
                ", date='" + date + '\'' +
                ", collections=" + collections +
                ", compressedSize=" + compressedSize +
                ", uncompressedSize=" + uncompressedSize +
                ", indexSize=" + indexSize +
                '}';
    }
}
