package org.jumbodb.database.service.management.storage.dto.collections;

import org.apache.commons.io.FileUtils;

import java.util.List;

/**
 * User: carsten
 * Date: 4/3/13
 * Time: 8:12 PM
 */
public class JumboCollection implements Comparable<JumboCollection> {
    private String collapseId;
    private String name;
    private final List<String> infos;
    private List<DeliveryChunk> chunks;
    private long compressedSize = -1;
    private long uncompressedSize = -1;
    private long indexSize = -1;
    private Boolean activeChunk;
    private Boolean activeVersion;

    public JumboCollection(String collapseId, String name, List<String> infos, List<DeliveryChunk> chunks) {
        this.collapseId = collapseId;
        this.name = name;
        this.infos = infos;
        this.chunks = chunks;
    }

    public List<String> getInfos() {
        return infos;
    }

    public String getCollapseId() {
        return collapseId;
    }

    public String getName() {
        return name;
    }

    public List<DeliveryChunk> getChunks() {
        return chunks;
    }

    public Boolean getActive() {
        return getActiveChunk() && getActiveVersion();
    }

    public Boolean getActiveChunk() {
        if(activeChunk == null) {
            activeChunk = calculateActiveChunk();
        }
        return activeChunk;
    }

    public Boolean getActiveVersion() {
        if(activeVersion == null) {
            activeVersion = calculateActiveVersion();
        }
        return activeVersion;
    }

    private Boolean calculateActiveVersion() {
        for (DeliveryChunk chunk : chunks) {
            for (DeliveryVersion deliveryVersion : chunk.getVersions()) {
                if(deliveryVersion.isActive()) {
                    return Boolean.TRUE;
                }
            }
        }
        return Boolean.FALSE;
    }

    private Boolean calculateActiveChunk() {
        for (DeliveryChunk chunk : chunks) {
            if(chunk.isActive()) {
                return Boolean.TRUE;
            }
        }
        return Boolean.FALSE;
    }

    public long getCompressedSize() {
        if(compressedSize == -1) {
            compressedSize = calculateCompressedSize();
        }
        return compressedSize;
    }

    private long calculateCompressedSize() {
        long result = 0l;
        for (DeliveryChunk version : chunks) {
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
        for (DeliveryChunk version : chunks) {
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
        for (DeliveryChunk version : chunks) {
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

    public boolean hasAtLeastOneActiveChunk() {
        for (DeliveryChunk chunk : chunks) {
            if(chunk.isActive()) {
                for (DeliveryVersion version : chunk.getVersions()) {
                    if(version.isActive()) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    @Override
    public int compareTo(JumboCollection jumboCollection) {
        return name.compareTo(jumboCollection.name);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof JumboCollection)) return false;

        JumboCollection that = (JumboCollection) o;

        if (!name.equals(that.name)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public String toString() {
        return "JumboCollection{" +
          "collapseId='" + collapseId + '\'' +
          ", name='" + name + '\'' +
          ", infos=" + infos +
          ", chunks=" + chunks +
          ", compressedSize=" + compressedSize +
          ", uncompressedSize=" + uncompressedSize +
          ", indexSize=" + indexSize +
          ", activeChunk=" + activeChunk +
          ", activeVersion=" + activeVersion +
          '}';
    }
}
