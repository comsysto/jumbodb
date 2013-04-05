package org.jumbodb.database.service.management.storage.dto.collections;

import org.apache.commons.io.FileUtils;

/**
 * User: carsten
 * Date: 4/3/13
 * Time: 8:13 PM
 */
public class DeliveryVersion implements Comparable<DeliveryVersion> {
    private String version;
    private String info;
    private String date;
    private long compressedSize;
    private long uncompressedSize;
    private long indexSize;
    private boolean active;

    public DeliveryVersion(String version, String info, String date, long compressedSize, long uncompressedSize, long indexSize, boolean active) {
        this.version = version;
        this.info = info;
        this.date = date;
        this.compressedSize = compressedSize;
        this.uncompressedSize = uncompressedSize;
        this.indexSize = indexSize;
        this.active = active;
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

    public String getVersion() {
        return version;
    }

    public String getInfo() {
        return info;
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

    public boolean isActive() {
        return active;
    }


    @Override
    public int compareTo(DeliveryVersion deliveryVersion) {
        return deliveryVersion.getDate().compareTo(getDate());
    }

    @Override
    public String toString() {
        return "DeliveryVersion{" +
                "version='" + version + '\'' +
                ", info='" + info + '\'' +
                ", date='" + date + '\'' +
                ", compressedSize=" + compressedSize +
                ", uncompressedSize=" + uncompressedSize +
                ", indexSize=" + indexSize +
                ", active=" + active +
                '}';
    }
}
