package org.jumbodb.database.service.query.data;

/**
 * @author Carsten Hufe
 */
public class CollectionDataSize {
    private long datasets;
    private long compressedSize;
    private long uncompressedSize;

    public CollectionDataSize(long datasets, long compressedSize, long uncompressedSize) {
        this.datasets = datasets;
        this.compressedSize = compressedSize;
        this.uncompressedSize = uncompressedSize;
    }

    public long getDatasets() {
        return datasets;
    }

    public long getCompressedSize() {
        return compressedSize;
    }

    public long getUncompressedSize() {
        return uncompressedSize;
    }
}
