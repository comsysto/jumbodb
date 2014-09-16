package org.jumbodb.data.common.snappy;

import java.util.List;

/**
 * User: carsten
 * Date: 3/25/13
 * Time: 3:14 PM
 */
public class SnappyChunks {
    private long length;
    private long datasets;
    private int chunkSize;
    private int numberOfChunks;
    private List<Integer> chunks;

    public SnappyChunks(long length, long datasets, int chunkSize, int numberOfChunks, List<Integer> chunks) {
        this.length = length;
        this.datasets = datasets;
        this.chunkSize = chunkSize;
        this.numberOfChunks = numberOfChunks;
        this.chunks = chunks;
    }

    public long getLength() {
        return length;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public int getNumberOfChunks() {
        return numberOfChunks;
    }

    public List<Integer> getChunks() {
        return chunks;
    }

    // CARSTEN use in frontend
    public long getDatasets() {
        return datasets;
    }

    public long getOffsetForChunk(long numberChunk) {
        long result = 16l; // snappy version
        for(int i = 0; i < numberChunk; i++) {
            result += chunks.get(i) + 4;
        }
        return result;
    }

    public long getSizeForChunk(long numberChunk) {
        return chunks.get((int) numberChunk);
    }

    @Override
    public String toString() {
        return "SnappyChunks{" +
          "length=" + length +
          ", datasets=" + datasets +
          ", chunkSize=" + chunkSize +
          ", numberOfChunks=" + numberOfChunks +
          ", chunks=" + chunks +
          '}';
    }
}
