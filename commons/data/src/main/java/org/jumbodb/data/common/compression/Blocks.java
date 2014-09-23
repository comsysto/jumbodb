package org.jumbodb.data.common.compression;

import java.util.List;

/**
 * User: carsten
 * Date: 3/25/13
 * Time: 3:14 PM
 */
public class Blocks {
    private long length;
    private long datasets;
    private int blockSize;
    private int numberOfBlocks;
    private List<Integer> blocks;

    public Blocks(long length, long datasets, int blockSize, int numberOfBlocks, List<Integer> blocks) {
        this.length = length;
        this.datasets = datasets;
        this.blockSize = blockSize;
        this.numberOfBlocks = numberOfBlocks;
        this.blocks = blocks;
    }

    public long getLength() {
        return length;
    }

    public int getBlockSize() {
        return blockSize;
    }

    public int getNumberOfBlocks() {
        return numberOfBlocks;
    }

    public List<Integer> getBlocks() {
        return blocks;
    }

    public long getDatasets() {
        return datasets;
    }

    public long getOffsetForBlock(long blockIndex, long headerSize, long blockOverhead) {
        long result = headerSize;
        for(int i = 0; i < blockIndex; i++) {
            result += blocks.get(i) + blockOverhead;
        }
        return result;
    }

    public long getSizeForBlock(long numberChunk) {
        return blocks.get((int) numberChunk);
    }

    @Override
    public String toString() {
        return "Blocks{" +
          "length=" + length +
          ", datasets=" + datasets +
          ", blockSize=" + blockSize +
          ", numberOfBlocks=" + numberOfBlocks +
          ", blocks=" + blocks +
          '}';
    }
}
