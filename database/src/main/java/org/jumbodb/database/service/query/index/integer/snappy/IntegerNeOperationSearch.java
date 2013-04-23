package org.jumbodb.database.service.query.index.integer.snappy;

import org.jumbodb.database.service.query.snappy.SnappyChunks;
import org.jumbodb.database.service.query.snappy.SnappyUtil;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @author Carsten Hufe
 */
public class IntegerNeOperationSearch implements OperationSearch<Integer> {
    @Override
    public long findFirstMatchingChunk(RandomAccessFile indexRaf, Integer searchValue, SnappyChunks snappyChunks) throws IOException {
        int numberOfChunks = snappyChunks.getNumberOfChunks();
        int fromChunk = 0;
        int toChunk = numberOfChunks;
        // TODO verify snappy version
        while(toChunk != 0) {
            int currentChunk = (toChunk - fromChunk) / 2;

            byte[] uncompressed = SnappyUtil.getUncompressed(indexRaf, snappyChunks, currentChunk);
            int firstInt = IntegerSnappySearchIndexUtils.readFirstInt(uncompressed);
            int lastInt = IntegerSnappySearchIndexUtils.readLastInt(uncompressed);

            // just going up
            if(firstInt != searchValue || lastInt != searchValue) {
                toChunk = currentChunk;
            } else {
                return currentChunk;
            }

        }
        return 0;
    }

    @Override
    public boolean matching(Integer currentValue, Integer searchValue) {
        return !currentValue.equals(searchValue);
    }
}
