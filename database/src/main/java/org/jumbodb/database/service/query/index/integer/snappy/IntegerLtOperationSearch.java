package org.jumbodb.database.service.query.index.integer.snappy;

import org.jumbodb.database.service.query.snappy.SnappyChunks;
import org.jumbodb.database.service.query.snappy.SnappyUtil;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @author Carsten Hufe
 */
public class IntegerLtOperationSearch implements OperationSearch<Integer> {
    @Override
    public long findFirstMatchingChunk(RandomAccessFile indexRaf, Integer searchValue, SnappyChunks snappyChunks) throws IOException {
        // wow that was easy .... file has already matched, everything from beginning must be smaller
        return 0;
    }

    @Override
    public boolean matching(Integer currentValue, Integer searchValue) {
        return currentValue < searchValue;
    }
}
