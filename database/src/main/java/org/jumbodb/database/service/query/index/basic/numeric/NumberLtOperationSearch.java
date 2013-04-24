package org.jumbodb.database.service.query.index.basic.numeric;

import org.jumbodb.database.service.query.snappy.SnappyChunks;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @author Carsten Hufe
 */
public abstract class NumberLtOperationSearch<T extends Number, IF extends NumberSnappyIndexFile<T>> implements OperationSearch<T, IF> {
    private NumberSnappyIndexStrategy<T, IF> strategy;

    public NumberLtOperationSearch(NumberSnappyIndexStrategy<T, IF> strategy) {
        this.strategy = strategy;
    }

    @Override
    public long findFirstMatchingChunk(RandomAccessFile indexRaf, QueryValueRetriever queryClause, SnappyChunks snappyChunks) throws IOException {
        // wow that was easy .... file has already matched, everything from beginning must be smaller
        return 0;
    }

}
