package org.jumbodb.database.service.query.index.basic.numeric;

import org.jumbodb.data.common.snappy.SnappyChunks;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @author Carsten Hufe
 */
public abstract class NumberLtOperationSearch<T, IFV, IF extends NumberSnappyIndexFile<IFV>> implements OperationSearch<T, IFV, IF> {
    private NumberSnappyIndexStrategy<T, IFV, IF> strategy;

    public NumberLtOperationSearch(NumberSnappyIndexStrategy<T, IFV, IF> strategy) {
        this.strategy = strategy;
    }

    @Override
    public long findFirstMatchingChunk(RandomAccessFile indexRaf, QueryValueRetriever queryClause, SnappyChunks snappyChunks) throws IOException {
        // wow that was easy .... file has already matched, everything from beginning must be smaller
        return 0;
    }

}
