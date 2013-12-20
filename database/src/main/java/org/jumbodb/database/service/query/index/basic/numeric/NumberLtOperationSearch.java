package org.jumbodb.database.service.query.index.basic.numeric;

import org.jumbodb.data.common.snappy.SnappyChunks;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @author Carsten Hufe
 */
public abstract class NumberLtOperationSearch<T, IFV, IF extends NumberSnappyIndexFile<IFV>> implements OperationSearch<T, IFV, IF> {

    @Override
    public long findFirstMatchingChunk(FileDataRetriever<T> fileDataRetriever, QueryValueRetriever queryClause, SnappyChunks snappyChunks) throws IOException {
        // wow that was easy .... file has already matched, everything from beginning must be smaller
        return 0;
    }

    @Override
    public boolean matchingChunk(T currentValue, QueryValueRetriever queryValueRetriever) {
        return matching(currentValue, queryValueRetriever);
    }

    @Override
    public boolean searchFinished(T currentValue, QueryValueRetriever queryValueRetriever, boolean resultsFound) {
        return resultsFound;
    }
}
