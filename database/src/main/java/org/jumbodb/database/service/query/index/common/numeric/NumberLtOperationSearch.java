package org.jumbodb.database.service.query.index.common.numeric;

import org.jumbodb.data.common.snappy.SnappyChunks;
import org.jumbodb.database.service.query.index.common.QueryValueRetriever;
import org.jumbodb.database.service.query.index.common.IndexOperationSearch;

import java.io.IOException;

/**
 * @author Carsten Hufe
 */
public abstract class NumberLtOperationSearch<T, IFV, IF extends NumberIndexFile<IFV>> implements IndexOperationSearch<T, IFV, IF> {

    // CARSTEN move to NumberSnappyIndexFile and call by delegate
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