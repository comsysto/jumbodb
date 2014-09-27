package org.jumbodb.database.service.query.index.common.numeric;

import org.jumbodb.data.common.compression.Blocks;
import org.jumbodb.database.service.query.index.common.QueryValueRetriever;
import org.jumbodb.database.service.query.index.common.IndexOperationSearch;

import java.io.IOException;

/**
 * @author Carsten Hufe
 */
public abstract class NumberLtOperationSearch<T, IFV, IF extends NumberIndexFile<IFV>> implements IndexOperationSearch<T, IFV, IF> {

    @Override
    public long findFirstMatchingBlock(FileDataRetriever<T> fileDataRetriever, QueryValueRetriever queryClause, Blocks blocks) throws IOException {
        // wow that was easy .... file has already matched, everything from beginning must be smaller
        return 0;
    }

    @Override
    public boolean matchingBlock(T currentValue, QueryValueRetriever queryValueRetriever) {
        return matching(currentValue, queryValueRetriever);
    }

    @Override
    public boolean searchFinished(T currentValue, QueryValueRetriever queryValueRetriever, boolean resultsFound) {
        return resultsFound;
    }
}
