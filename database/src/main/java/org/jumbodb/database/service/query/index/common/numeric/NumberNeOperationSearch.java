package org.jumbodb.database.service.query.index.common.numeric;

import org.jumbodb.data.common.compression.Blocks;
import org.jumbodb.database.service.query.index.common.BlockRange;
import org.jumbodb.database.service.query.index.common.QueryValueRetriever;
import org.jumbodb.database.service.query.index.common.IndexOperationSearch;

import java.io.IOException;

/**
 * @author Carsten Hufe
 */
public abstract class NumberNeOperationSearch<T, IFV, IF extends NumberIndexFile<IFV>> implements IndexOperationSearch<T, IFV, IF> {

    // CARSTEN move to NumberSnappyIndexFile and call by delegate
    @Override
    public long findFirstMatchingBlock(FileDataRetriever<T> fileDataRetriever, QueryValueRetriever queryClause, Blocks blocks) throws IOException {
        T searchValue = (T)queryClause.getValue();
        int numberOfChunks = blocks.getNumberOfBlocks();
        int fromChunk = 0;
        int toChunk = numberOfChunks;
        while(toChunk != 0) {
            int currentChunk = (toChunk - fromChunk) / 2;
            BlockRange<T> blockRange = fileDataRetriever.getBlockRange(currentChunk);
            T firstInt = blockRange.getFirstValue();
            T lastInt = blockRange.getLastValue();

            // just going up
            //firstInt != searchValue || lastInt != searchValue
            if(ne(firstInt, searchValue) || ne(lastInt, searchValue)) {
                toChunk = currentChunk;
            } else {
                return currentChunk;
            }

        }
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

    public abstract boolean ne(T val1, T val2);
}
